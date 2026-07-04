package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"notifier/internal/delivery"
	"notifier/internal/domain"
	"notifier/internal/queue/rabbit"
)

type fakeRepository struct {
	stored       map[uuid.UUID]domain.Notification
	claimFailure error
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{stored: map[uuid.UUID]domain.Notification{}}
}

func (repo *fakeRepository) ClaimForProcessing(_ context.Context, id uuid.UUID, allowedFrom ...domain.Status) (domain.Notification, error) {
	if repo.claimFailure != nil {
		return domain.Notification{}, repo.claimFailure
	}
	notification, ok := repo.stored[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	for _, from := range allowedFrom {
		if notification.Status == from {
			notification.Status = domain.StatusProcessing
			notification.Attempts++
			repo.stored[id] = notification
			return notification, nil
		}
	}
	return domain.Notification{}, domain.ErrInvalidTransition
}

func (repo *fakeRepository) MarkSent(_ context.Context, id uuid.UUID, providerMessageID string, sentAt time.Time) error {
	notification := repo.stored[id]
	if notification.Status != domain.StatusProcessing {
		return domain.ErrInvalidTransition
	}
	notification.Status = domain.StatusSent
	notification.ProviderMessageID = &providerMessageID
	notification.SentAt = &sentAt
	repo.stored[id] = notification
	return nil
}

func (repo *fakeRepository) MarkRetrying(_ context.Context, id uuid.UUID, lastError string) error {
	notification := repo.stored[id]
	if notification.Status != domain.StatusProcessing {
		return domain.ErrInvalidTransition
	}
	notification.Status = domain.StatusRetrying
	notification.LastError = &lastError
	repo.stored[id] = notification
	return nil
}

func (repo *fakeRepository) MarkFailed(_ context.Context, id uuid.UUID, lastError string) error {
	notification := repo.stored[id]
	if notification.Status != domain.StatusProcessing {
		return domain.ErrInvalidTransition
	}
	notification.Status = domain.StatusFailed
	notification.LastError = &lastError
	repo.stored[id] = notification
	return nil
}

type retryPublish struct {
	id      uuid.UUID
	attempt int
}

type fakePublisher struct {
	retries      []retryPublish
	deadLetters  []uuid.UUID
	events       []rabbit.StatusEvent
	retryFailure error
}

func (publisher *fakePublisher) PublishEvent(_ context.Context, event rabbit.StatusEvent) error {
	publisher.events = append(publisher.events, event)
	return nil
}

func (publisher *fakePublisher) PublishRetry(_ context.Context, notification domain.Notification, attempt int) error {
	if publisher.retryFailure != nil {
		return publisher.retryFailure
	}
	publisher.retries = append(publisher.retries, retryPublish{id: notification.ID, attempt: attempt})
	return nil
}

func (publisher *fakePublisher) PublishDeadLetter(_ context.Context, notification domain.Notification, _ string) error {
	publisher.deadLetters = append(publisher.deadLetters, notification.ID)
	return nil
}

type fakeSender struct {
	sent    []domain.Notification
	failure error
}

func (sender *fakeSender) Send(_ context.Context, notification domain.Notification) (delivery.Result, error) {
	if sender.failure != nil {
		return delivery.Result{}, sender.failure
	}
	sender.sent = append(sender.sent, notification)
	return delivery.Result{ProviderMessageID: "provider-msg-1", StatusCode: 202, Body: `{"messageId":"provider-msg-1"}`}, nil
}

type fakePauseChecker struct {
	paused  bool
	failure error
}

func (checker *fakePauseChecker) WorkerPaused(context.Context) (bool, error) {
	return checker.paused, checker.failure
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

var testNow = time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

const (
	testMaxAttempts = 4
	// testRateLimit is high enough that ordinary tests never throttle.
	testRateLimit   = 10000
	testConcurrency = 2
)

func newTestWorker(repo *fakeRepository, sender *fakeSender, publisher *fakePublisher) *Worker {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return New(repo, sender, publisher, &fakePauseChecker{}, fixedClock{now: testNow}, logger, nil,
		testMaxAttempts, testRateLimit, testConcurrency)
}

func TestRateLimiterThrottlesDeliveries(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	// 20/s with burst 20... too permissive to observe; use a limiter of
	// 20/s but burst 1 by constructing then draining the initial burst.
	queueWorker := New(repo, sender, &fakePublisher{}, &fakePauseChecker{}, fixedClock{now: testNow}, logger, nil,
		testMaxAttempts, 20, testConcurrency)
	// Drain the initial burst allowance so subsequent sends pay full price.
	limiter := queueWorker.limiters[domain.ChannelSMS]
	_ = limiter.ReserveN(time.Now(), 20)

	const sends = 4 // 4 sends at 20/s after burst drain ≈ ≥150ms
	started := time.Now()
	for i := 0; i < sends; i++ {
		notification := seedNotification(repo, domain.StatusQueued, 0)
		if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
			t.Fatalf("processNotification: %v", err)
		}
	}
	elapsed := time.Since(started)

	// Coarse lower bound to avoid flakes: 4 tokens at 20/s ≥ 150ms.
	if elapsed < 150*time.Millisecond {
		t.Errorf("4 throttled sends finished in %v, want ≥150ms", elapsed)
	}
	if len(sender.sent) != sends {
		t.Errorf("sent %d, want %d", len(sender.sent), sends)
	}
}

func TestRateLimiterIsPerChannel(t *testing.T) {
	queueWorker := newTestWorker(newFakeRepository(), &fakeSender{}, &fakePublisher{})
	seen := map[*rate.Limiter]bool{}
	for _, deliveryChannel := range domain.Channels() {
		limiter := queueWorker.limiters[deliveryChannel]
		if limiter == nil {
			t.Fatalf("no limiter for channel %s", deliveryChannel)
		}
		if seen[limiter] {
			t.Error("channels share a limiter; caps must be independent")
		}
		seen[limiter] = true
	}
}

func TestIsPausedReflectsChecker(t *testing.T) {
	checker := &fakePauseChecker{paused: true}
	queueWorker := newTestWorker(newFakeRepository(), &fakeSender{}, &fakePublisher{})
	queueWorker.pause = checker

	if !queueWorker.isPaused(context.Background()) {
		t.Error("isPaused = false while checker reports paused")
	}

	checker.paused = false
	if queueWorker.isPaused(context.Background()) {
		t.Error("isPaused = true while checker reports unpaused")
	}
}

func TestIsPausedKeepsLastStateOnFailure(t *testing.T) {
	checker := &fakePauseChecker{paused: true}
	queueWorker := newTestWorker(newFakeRepository(), &fakeSender{}, &fakePublisher{})
	queueWorker.pause = checker

	if !queueWorker.isPaused(context.Background()) {
		t.Fatal("setup: expected paused")
	}

	checker.failure = errors.New("db down")
	checker.paused = false // would flip, but the check fails
	if !queueWorker.isPaused(context.Background()) {
		t.Error("isPaused dropped last-known paused state on check failure")
	}
}

func seedNotification(repo *fakeRepository, status domain.Status, attempts int) domain.Notification {
	notification := domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
		Priority:  domain.PriorityNormal,
		Status:    status,
		Attempts:  attempts,
	}
	repo.stored[notification.ID] = notification
	return notification
}

func TestProcessDeliversQueuedNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender, &fakePublisher{})
	notification := seedNotification(repo, domain.StatusQueued, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	stored := repo.stored[notification.ID]
	if stored.Status != domain.StatusSent {
		t.Errorf("status = %s, want sent", stored.Status)
	}
	if stored.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", stored.Attempts)
	}
	publisher := queueWorker.publisher.(*fakePublisher)
	if len(publisher.events) != 1 || publisher.events[0].Status != "sent" {
		t.Errorf("events = %+v, want one sent event", publisher.events)
	}
	if sent := publisher.events[0]; sent.ProviderStatusCode != 202 || sent.ProviderResponse == "" {
		t.Errorf("sent event provider fields = %d/%q, want 202 with response body", sent.ProviderStatusCode, sent.ProviderResponse)
	}
	if stored.ProviderMessageID == nil || *stored.ProviderMessageID != "provider-msg-1" {
		t.Errorf("provider_message_id = %v, want provider-msg-1", stored.ProviderMessageID)
	}
}

func TestProcessDeliversPendingNotificationFromPublishRace(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender, &fakePublisher{})
	notification := seedNotification(repo, domain.StatusPending, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}
	if repo.stored[notification.ID].Status != domain.StatusSent {
		t.Errorf("status = %s, want sent", repo.stored[notification.ID].Status)
	}
}

func TestProcessDropsCancelledNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender, &fakePublisher{})
	notification := seedNotification(repo, domain.StatusCancelled, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Error("cancelled notification was sent")
	}
}

func TestProcessDropsUnknownNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender, &fakePublisher{})

	if err := queueWorker.processNotification(context.Background(), uuid.New()); err != nil {
		t.Fatalf("processNotification: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Error("unknown notification was sent")
	}
}

func TestProcessReturnsInfrastructureErrors(t *testing.T) {
	repo := newFakeRepository()
	repo.claimFailure = errors.New("db down")
	queueWorker := newTestWorker(repo, &fakeSender{}, &fakePublisher{})

	if err := queueWorker.processNotification(context.Background(), uuid.New()); err == nil {
		t.Fatal("infrastructure error swallowed; message would be lost")
	}
}

func TestRetryableFailureSchedulesRetry(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{failure: &delivery.SendError{StatusCode: 500, Retryable: true, Message: "provider boom"}}
	publisher := &fakePublisher{}
	queueWorker := newTestWorker(repo, sender, publisher)
	notification := seedNotification(repo, domain.StatusQueued, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	stored := repo.stored[notification.ID]
	if stored.Status != domain.StatusRetrying {
		t.Errorf("status = %s, want retrying", stored.Status)
	}
	if stored.LastError == nil || *stored.LastError == "" {
		t.Error("last_error not recorded")
	}
	if len(publisher.retries) != 1 || publisher.retries[0].attempt != 1 {
		t.Errorf("retry publishes = %+v, want one publish for attempt 1", publisher.retries)
	}
	if len(publisher.deadLetters) != 0 {
		t.Error("retryable failure went to DLQ")
	}
}

func TestPermanentFailureGoesToDLQ(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{failure: &delivery.SendError{StatusCode: 400, Retryable: false, Message: "bad recipient"}}
	publisher := &fakePublisher{}
	queueWorker := newTestWorker(repo, sender, publisher)
	notification := seedNotification(repo, domain.StatusQueued, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	if repo.stored[notification.ID].Status != domain.StatusFailed {
		t.Errorf("status = %s, want failed", repo.stored[notification.ID].Status)
	}
	if len(publisher.retries) != 0 {
		t.Error("permanent failure scheduled a retry")
	}
	if len(publisher.deadLetters) != 1 {
		t.Errorf("dead letters = %d, want 1", len(publisher.deadLetters))
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{failure: &delivery.SendError{StatusCode: 503, Retryable: true, Message: "still down"}}
	publisher := &fakePublisher{}
	queueWorker := newTestWorker(repo, sender, publisher)
	// Three prior attempts; this claim makes it the fourth and final.
	notification := seedNotification(repo, domain.StatusRetrying, testMaxAttempts-1)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	stored := repo.stored[notification.ID]
	if stored.Status != domain.StatusFailed {
		t.Errorf("status = %s, want failed after exhausting attempts", stored.Status)
	}
	if stored.Attempts != testMaxAttempts {
		t.Errorf("attempts = %d, want %d", stored.Attempts, testMaxAttempts)
	}
	if len(publisher.retries) != 0 {
		t.Error("exhausted notification scheduled another retry")
	}
	if len(publisher.deadLetters) != 1 {
		t.Errorf("dead letters = %d, want 1", len(publisher.deadLetters))
	}
}

func TestRetryPublishFailureRequeuesMessage(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{failure: &delivery.SendError{StatusCode: 500, Retryable: true, Message: "boom"}}
	publisher := &fakePublisher{retryFailure: errors.New("broker down")}
	queueWorker := newTestWorker(repo, sender, publisher)
	notification := seedNotification(repo, domain.StatusQueued, 0)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err == nil {
		t.Fatal("retry publish failure swallowed; retry message would be lost")
	}
	// Row is retrying, so the nack-redelivered original message can
	// re-claim it (retrying is allowed into processing).
	if repo.stored[notification.ID].Status != domain.StatusRetrying {
		t.Errorf("status = %s, want retrying for redelivery recovery", repo.stored[notification.ID].Status)
	}
}
