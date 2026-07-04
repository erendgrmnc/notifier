package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
)

type fakeRepository struct {
	stored      map[uuid.UUID]domain.Notification
	getFailure  error
	markedSent  []uuid.UUID
	transitions []domain.Status
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{stored: map[uuid.UUID]domain.Notification{}}
}

func (repo *fakeRepository) GetByID(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	if repo.getFailure != nil {
		return domain.Notification{}, repo.getFailure
	}
	notification, ok := repo.stored[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return notification, nil
}

func (repo *fakeRepository) UpdateStatus(_ context.Context, id uuid.UUID, to domain.Status, allowedFrom ...domain.Status) error {
	notification, ok := repo.stored[id]
	if !ok {
		return domain.ErrNotFound
	}
	for _, from := range allowedFrom {
		if notification.Status == from {
			notification.Status = to
			repo.stored[id] = notification
			repo.transitions = append(repo.transitions, to)
			return nil
		}
	}
	return domain.ErrInvalidTransition
}

func (repo *fakeRepository) MarkSent(_ context.Context, id uuid.UUID, providerMessageID string, sentAt time.Time) error {
	notification, ok := repo.stored[id]
	if !ok {
		return domain.ErrNotFound
	}
	if notification.Status != domain.StatusProcessing {
		return domain.ErrInvalidTransition
	}
	notification.Status = domain.StatusSent
	notification.ProviderMessageID = &providerMessageID
	notification.SentAt = &sentAt
	repo.stored[id] = notification
	repo.markedSent = append(repo.markedSent, id)
	return nil
}

type fakeSender struct {
	sent    []domain.Notification
	failure error
}

func (sender *fakeSender) Send(_ context.Context, notification domain.Notification) (string, error) {
	if sender.failure != nil {
		return "", sender.failure
	}
	sender.sent = append(sender.sent, notification)
	return "provider-msg-1", nil
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

var testNow = time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

func newTestWorker(repo *fakeRepository, sender *fakeSender) *Worker {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return New(repo, sender, fixedClock{now: testNow}, logger)
}

func seedNotification(repo *fakeRepository, status domain.Status) domain.Notification {
	notification := domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
		Priority:  domain.PriorityNormal,
		Status:    status,
	}
	repo.stored[notification.ID] = notification
	return notification
}

func TestProcessDeliversQueuedNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender)
	notification := seedNotification(repo, domain.StatusQueued)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sender called %d times, want 1", len(sender.sent))
	}
	stored := repo.stored[notification.ID]
	if stored.Status != domain.StatusSent {
		t.Errorf("status = %s, want sent", stored.Status)
	}
	if stored.ProviderMessageID == nil || *stored.ProviderMessageID != "provider-msg-1" {
		t.Errorf("provider_message_id = %v, want provider-msg-1", stored.ProviderMessageID)
	}
	if stored.SentAt == nil || !stored.SentAt.Equal(testNow) {
		t.Errorf("sent_at = %v, want clock time", stored.SentAt)
	}
}

func TestProcessDropsCancelledNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender)
	notification := seedNotification(repo, domain.StatusCancelled)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	if len(sender.sent) != 0 {
		t.Error("cancelled notification was sent")
	}
	if repo.stored[notification.ID].Status != domain.StatusCancelled {
		t.Errorf("status changed to %s, want cancelled untouched", repo.stored[notification.ID].Status)
	}
}

func TestProcessDropsAlreadySentNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender)
	notification := seedNotification(repo, domain.StatusSent)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	if len(sender.sent) != 0 {
		t.Error("already-sent notification was sent again")
	}
}

func TestProcessDropsUnknownNotification(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{}
	queueWorker := newTestWorker(repo, sender)

	if err := queueWorker.processNotification(context.Background(), uuid.New()); err != nil {
		t.Fatalf("processNotification: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Error("unknown notification was sent")
	}
}

func TestProcessReturnsInfrastructureErrors(t *testing.T) {
	repo := newFakeRepository()
	repo.getFailure = errors.New("db down")
	queueWorker := newTestWorker(repo, &fakeSender{})

	if err := queueWorker.processNotification(context.Background(), uuid.New()); err == nil {
		t.Fatal("infrastructure error swallowed; message would be lost")
	}
}

func TestProcessSenderFailureMarksFailed(t *testing.T) {
	repo := newFakeRepository()
	sender := &fakeSender{failure: errors.New("provider unavailable")}
	queueWorker := newTestWorker(repo, sender)
	notification := seedNotification(repo, domain.StatusQueued)

	if err := queueWorker.processNotification(context.Background(), notification.ID); err != nil {
		t.Fatalf("processNotification: %v", err)
	}

	if repo.stored[notification.ID].Status != domain.StatusFailed {
		t.Errorf("status = %s, want failed", repo.stored[notification.ID].Status)
	}
}
