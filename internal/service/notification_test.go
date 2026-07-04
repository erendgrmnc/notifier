package service

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
	created       []domain.Notification
	stored        map[uuid.UUID]domain.Notification
	failure       error
	lastListQuery domain.ListQuery
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{stored: map[uuid.UUID]domain.Notification{}}
}

func (repo *fakeRepository) Create(_ context.Context, notification domain.Notification) error {
	if repo.failure != nil {
		return repo.failure
	}
	if notification.IdempotencyKey != nil {
		for _, existing := range repo.stored {
			if existing.IdempotencyKey != nil && *existing.IdempotencyKey == *notification.IdempotencyKey {
				return domain.ErrDuplicateIdempotencyKey
			}
		}
	}
	repo.created = append(repo.created, notification)
	repo.stored[notification.ID] = notification
	return nil
}

func (repo *fakeRepository) GetByID(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	notification, ok := repo.stored[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return notification, nil
}

func (repo *fakeRepository) List(_ context.Context, query domain.ListQuery) ([]domain.Notification, error) {
	repo.lastListQuery = query
	var notifications []domain.Notification
	for _, notification := range repo.stored {
		if query.Status != "" && notification.Status != query.Status {
			continue
		}
		notifications = append(notifications, notification)
		if len(notifications) == query.Limit {
			break
		}
	}
	return notifications, nil
}

func (repo *fakeRepository) GetByIdempotencyKey(_ context.Context, key string) (domain.Notification, error) {
	for _, notification := range repo.stored {
		if notification.IdempotencyKey != nil && *notification.IdempotencyKey == key {
			return notification, nil
		}
	}
	return domain.Notification{}, domain.ErrNotFound
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
			return nil
		}
	}
	return domain.ErrInvalidTransition
}

type fakePublisher struct {
	published []domain.Notification
	failure   error
}

func (publisher *fakePublisher) PublishCreated(_ context.Context, notification domain.Notification) error {
	if publisher.failure != nil {
		return publisher.failure
	}
	publisher.published = append(publisher.published, notification)
	return nil
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

var testNow = time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func newTestService(repo *fakeRepository) *NotificationService {
	return newTestServiceWithPublisher(repo, &fakePublisher{})
}

func newTestServiceWithPublisher(repo *fakeRepository, publisher *fakePublisher) *NotificationService {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewNotificationService(repo, &fakeBatchRepository{fakeRepository: repo}, &fakeTemplateRepository{}, publisher, fixedClock{now: testNow}, logger, nil)
}

func TestCreatePublishesAndMarksQueued(t *testing.T) {
	repo := newFakeRepository()
	publisher := &fakePublisher{}
	svc := newTestServiceWithPublisher(repo, publisher)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := result.Notification

	if created.ID == uuid.Nil {
		t.Error("ID not assigned")
	}
	if created.Status != domain.StatusQueued {
		t.Errorf("Status = %s, want %s after publish", created.Status, domain.StatusQueued)
	}
	if created.Priority != domain.PriorityNormal {
		t.Errorf("Priority = %s, want default %s", created.Priority, domain.PriorityNormal)
	}
	if !created.CreatedAt.Equal(testNow) {
		t.Errorf("CreatedAt = %v, want clock time %v", created.CreatedAt, testNow)
	}
	if len(repo.created) != 1 {
		t.Fatalf("persisted %d notifications, want 1", len(repo.created))
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.published))
	}
	if stored := repo.stored[created.ID]; stored.Status != domain.StatusQueued {
		t.Errorf("stored status = %s, want queued", stored.Status)
	}
}

func TestCreatePublishFailureLeavesPending(t *testing.T) {
	repo := newFakeRepository()
	publisher := &fakePublisher{failure: errors.New("broker down")}
	svc := newTestServiceWithPublisher(repo, publisher)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("Create should tolerate publish failure, got: %v", err)
	}
	created := result.Notification

	if created.Status != domain.StatusPending {
		t.Errorf("Status = %s, want pending when publish fails", created.Status)
	}
	if stored := repo.stored[created.ID]; stored.Status != domain.StatusPending {
		t.Errorf("stored status = %s, want pending for sweeper recovery", stored.Status)
	}
}

func TestCreateSchedulesFutureNotification(t *testing.T) {
	repo := newFakeRepository()
	publisher := &fakePublisher{}
	svc := newTestServiceWithPublisher(repo, publisher)
	future := testNow.Add(time.Hour)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient:   "+905551234567",
		Channel:     domain.ChannelSMS,
		Content:     "hello",
		ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := result.Notification

	if created.Status != domain.StatusScheduled {
		t.Errorf("Status = %s, want %s", created.Status, domain.StatusScheduled)
	}
	if len(publisher.published) != 0 {
		t.Errorf("scheduled notification was published immediately; the scheduler owns future deliveries")
	}
}

func TestCreateRejectsInvalidInput(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)

	_, err := svc.Create(context.Background(), CreateInput{
		Recipient: "not-a-phone",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})

	var validationErrs domain.ValidationErrors
	if !errors.As(err, &validationErrs) {
		t.Fatalf("error = %v (%T), want domain.ValidationErrors", err, err)
	}
	if len(repo.created) != 0 {
		t.Error("invalid notification was persisted")
	}
}

func TestCreatePropagatesRepositoryFailure(t *testing.T) {
	repo := newFakeRepository()
	repo.failure = errors.New("connection lost")
	svc := newTestService(repo)

	_, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err == nil {
		t.Fatal("Create = nil error, want repository failure")
	}
}

func TestGetReturnsStoredNotification(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := result.Notification

	fetched, err := svc.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.ID != created.ID {
		t.Errorf("Get returned ID %s, want %s", fetched.ID, created.ID)
	}
}

func TestGetUnknownIDReturnsNotFound(t *testing.T) {
	svc := newTestService(newFakeRepository())

	_, err := svc.Get(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Get error = %v, want domain.ErrNotFound", err)
	}
}

func TestCreateReplaysDuplicateIdempotencyKey(t *testing.T) {
	repo := newFakeRepository()
	publisher := &fakePublisher{}
	svc := newTestServiceWithPublisher(repo, publisher)
	key := "client-key-1"

	first, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "hello",
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	replay, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "hello",
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("replayed Create: %v", err)
	}

	if !replay.Replayed {
		t.Error("Replayed = false on duplicate key")
	}
	if replay.Notification.ID != first.Notification.ID {
		t.Errorf("replay returned ID %s, want original %s", replay.Notification.ID, first.Notification.ID)
	}
	if len(repo.created) != 1 {
		t.Errorf("persisted %d notifications, want 1", len(repo.created))
	}
	if len(publisher.published) != 1 {
		t.Errorf("published %d messages, want 1 (replay must not republish)", len(publisher.published))
	}
}

func TestCancelPendingNotification(t *testing.T) {
	repo := newFakeRepository()
	publisher := &fakePublisher{failure: errors.New("broker down")} // keeps status pending
	svc := newTestServiceWithPublisher(repo, publisher)

	result, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "hello",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	cancelled, err := svc.Cancel(context.Background(), result.Notification.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", cancelled.Status)
	}
}

func TestCancelRejectsTerminalStatus(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)
	notification := domain.Notification{ID: uuid.New(), Status: domain.StatusSent}
	repo.stored[notification.ID] = notification

	if _, err := svc.Cancel(context.Background(), notification.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("Cancel(sent) error = %v, want ErrInvalidTransition", err)
	}
	if _, err := svc.Cancel(context.Background(), uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Cancel(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestListClampsLimit(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)

	if _, err := svc.List(context.Background(), domain.ListQuery{Limit: 100000}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if repo.lastListQuery.Limit != MaxListLimit {
		t.Errorf("limit = %d, want clamped to %d", repo.lastListQuery.Limit, MaxListLimit)
	}

	if _, err := svc.List(context.Background(), domain.ListQuery{}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if repo.lastListQuery.Limit != DefaultListLimit {
		t.Errorf("limit = %d, want default %d", repo.lastListQuery.Limit, DefaultListLimit)
	}
}

func TestListRejectsInvalidFilter(t *testing.T) {
	svc := newTestService(newFakeRepository())

	_, err := svc.List(context.Background(), domain.ListQuery{Status: domain.Status("bogus")})
	var validationErrs domain.ValidationErrors
	if !errors.As(err, &validationErrs) {
		t.Errorf("List error = %v, want ValidationErrors", err)
	}
}
