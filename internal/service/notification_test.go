package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
)

type fakeRepository struct {
	created []domain.Notification
	stored  map[uuid.UUID]domain.Notification
	failure error
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{stored: map[uuid.UUID]domain.Notification{}}
}

func (repo *fakeRepository) Create(_ context.Context, notification domain.Notification) error {
	if repo.failure != nil {
		return repo.failure
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

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

var testNow = time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

func newTestService(repo *fakeRepository) *NotificationService {
	return NewNotificationService(repo, fixedClock{now: testNow})
}

func TestCreatePersistsPendingNotification(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)

	created, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if created.ID == uuid.Nil {
		t.Error("ID not assigned")
	}
	if created.Status != domain.StatusPending {
		t.Errorf("Status = %s, want %s", created.Status, domain.StatusPending)
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
}

func TestCreateSchedulesFutureNotification(t *testing.T) {
	repo := newFakeRepository()
	svc := newTestService(repo)
	future := testNow.Add(time.Hour)

	created, err := svc.Create(context.Background(), CreateInput{
		Recipient:   "+905551234567",
		Channel:     domain.ChannelSMS,
		Content:     "hello",
		ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if created.Status != domain.StatusScheduled {
		t.Errorf("Status = %s, want %s", created.Status, domain.StatusScheduled)
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

	created, err := svc.Create(context.Background(), CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

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
