package scheduler

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
	due        []domain.Notification
	stale      []domain.Notification
	claimErr   error
	touched    []uuid.UUID
	claimCalls int
}

func (repo *fakeRepository) ClaimDueForQueue(_ context.Context, _ time.Duration, _ int) ([]domain.Notification, error) {
	repo.claimCalls++
	if repo.claimErr != nil {
		return nil, repo.claimErr
	}
	claimed := repo.due
	repo.due = nil
	return claimed, nil
}

func (repo *fakeRepository) ListStaleQueued(_ context.Context, _ time.Duration, _ int) ([]domain.Notification, error) {
	stale := repo.stale
	repo.stale = nil
	return stale, nil
}

func (repo *fakeRepository) TouchQueued(_ context.Context, id uuid.UUID) error {
	repo.touched = append(repo.touched, id)
	return nil
}

type fakePublisher struct {
	published []uuid.UUID
	failFor   map[uuid.UUID]error
}

func (publisher *fakePublisher) PublishCreated(_ context.Context, notification domain.Notification) error {
	if err := publisher.failFor[notification.ID]; err != nil {
		return err
	}
	publisher.published = append(publisher.published, notification.ID)
	return nil
}

func newTestScheduler(repo *fakeRepository, publisher *fakePublisher) *Scheduler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return New(repo, publisher, logger, time.Second, time.Minute)
}

func dueNotification(status domain.Status) domain.Notification {
	return domain.Notification{ID: uuid.New(), Channel: domain.ChannelSMS, Status: status}
}

func TestRunOncePublishesClaimedDueRows(t *testing.T) {
	first := dueNotification(domain.StatusQueued)
	second := dueNotification(domain.StatusQueued)
	repo := &fakeRepository{due: []domain.Notification{first, second}}
	publisher := &fakePublisher{}

	newTestScheduler(repo, publisher).RunOnce(context.Background())

	if len(publisher.published) != 2 {
		t.Fatalf("published %d, want 2", len(publisher.published))
	}
}

func TestRunOnceToleratesPublishFailure(t *testing.T) {
	failing := dueNotification(domain.StatusQueued)
	healthy := dueNotification(domain.StatusQueued)
	repo := &fakeRepository{due: []domain.Notification{failing, healthy}}
	publisher := &fakePublisher{failFor: map[uuid.UUID]error{failing.ID: errors.New("broker down")}}

	newTestScheduler(repo, publisher).RunOnce(context.Background())

	// The healthy row still publishes; the failed one stays queued for
	// the stale sweep to retry later.
	if len(publisher.published) != 1 || publisher.published[0] != healthy.ID {
		t.Errorf("published %v, want only healthy %s", publisher.published, healthy.ID)
	}
}

func TestRunOnceRepublishesStaleQueuedAndTouches(t *testing.T) {
	stale := dueNotification(domain.StatusQueued)
	repo := &fakeRepository{stale: []domain.Notification{stale}}
	publisher := &fakePublisher{}

	newTestScheduler(repo, publisher).RunOnce(context.Background())

	if len(publisher.published) != 1 || publisher.published[0] != stale.ID {
		t.Fatalf("published %v, want stale row", publisher.published)
	}
	if len(repo.touched) != 1 || repo.touched[0] != stale.ID {
		t.Errorf("touched %v, want stale row refreshed", repo.touched)
	}
}

func TestRunOnceToleratesClaimError(t *testing.T) {
	repo := &fakeRepository{claimErr: errors.New("db down")}
	publisher := &fakePublisher{}

	// Must not panic or publish; the next tick retries.
	newTestScheduler(repo, publisher).RunOnce(context.Background())

	if len(publisher.published) != 0 {
		t.Errorf("published %v after claim error, want none", publisher.published)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	repo := &fakeRepository{}
	scheduler := newTestScheduler(repo, &fakePublisher{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on context cancel")
	}
}
