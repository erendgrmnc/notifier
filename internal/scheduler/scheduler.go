// Package scheduler moves time-based work into the queue: scheduled
// notifications whose delivery time has arrived, and rows whose publish
// was lost (pending past the sweep cutoff, queued but never consumed).
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/queue/rabbit"
)

// claimBatchLimit bounds the rows claimed per tick so one giant backlog
// cannot monopolize a tick.
const claimBatchLimit = 100

// Repository is what the scheduler needs from persistence.
type Repository interface {
	ClaimDueForQueue(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)
	ListStaleQueued(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)
	RecoverStaleProcessing(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)
	TouchQueued(ctx context.Context, id uuid.UUID) error
}

// Publisher hands claimed notifications to the queue and fans out
// status events.
type Publisher interface {
	PublishCreated(ctx context.Context, notification domain.Notification) error
	PublishEvent(ctx context.Context, event rabbit.StatusEvent) error
}

// Clock supplies time so event timestamps are testable.
type Clock interface {
	Now() time.Time
}

// Scheduler polls for due and stranded work.
type Scheduler struct {
	repo         Repository
	publisher    Publisher
	clock        Clock
	logger       *slog.Logger
	pollInterval time.Duration
	staleAfter   time.Duration
}

func New(repo Repository, publisher Publisher, clock Clock, logger *slog.Logger, pollInterval, staleAfter time.Duration) *Scheduler {
	return &Scheduler{
		repo:         repo,
		publisher:    publisher,
		clock:        clock,
		logger:       logger,
		pollInterval: pollInterval,
		staleAfter:   staleAfter,
	}
}

// Run polls until the context is cancelled.
func (scheduler *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(scheduler.pollInterval)
	defer ticker.Stop()

	scheduler.logger.Info("scheduler running",
		slog.Duration("poll_interval", scheduler.pollInterval),
		slog.Duration("stale_after", scheduler.staleAfter),
	)
	for {
		select {
		case <-ctx.Done():
			scheduler.logger.Info("scheduler stopping")
			return
		case <-ticker.C:
			scheduler.RunOnce(ctx)
		}
	}
}

// RunOnce performs one poll: queue due/stuck rows, republish stale
// queued ones, and recover rows stranded in processing by a crashed
// worker. Errors are logged, never fatal — the next tick retries.
func (scheduler *Scheduler) RunOnce(ctx context.Context) {
	scheduler.queueDue(ctx)
	scheduler.republishStale(ctx)
	scheduler.recoverStaleProcessing(ctx)
}

// recoverStaleProcessing moves crash-stranded processing rows back to
// retrying and republishes them. May re-attempt an already-sent message
// when the crash landed between provider accept and the sent mark — the
// documented at-least-once trade-off.
func (scheduler *Scheduler) recoverStaleProcessing(ctx context.Context) {
	recovered, err := scheduler.repo.RecoverStaleProcessing(ctx, scheduler.staleAfter, claimBatchLimit)
	if err != nil {
		scheduler.logger.Error("recover stale processing failed", slog.Any("error", err))
		return
	}

	for _, notification := range recovered {
		scheduler.logger.Warn("recovered notification stuck in processing",
			slog.String("notification_id", notification.ID.String()),
			slog.Int("attempts", notification.Attempts),
		)
		if err := scheduler.publisher.PublishCreated(ctx, notification); err != nil {
			scheduler.logger.Warn("republish recovered notification failed",
				slog.String("notification_id", notification.ID.String()),
				slog.Any("error", err),
			)
		}
	}
}

func (scheduler *Scheduler) queueDue(ctx context.Context) {
	claimed, err := scheduler.repo.ClaimDueForQueue(ctx, scheduler.staleAfter, claimBatchLimit)
	if err != nil {
		scheduler.logger.Error("claim due notifications failed", slog.Any("error", err))
		return
	}
	if len(claimed) == 0 {
		return
	}

	published := 0
	for _, notification := range claimed {
		// A failed publish leaves the row queued; the stale sweep
		// republishes it once it passes the cutoff.
		if err := scheduler.publisher.PublishCreated(ctx, notification); err != nil {
			scheduler.logger.Warn("publish due notification failed; stale sweep will retry",
				slog.String("notification_id", notification.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		published++
		// Live listeners see the scheduled→queued transition too.
		if err := scheduler.publisher.PublishEvent(ctx, rabbit.StatusEvent{
			NotificationID: notification.ID,
			Status:         string(domain.StatusQueued),
			Channel:        string(notification.Channel),
			Attempts:       notification.Attempts,
			OccurredAt:     scheduler.clock.Now(),
		}); err != nil {
			scheduler.logger.Warn("queued event publish failed", slog.Any("error", err))
		}
	}
	scheduler.logger.Info("queued due notifications",
		slog.Int("claimed", len(claimed)), slog.Int("published", published))
}

func (scheduler *Scheduler) republishStale(ctx context.Context) {
	stale, err := scheduler.repo.ListStaleQueued(ctx, scheduler.staleAfter, claimBatchLimit)
	if err != nil {
		scheduler.logger.Error("list stale queued failed", slog.Any("error", err))
		return
	}

	for _, notification := range stale {
		if err := scheduler.publisher.PublishCreated(ctx, notification); err != nil {
			scheduler.logger.Warn("republish stale notification failed",
				slog.String("notification_id", notification.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if err := scheduler.repo.TouchQueued(ctx, notification.ID); err != nil {
			scheduler.logger.Warn("touch republished notification failed",
				slog.String("notification_id", notification.ID.String()),
				slog.Any("error", err),
			)
		}
		scheduler.logger.Info("republished stale queued notification",
			slog.String("notification_id", notification.ID.String()))
	}
}
