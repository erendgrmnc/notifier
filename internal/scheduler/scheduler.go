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
)

// claimBatchLimit bounds the rows claimed per tick so one giant backlog
// cannot monopolize a tick.
const claimBatchLimit = 100

// Repository is what the scheduler needs from persistence.
type Repository interface {
	ClaimDueForQueue(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)
	ListStaleQueued(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)
	TouchQueued(ctx context.Context, id uuid.UUID) error
}

// Publisher hands claimed notifications to the queue.
type Publisher interface {
	PublishCreated(ctx context.Context, notification domain.Notification) error
}

// Scheduler polls for due and stranded work.
type Scheduler struct {
	repo         Repository
	publisher    Publisher
	logger       *slog.Logger
	pollInterval time.Duration
	staleAfter   time.Duration
}

func New(repo Repository, publisher Publisher, logger *slog.Logger, pollInterval, staleAfter time.Duration) *Scheduler {
	return &Scheduler{
		repo:         repo,
		publisher:    publisher,
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

// RunOnce performs one poll: queue due/stuck rows, then republish stale
// queued ones. Errors are logged, never fatal — the next tick retries.
func (scheduler *Scheduler) RunOnce(ctx context.Context) {
	scheduler.queueDue(ctx)
	scheduler.republishStale(ctx)
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
