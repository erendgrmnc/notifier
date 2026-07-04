// Package worker consumes notification messages and orchestrates
// delivery: claiming, sending, retry-tier decisions, and ack/nack.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"notifier/internal/delivery"
	"notifier/internal/domain"
	"notifier/internal/observability"
)

// Repository is what the worker needs from persistence.
type Repository interface {
	ClaimForProcessing(ctx context.Context, id uuid.UUID, allowedFrom ...domain.Status) (domain.Notification, error)
	MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string, sentAt time.Time) error
	MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error
	MarkFailed(ctx context.Context, id uuid.UUID, lastError string) error
}

// Sender delivers one notification to the external provider.
type Sender interface {
	Send(ctx context.Context, notification domain.Notification) (providerMessageID string, err error)
}

// Publisher schedules retries and records dead letters.
type Publisher interface {
	PublishRetry(ctx context.Context, notification domain.Notification, attempt int) error
	PublishDeadLetter(ctx context.Context, notification domain.Notification, reason string) error
}

// PauseChecker reports whether delivery processing is paused (the
// dashboard's worker toggle, shared via the database).
type PauseChecker interface {
	WorkerPaused(ctx context.Context) (bool, error)
}

// Clock supplies time so delivery timestamps are testable.
type Clock interface {
	Now() time.Time
}

// outcomeWriteTimeout bounds the persistence of a delivery outcome. The
// write runs on a detached context: once the provider accepted (or
// definitively rejected) the message, the result must be recorded even
// if shutdown cancelled the consumer context mid-flight.
const outcomeWriteTimeout = 5 * time.Second

// Worker processes queued notifications.
type Worker struct {
	repo        Repository
	sender      Sender
	publisher   Publisher
	pause       PauseChecker
	clock       Clock
	logger      *slog.Logger
	maxAttempts int
	concurrency int
	// limiters throttle deliveries per channel. One limiter is shared by
	// all of a channel's handler goroutines, so the per-channel cap holds
	// regardless of concurrency. With N worker processes the effective
	// cap is N× the configured rate (documented in config).
	limiters   map[domain.Channel]*rate.Limiter
	lastPaused atomic.Bool
}

func New(repo Repository, sender Sender, publisher Publisher, pause PauseChecker, clock Clock, logger *slog.Logger, maxAttempts, ratePerChannel, concurrency int) *Worker {
	limiters := make(map[domain.Channel]*rate.Limiter, len(domain.Channels()))
	for _, deliveryChannel := range domain.Channels() {
		limiters[deliveryChannel] = rate.NewLimiter(rate.Limit(ratePerChannel), ratePerChannel)
	}
	return &Worker{
		repo:        repo,
		sender:      sender,
		publisher:   publisher,
		pause:       pause,
		clock:       clock,
		logger:      logger,
		maxAttempts: maxAttempts,
		concurrency: concurrency,
		limiters:    limiters,
	}
}

// processNotification handles one consumed message. A nil return means
// the message is settled (delivered, scheduled for retry, failed, or
// deliberately dropped) and must be acked; an error means infrastructure
// trouble and the message should be redelivered.
func (worker *Worker) processNotification(ctx context.Context, id uuid.UUID) error {
	logger := observability.LoggerFrom(ctx, worker.logger).With(slog.String("notification_id", id.String()))

	// The guarded claim is the idempotency gate: cancelled, already sent,
	// or concurrently processing rows are rejected and the redelivered
	// message is dropped without a duplicate send. The allowed-from set is
	// derived from the domain state machine (it includes pending because a
	// consumed message proves publication even when the producer's queued
	// mark has not landed yet).
	claimed, err := worker.repo.ClaimForProcessing(ctx, id,
		domain.StatusesAllowedInto(domain.StatusProcessing)...)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrNotFound):
			logger.Warn("message references unknown notification; dropping")
			return nil
		case errors.Is(err, domain.ErrInvalidTransition):
			logger.Info("skipping delivery; notification not claimable")
			return nil
		default:
			return fmt.Errorf("claim notification: %w", err)
		}
	}

	// Throttle before the provider call. Waiting after the claim is safe:
	// the row sits in processing and redelivery is guarded anyway.
	if err := worker.limiters[claimed.Channel].Wait(ctx); err != nil {
		// Shutdown while throttled: nothing was sent, so release the
		// claim back to retrying (on a detached context) and requeue the
		// message — the redelivered message re-claims from retrying.
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), outcomeWriteTimeout)
		defer cancelRelease()
		if releaseErr := worker.repo.MarkRetrying(releaseCtx, claimed.ID, "shutdown while rate limited"); releaseErr != nil {
			logger.Error("release throttled claim failed", slog.Any("error", releaseErr))
		}
		return fmt.Errorf("rate limit wait: %w", err)
	}

	providerMessageID, sendErr := worker.sender.Send(ctx, claimed)

	// The send is irreversible; its outcome is written on a detached
	// context so shutdown cannot lose what actually happened.
	outcomeCtx, cancelOutcome := context.WithTimeout(context.WithoutCancel(ctx), outcomeWriteTimeout)
	defer cancelOutcome()

	if sendErr != nil {
		return worker.handleSendFailure(outcomeCtx, logger, claimed, sendErr)
	}

	if err := worker.repo.MarkSent(outcomeCtx, id, providerMessageID, worker.clock.Now()); err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			logger.Warn("sent notification changed status concurrently")
			return nil
		}
		return fmt.Errorf("mark sent: %w", err)
	}

	logger.Info("notification delivered",
		slog.String("provider_message_id", providerMessageID),
		slog.Int("attempt", claimed.Attempts),
	)
	return nil
}

// handleSendFailure applies the retry policy: retryable failures with
// attempts left go to a backoff tier; permanent or exhausted ones are
// failed and dead-lettered. The database write always precedes the queue
// publish so the row never claims less than what happened.
func (worker *Worker) handleSendFailure(ctx context.Context, logger *slog.Logger, claimed domain.Notification, sendErr error) error {
	retryable := delivery.IsRetryable(sendErr)
	attemptsLeft := claimed.Attempts < worker.maxAttempts

	logger = logger.With(
		slog.Int("attempt", claimed.Attempts),
		slog.Bool("retryable", retryable),
		slog.Any("error", sendErr),
	)

	if retryable && attemptsLeft {
		if err := worker.repo.MarkRetrying(ctx, claimed.ID, sendErr.Error()); err != nil {
			if errors.Is(err, domain.ErrInvalidTransition) {
				logger.Warn("retrying notification changed status concurrently")
				return nil
			}
			return fmt.Errorf("mark retrying: %w", err)
		}
		if err := worker.publisher.PublishRetry(ctx, claimed, claimed.Attempts); err != nil {
			// The row reads retrying, which the claim guard accepts, so
			// nack-redelivery of the original message recovers this.
			return fmt.Errorf("publish retry: %w", err)
		}
		logger.Info("delivery failed; retry scheduled")
		return nil
	}

	reason := fmt.Sprintf("attempt %d/%d: %v", claimed.Attempts, worker.maxAttempts, sendErr)
	if err := worker.repo.MarkFailed(ctx, claimed.ID, sendErr.Error()); err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			logger.Warn("failed notification changed status concurrently")
			return nil
		}
		return fmt.Errorf("mark failed: %w", err)
	}
	// DLQ publish failure is logged, not retried: the database row is the
	// source of truth and already records the failure.
	if err := worker.publisher.PublishDeadLetter(ctx, claimed, reason); err != nil {
		logger.Error("dead-letter publish failed", slog.Any("error", err))
	}
	logger.Error("delivery failed permanently")
	return nil
}
