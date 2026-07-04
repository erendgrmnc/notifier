// Package worker consumes notification messages and orchestrates
// delivery: status transitions, sending, and (in later phases) retries.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/observability"
)

// Repository is what the worker needs from persistence.
type Repository interface {
	GetByID(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status, allowedFrom ...domain.Status) error
	MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string, sentAt time.Time) error
}

// Sender delivers one notification to the external provider.
type Sender interface {
	Send(ctx context.Context, notification domain.Notification) (providerMessageID string, err error)
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
	repo   Repository
	sender Sender
	clock  Clock
	logger *slog.Logger
}

func New(repo Repository, sender Sender, clock Clock, logger *slog.Logger) *Worker {
	return &Worker{repo: repo, sender: sender, clock: clock, logger: logger}
}

// processNotification handles one consumed message. A nil return means
// the message is settled (delivered or deliberately dropped) and must be
// acked; an error means infrastructure trouble and the message should be
// redelivered.
func (worker *Worker) processNotification(ctx context.Context, id uuid.UUID) error {
	logger := observability.LoggerFrom(ctx, worker.logger).With(slog.String("notification_id", id.String()))

	notification, err := worker.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			logger.Warn("message references unknown notification; dropping")
			return nil
		}
		return fmt.Errorf("load notification: %w", err)
	}

	// The guarded transition is the idempotency gate: cancelled, already
	// sent, or concurrently processing rows all fail the guard and the
	// redelivered message is dropped without a duplicate send. The
	// allowed-from set is derived from the domain state machine (it
	// includes pending because a consumed message proves publication
	// even when the producer's queued mark has not landed yet).
	err = worker.repo.UpdateStatus(ctx, id, domain.StatusProcessing,
		domain.StatusesAllowedInto(domain.StatusProcessing)...)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) || errors.Is(err, domain.ErrNotFound) {
			logger.Info("skipping delivery", slog.String("status", string(notification.Status)))
			return nil
		}
		return fmt.Errorf("mark processing: %w", err)
	}

	providerMessageID, err := worker.sender.Send(ctx, notification)

	// The send is irreversible; its outcome is written on a detached
	// context so shutdown cannot lose what actually happened.
	outcomeCtx, cancelOutcome := context.WithTimeout(context.WithoutCancel(ctx), outcomeWriteTimeout)
	defer cancelOutcome()

	if err != nil {
		// Interim failure handling: retry tiers and error
		// classification arrive with the delivery provider phase.
		logger.Error("delivery failed", slog.Any("error", err))
		if err := worker.repo.UpdateStatus(outcomeCtx, id, domain.StatusFailed, domain.StatusProcessing); err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		return nil
	}

	if err := worker.repo.MarkSent(outcomeCtx, id, providerMessageID, worker.clock.Now()); err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			logger.Warn("sent notification changed status concurrently")
			return nil
		}
		return fmt.Errorf("mark sent: %w", err)
	}

	logger.Info("notification delivered", slog.String("provider_message_id", providerMessageID))
	return nil
}
