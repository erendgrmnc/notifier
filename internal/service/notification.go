// Package service orchestrates notification use cases between the
// transport layers and the repository.
package service

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

// Repository is what this service needs from persistence.
type Repository interface {
	Create(ctx context.Context, notification domain.Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status, allowedFrom ...domain.Status) error
	List(ctx context.Context, query domain.ListQuery) ([]domain.Notification, error)
}

// List limits: the API clamps rather than rejects, so dashboards can
// always ask for "the recent ones" without negotiating. Exported so the
// transport layer can compute pagination cursors consistently.
const (
	DefaultListLimit = 50
	MaxListLimit     = 100
)

// Publisher hands a created notification to the queue for delivery.
type Publisher interface {
	PublishCreated(ctx context.Context, notification domain.Notification) error
}

// Clock supplies time so scheduling logic is testable.
type Clock interface {
	Now() time.Time
}

// Instrumentation records business metrics; nil disables recording.
type Instrumentation interface {
	NotificationCreated(channel, priority string)
}

// NotificationService implements the create/query use cases.
type NotificationService struct {
	repo      Repository
	batchRepo BatchRepository
	publisher Publisher
	clock     Clock
	logger    *slog.Logger
	metrics   Instrumentation
}

func NewNotificationService(repo Repository, batchRepo BatchRepository, publisher Publisher, clock Clock, logger *slog.Logger, metrics Instrumentation) *NotificationService {
	return &NotificationService{repo: repo, batchRepo: batchRepo, publisher: publisher, clock: clock, logger: logger, metrics: metrics}
}

func (svc *NotificationService) recordCreated(notification domain.Notification) {
	if svc.metrics != nil {
		svc.metrics.NotificationCreated(string(notification.Channel), string(notification.Priority))
	}
}

// CreateInput carries the validated-shape request for one notification.
type CreateInput struct {
	Recipient      string
	Channel        domain.Channel
	Content        string
	Priority       domain.Priority
	ScheduledAt    *time.Time
	IdempotencyKey *string
}

// CreateResult is a created (or replayed) notification. Replayed means
// the idempotency key matched an existing notification, which is
// returned unchanged instead of creating a duplicate.
type CreateResult struct {
	Notification domain.Notification
	Replayed     bool
}

// Create validates the input, assigns identity and lifecycle fields,
// and persists the notification. Future deliveries start as scheduled;
// immediate ones as pending until published to the queue.
func (svc *NotificationService) Create(ctx context.Context, input CreateInput) (CreateResult, error) {
	now := svc.clock.Now()

	notification := domain.Notification{
		ID:             uuid.New(),
		Recipient:      input.Recipient,
		Channel:        input.Channel,
		Content:        input.Content,
		Priority:       input.Priority,
		Status:         domain.StatusPending,
		IdempotencyKey: input.IdempotencyKey,
		ScheduledAt:    input.ScheduledAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if notification.Priority == "" {
		notification.Priority = domain.PriorityNormal
	}
	if notification.ScheduledAt != nil {
		notification.Status = domain.StatusScheduled
	}

	if err := domain.ValidateNew(notification, now); err != nil {
		return CreateResult{}, err
	}

	err := svc.repo.Create(ctx, notification)
	if errors.Is(err, domain.ErrDuplicateIdempotencyKey) && input.IdempotencyKey != nil {
		existing, lookupErr := svc.repo.GetByIdempotencyKey(ctx, *input.IdempotencyKey)
		if lookupErr != nil {
			return CreateResult{}, fmt.Errorf("replay idempotent create: %w", lookupErr)
		}
		return CreateResult{Notification: existing, Replayed: true}, nil
	}
	if err != nil {
		return CreateResult{}, fmt.Errorf("create notification: %w", err)
	}
	svc.recordCreated(notification)

	// Scheduled notifications wait for the scheduler; immediate ones go
	// to the queue now. A failed publish is deliberately not an error:
	// the row stays pending and the sweeper republishes it later.
	if notification.Status == domain.StatusPending {
		notification = svc.publishForDelivery(ctx, notification)
	}
	return CreateResult{Notification: notification}, nil
}

// Cancel stops a not-yet-processing notification. The guarded update
// enforces cancellability; already-published messages are dropped by the
// worker's claim guard when they surface.
func (svc *NotificationService) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	err := svc.repo.UpdateStatus(ctx, id, domain.StatusCancelled, domain.CancellableStatuses()...)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalidTransition) {
			return domain.Notification{}, err
		}
		return domain.Notification{}, fmt.Errorf("cancel notification: %w", err)
	}

	cancelled, err := svc.repo.GetByID(ctx, id)
	if err != nil {
		return domain.Notification{}, fmt.Errorf("load cancelled notification: %w", err)
	}
	return cancelled, nil
}

// publishForDelivery hands the notification to the queue and records the
// pending → queued transition. On any failure the stored status is left
// as-is for recovery; the returned copy reflects what actually happened.
func (svc *NotificationService) publishForDelivery(ctx context.Context, notification domain.Notification) domain.Notification {
	logger := observability.LoggerFrom(ctx, svc.logger)

	if err := svc.publisher.PublishCreated(ctx, notification); err != nil {
		logger.Warn("publish failed; notification stays pending for sweeper",
			slog.String("notification_id", notification.ID.String()),
			slog.Any("error", err),
		)
		return notification
	}

	if err := svc.repo.UpdateStatus(ctx, notification.ID, domain.StatusQueued, domain.StatusPending); err != nil {
		logger.Warn("mark queued failed after publish",
			slog.String("notification_id", notification.ID.String()),
			slog.Any("error", err),
		)
		return notification
	}

	notification.Status = domain.StatusQueued
	return notification
}

// Get returns one notification by ID.
func (svc *NotificationService) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	notification, err := svc.repo.GetByID(ctx, id)
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return notification, nil
}

// List returns filtered notifications, newest first, clamping the limit
// to [1, MaxListLimit] with a default when unspecified (limit <= 0).
func (svc *NotificationService) List(ctx context.Context, query domain.ListQuery) ([]domain.Notification, error) {
	if query.Limit <= 0 {
		query.Limit = DefaultListLimit
	}
	if query.Limit > MaxListLimit {
		query.Limit = MaxListLimit
	}

	if err := query.Validate(); err != nil {
		return nil, err
	}

	notifications, err := svc.repo.List(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	return notifications, nil
}
