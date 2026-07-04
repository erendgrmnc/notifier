// Package service orchestrates notification use cases between the
// transport layers and the repository.
package service

import (
	"context"
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
	UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status, allowedFrom ...domain.Status) error
	ListRecent(ctx context.Context, limit int) ([]domain.Notification, error)
}

// List limits: the API clamps rather than rejects, so dashboards can
// always ask for "the recent ones" without negotiating.
const (
	defaultListLimit = 50
	maxListLimit     = 100
)

// Publisher hands a created notification to the queue for delivery.
type Publisher interface {
	PublishCreated(ctx context.Context, notification domain.Notification) error
}

// Clock supplies time so scheduling logic is testable.
type Clock interface {
	Now() time.Time
}

// NotificationService implements the create/query use cases.
type NotificationService struct {
	repo      Repository
	publisher Publisher
	clock     Clock
	logger    *slog.Logger
}

func NewNotificationService(repo Repository, publisher Publisher, clock Clock, logger *slog.Logger) *NotificationService {
	return &NotificationService{repo: repo, publisher: publisher, clock: clock, logger: logger}
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

// Create validates the input, assigns identity and lifecycle fields,
// and persists the notification. Future deliveries start as scheduled;
// immediate ones as pending until published to the queue.
func (svc *NotificationService) Create(ctx context.Context, input CreateInput) (domain.Notification, error) {
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
		return domain.Notification{}, err
	}

	if err := svc.repo.Create(ctx, notification); err != nil {
		return domain.Notification{}, fmt.Errorf("create notification: %w", err)
	}

	// Scheduled notifications wait for the scheduler; immediate ones go
	// to the queue now. A failed publish is deliberately not an error:
	// the row stays pending and the sweeper republishes it later.
	if notification.Status == domain.StatusPending {
		notification = svc.publishForDelivery(ctx, notification)
	}
	return notification, nil
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

// ListRecent returns the newest notifications, clamping the limit to
// [1, maxListLimit] with a default when unspecified (limit <= 0).
func (svc *NotificationService) ListRecent(ctx context.Context, limit int) ([]domain.Notification, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	notifications, err := svc.repo.ListRecent(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent notifications: %w", err)
	}
	return notifications, nil
}
