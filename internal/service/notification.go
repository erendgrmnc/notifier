// Package service orchestrates notification use cases between the
// transport layers and the repository.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
)

// Repository is what this service needs from persistence.
type Repository interface {
	Create(ctx context.Context, notification domain.Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (domain.Notification, error)
}

// Clock supplies time so scheduling logic is testable.
type Clock interface {
	Now() time.Time
}

// NotificationService implements the create/query use cases.
type NotificationService struct {
	repo  Repository
	clock Clock
}

func NewNotificationService(repo Repository, clock Clock) *NotificationService {
	return &NotificationService{repo: repo, clock: clock}
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
	return notification, nil
}

// Get returns one notification by ID.
func (svc *NotificationService) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	notification, err := svc.repo.GetByID(ctx, id)
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return notification, nil
}
