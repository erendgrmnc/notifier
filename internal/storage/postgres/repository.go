package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifier/internal/domain"
)

// NotificationRepository persists notifications. It speaks SQL only;
// callers receive domain types and domain errors.
type NotificationRepository struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{pool: pool}
}

const notificationColumns = `
	id, batch_id, recipient, channel, content, priority, status,
	idempotency_key, scheduled_at, attempts, last_error,
	provider_message_id, created_at, updated_at, sent_at`

// Create inserts a new notification row.
func (repo *NotificationRepository) Create(ctx context.Context, notification domain.Notification) error {
	const insertNotification = `
		INSERT INTO notifications
			(id, batch_id, recipient, channel, content, priority, status,
			 idempotency_key, scheduled_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	_, err := repo.pool.Exec(ctx, insertNotification,
		notification.ID,
		notification.BatchID,
		notification.Recipient,
		notification.Channel,
		notification.Content,
		notification.Priority,
		notification.Status,
		notification.IdempotencyKey,
		notification.ScheduledAt,
		notification.CreatedAt,
		notification.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	return nil
}

// GetByID loads one notification, or domain.ErrNotFound.
func (repo *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	query := `SELECT ` + notificationColumns + ` FROM notifications WHERE id = $1`

	row := repo.pool.QueryRow(ctx, query, id)
	notification, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Notification{}, domain.ErrNotFound
		}
		return domain.Notification{}, fmt.Errorf("select notification %s: %w", id, err)
	}
	return notification, nil
}

func scanNotification(row pgx.Row) (domain.Notification, error) {
	var notification domain.Notification
	err := row.Scan(
		&notification.ID,
		&notification.BatchID,
		&notification.Recipient,
		&notification.Channel,
		&notification.Content,
		&notification.Priority,
		&notification.Status,
		&notification.IdempotencyKey,
		&notification.ScheduledAt,
		&notification.Attempts,
		&notification.LastError,
		&notification.ProviderMessageID,
		&notification.CreatedAt,
		&notification.UpdatedAt,
		&notification.SentAt,
	)
	return notification, err
}

// Connect opens a pgx pool and verifies connectivity.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
