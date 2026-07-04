package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// UpdateStatus transitions a notification's status only if its current
// status is in the allowed set — the SQL-side enforcement of the domain
// state machine. Returns domain.ErrInvalidTransition when the guard
// rejects the write, domain.ErrNotFound when the row does not exist.
func (repo *NotificationRepository) UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status, allowedFrom ...domain.Status) error {
	const updateStatus = `
		UPDATE notifications
		SET status = $1, updated_at = now()
		WHERE id = $2 AND status::text = ANY($3)`

	fromValues := make([]string, len(allowedFrom))
	for i, status := range allowedFrom {
		fromValues[i] = string(status)
	}

	tag, err := repo.pool.Exec(ctx, updateStatus, to, id, fromValues)
	if err != nil {
		return fmt.Errorf("update notification %s status: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		// Distinguish missing row from guard rejection. A failed lookup
		// must surface as an infrastructure error — misreporting it as
		// ErrInvalidTransition would make callers settle (ack) the work.
		_, lookupErr := repo.GetByID(ctx, id)
		switch {
		case errors.Is(lookupErr, domain.ErrNotFound):
			return domain.ErrNotFound
		case lookupErr != nil:
			return fmt.Errorf("disambiguate rejected status update for %s: %w", id, lookupErr)
		default:
			return domain.ErrInvalidTransition
		}
	}
	return nil
}

// MarkSent finalizes a successful delivery: processing → sent with the
// provider's message ID and the send timestamp.
func (repo *NotificationRepository) MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string, sentAt time.Time) error {
	const markSent = `
		UPDATE notifications
		SET status = $1, provider_message_id = $2, sent_at = $3, updated_at = now()
		WHERE id = $4 AND status = $5`

	tag, err := repo.pool.Exec(ctx, markSent, domain.StatusSent, providerMessageID, sentAt, id, domain.StatusProcessing)
	if err != nil {
		return fmt.Errorf("mark notification %s sent: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInvalidTransition
	}
	return nil
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
