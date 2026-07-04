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

// ClaimForProcessing atomically claims a notification for delivery:
// guarded transition to processing, attempt counter incremented, and the
// claimed row returned — one round trip. Returns domain.ErrNotFound for
// missing rows and domain.ErrInvalidTransition when the guard rejects
// (cancelled, already sent, or concurrently processing).
func (repo *NotificationRepository) ClaimForProcessing(ctx context.Context, id uuid.UUID, allowedFrom ...domain.Status) (domain.Notification, error) {
	claim := `
		UPDATE notifications
		SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND status::text = ANY($2)
		RETURNING ` + notificationColumns

	fromValues := make([]string, len(allowedFrom))
	for i, status := range allowedFrom {
		fromValues[i] = string(status)
	}

	row := repo.pool.QueryRow(ctx, claim, id, fromValues)
	notification, err := scanNotification(row)
	if err == nil {
		return notification, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, fmt.Errorf("claim notification %s: %w", id, err)
	}

	// Zero rows: distinguish missing from guard-rejected. A failed lookup
	// surfaces as infrastructure error so callers requeue, not settle.
	_, lookupErr := repo.GetByID(ctx, id)
	switch {
	case errors.Is(lookupErr, domain.ErrNotFound):
		return domain.Notification{}, domain.ErrNotFound
	case lookupErr != nil:
		return domain.Notification{}, fmt.Errorf("disambiguate rejected claim for %s: %w", id, lookupErr)
	default:
		return domain.Notification{}, domain.ErrInvalidTransition
	}
}

// MarkRetrying records a retryable delivery failure: processing →
// retrying with the error preserved for API consumers.
func (repo *NotificationRepository) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	return repo.recordOutcome(ctx, id, domain.StatusRetrying, lastError)
}

// MarkFailed records a permanent or exhausted delivery failure.
func (repo *NotificationRepository) MarkFailed(ctx context.Context, id uuid.UUID, lastError string) error {
	return repo.recordOutcome(ctx, id, domain.StatusFailed, lastError)
}

func (repo *NotificationRepository) recordOutcome(ctx context.Context, id uuid.UUID, to domain.Status, lastError string) error {
	const record = `
		UPDATE notifications
		SET status = $1, last_error = $2, updated_at = now()
		WHERE id = $3 AND status = $4`

	tag, err := repo.pool.Exec(ctx, record, to, lastError, id, domain.StatusProcessing)
	if err != nil {
		return fmt.Errorf("mark notification %s %s: %w", id, to, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInvalidTransition
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

// ListRecent returns the newest notifications, most recent first. The
// full filtered/paginated listing extends this later.
func (repo *NotificationRepository) ListRecent(ctx context.Context, limit int) ([]domain.Notification, error) {
	query := `SELECT ` + notificationColumns + `
		FROM notifications
		ORDER BY created_at DESC, id DESC
		LIMIT $1`

	rows, err := repo.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent notifications: %w", err)
	}
	defer rows.Close()

	var notifications []domain.Notification
	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan recent notification: %w", err)
		}
		notifications = append(notifications, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list recent notifications: %w", err)
	}
	return notifications, nil
}

// WorkerPaused reads the worker pause flag.
func (repo *NotificationRepository) WorkerPaused(ctx context.Context) (bool, error) {
	var paused bool
	err := repo.pool.QueryRow(ctx, `SELECT paused FROM worker_control WHERE id = 1`).Scan(&paused)
	if err != nil {
		return false, fmt.Errorf("read worker pause flag: %w", err)
	}
	return paused, nil
}

// SetWorkerPaused flips the worker pause flag.
func (repo *NotificationRepository) SetWorkerPaused(ctx context.Context, paused bool) error {
	_, err := repo.pool.Exec(ctx,
		`UPDATE worker_control SET paused = $1, updated_at = now() WHERE id = 1`, paused)
	if err != nil {
		return fmt.Errorf("set worker pause flag: %w", err)
	}
	return nil
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
