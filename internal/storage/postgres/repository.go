package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifier/internal/domain"
)

// uniqueViolationCode is Postgres SQLSTATE 23505.
const uniqueViolationCode = "23505"

// isIdempotencyKeyViolation reports whether err is the unique violation
// on the idempotency-key partial index.
func isIdempotencyKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == uniqueViolationCode &&
		pgErr.ConstraintName == "ux_notifications_idem"
}

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
		if isIdempotencyKeyViolation(err) {
			return domain.ErrDuplicateIdempotencyKey
		}
		return fmt.Errorf("insert notification: %w", err)
	}
	return nil
}

// GetByIdempotencyKey loads the notification a duplicate create collided
// with, so the original can be replayed to the client.
func (repo *NotificationRepository) GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error) {
	query := `SELECT ` + notificationColumns + ` FROM notifications WHERE idempotency_key = $1`

	row := repo.pool.QueryRow(ctx, query, key)
	notification, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Notification{}, domain.ErrNotFound
		}
		return domain.Notification{}, fmt.Errorf("select notification by idempotency key: %w", err)
	}
	return notification, nil
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

// CreateBatch inserts notifications in one transaction via COPY. All or
// nothing: a mid-batch failure (e.g. an idempotency-key race) rolls the
// whole insert back and surfaces as an error.
func (repo *NotificationRepository) CreateBatch(ctx context.Context, notifications []domain.Notification) error {
	if len(notifications) == 0 {
		return nil
	}

	tx, err := repo.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin batch insert: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"notifications"},
		[]string{"id", "batch_id", "recipient", "channel", "content", "priority",
			"status", "idempotency_key", "scheduled_at", "created_at", "updated_at"},
		pgx.CopyFromSlice(len(notifications), func(i int) ([]any, error) {
			n := notifications[i]
			return []any{n.ID, n.BatchID, n.Recipient, n.Channel, n.Content, n.Priority,
				n.Status, n.IdempotencyKey, n.ScheduledAt, n.CreatedAt, n.UpdatedAt}, nil
		}),
	)
	if err != nil {
		if isIdempotencyKeyViolation(err) {
			return domain.ErrDuplicateIdempotencyKey
		}
		return fmt.Errorf("copy batch notifications: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch insert: %w", err)
	}
	return nil
}

// ExistingIdempotencyKeys returns which of the given keys already have
// notifications, so batch creation can report duplicates per item
// instead of failing the whole COPY.
func (repo *NotificationRepository) ExistingIdempotencyKeys(ctx context.Context, keys []string) (map[string]uuid.UUID, error) {
	if len(keys) == 0 {
		return map[string]uuid.UUID{}, nil
	}

	rows, err := repo.pool.Query(ctx,
		`SELECT idempotency_key, id FROM notifications WHERE idempotency_key = ANY($1)`, keys)
	if err != nil {
		return nil, fmt.Errorf("check existing idempotency keys: %w", err)
	}
	defer rows.Close()

	existing := map[string]uuid.UUID{}
	for rows.Next() {
		var key string
		var id uuid.UUID
		if err := rows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("scan idempotency key: %w", err)
		}
		existing[key] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("check existing idempotency keys: %w", err)
	}
	return existing, nil
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

// collectNotifications drains a notification query's rows, wrapping
// errors with the operation label.
func collectNotifications(rows pgx.Rows, label string) ([]domain.Notification, error) {
	var notifications []domain.Notification
	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan %s notification: %w", label, err)
		}
		notifications = append(notifications, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s notifications: %w", label, err)
	}
	return notifications, nil
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

// List returns notifications matching the query, newest first, using
// keyset pagination on (created_at, id).
func (repo *NotificationRepository) List(ctx context.Context, listQuery domain.ListQuery) ([]domain.Notification, error) {
	sql := `SELECT ` + notificationColumns + ` FROM notifications`
	var conditions []string
	var args []any

	addCondition := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(condition, len(args)))
	}

	if listQuery.Status != "" {
		addCondition("status = $%d", listQuery.Status)
	}
	if listQuery.Channel != "" {
		addCondition("channel = $%d", listQuery.Channel)
	}
	if listQuery.BatchID != nil {
		addCondition("batch_id = $%d", *listQuery.BatchID)
	}
	if listQuery.From != nil {
		addCondition("created_at >= $%d", *listQuery.From)
	}
	if listQuery.To != nil {
		addCondition("created_at <= $%d", *listQuery.To)
	}
	if listQuery.CursorCreatedAt != nil && listQuery.CursorID != nil {
		args = append(args, *listQuery.CursorCreatedAt, *listQuery.CursorID)
		conditions = append(conditions, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}

	if len(conditions) > 0 {
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, listQuery.Limit)
	sql += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := repo.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()
	return collectNotifications(rows, "listed")
}

// ClaimDueForQueue atomically moves due work to queued and returns it
// for publishing: scheduled notifications whose time has come, plus
// pending rows older than staleAfter (created but never published —
// e.g. a broker outage at create time). SKIP LOCKED keeps concurrent
// scheduler instances from claiming the same rows.
func (repo *NotificationRepository) ClaimDueForQueue(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error) {
	claim := `
		UPDATE notifications SET status = 'queued', updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE (status = 'scheduled' AND scheduled_at <= now())
			   OR (status = 'pending' AND created_at < now() - make_interval(secs => $1))
			ORDER BY created_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING ` + notificationColumns

	rows, err := repo.pool.Query(ctx, claim, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim due notifications: %w", err)
	}
	defer rows.Close()
	return collectNotifications(rows, "due")
}

// RecoverStaleProcessing moves rows stuck in processing (a worker crash
// between claim and outcome, or a lost redelivery) back to retrying so
// they become claimable again. If the crash happened after a successful
// provider send but before MarkSent, this re-attempts an already-sent
// message — the documented at-least-once trade-off.
func (repo *NotificationRepository) RecoverStaleProcessing(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error) {
	recover := `
		UPDATE notifications
		SET status = 'retrying', last_error = 'recovered from stale processing', updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE status = 'processing' AND updated_at < now() - make_interval(secs => $1)
			ORDER BY updated_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING ` + notificationColumns

	rows, err := repo.pool.Query(ctx, recover, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("recover stale processing: %w", err)
	}
	defer rows.Close()
	return collectNotifications(rows, "stale processing")
}

// ListStaleQueued returns queued rows untouched for staleAfter — their
// publish may have been lost. Republishing is safe: the worker's claim
// guard drops duplicates.
func (repo *NotificationRepository) ListStaleQueued(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error) {
	query := `SELECT ` + notificationColumns + `
		FROM notifications
		WHERE status = 'queued' AND updated_at < now() - make_interval(secs => $1)
		ORDER BY updated_at
		LIMIT $2`

	rows, err := repo.pool.Query(ctx, query, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("list stale queued notifications: %w", err)
	}
	defer rows.Close()
	return collectNotifications(rows, "stale queued")
}

// TouchQueued refreshes a queued row's updated_at after a republish so
// the stale sweep does not pick it again immediately.
func (repo *NotificationRepository) TouchQueued(ctx context.Context, id uuid.UUID) error {
	_, err := repo.pool.Exec(ctx,
		`UPDATE notifications SET updated_at = now() WHERE id = $1 AND status = 'queued'`, id)
	if err != nil {
		return fmt.Errorf("touch queued notification %s: %w", id, err)
	}
	return nil
}

// CountNotificationStatuses returns lifetime totals per channel and
// status. Unlike in-process counters these survive restarts: the table
// is the system of record.
func (repo *NotificationRepository) CountNotificationStatuses(ctx context.Context) ([]domain.StatusCount, error) {
	rows, err := repo.pool.Query(ctx,
		`SELECT channel, status, count(*) FROM notifications GROUP BY channel, status`)
	if err != nil {
		return nil, fmt.Errorf("count notification statuses: %w", err)
	}
	defer rows.Close()

	var counts []domain.StatusCount
	for rows.Next() {
		var count domain.StatusCount
		if err := rows.Scan(&count.Channel, &count.Status, &count.Count); err != nil {
			return nil, fmt.Errorf("scan status count: %w", err)
		}
		counts = append(counts, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count notification statuses: %w", err)
	}
	return counts, nil
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
