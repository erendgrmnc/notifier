package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// workerControlLockKey serializes concurrent boots declaring the table;
// CREATE TABLE IF NOT EXISTS alone can race when api and worker start
// simultaneously. Distinct from golang-migrate's advisory lock key.
const workerControlLockKey = 0x6e6f7469_66696572 // "notifier"

// EnsureWorkerControl declares the single-row worker_control table: the
// dashboard's pause flag and runtime provider override, shared by api
// and worker processes. This is operational state, not business schema,
// so it is declared idempotently at boot — the same treatment as the
// RabbitMQ topology — rather than carried in the versioned migrations.
func EnsureWorkerControl(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin worker_control declare: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, workerControlLockKey); err != nil {
		return fmt.Errorf("lock worker_control declare: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS worker_control (
			id           SMALLINT PRIMARY KEY CHECK (id = 1),
			paused       BOOLEAN NOT NULL DEFAULT false,
			provider_url TEXT NOT NULL DEFAULT '',
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("declare worker_control: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO worker_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
		return fmt.Errorf("seed worker_control row: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit worker_control declare: %w", err)
	}
	return nil
}
