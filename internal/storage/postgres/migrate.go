// Package postgres implements persistence: embedded schema migrations
// and the pgx-backed repositories.
package postgres

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5 driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate applies all pending migrations and reports whether any ran,
// so callers can log honestly. golang-migrate takes a Postgres advisory
// lock, so concurrently booting api and worker processes cannot race
// each other.
func Migrate(databaseURL string) (applied bool, err error) {
	source, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return false, fmt.Errorf("open embedded migrations: %w", err)
	}

	// The service uses one postgres URL everywhere; golang-migrate
	// selects its pgx/v5 driver by the pgx5:// scheme. Both URL schemes
	// pgxpool accepts must be rewritten.
	migrateURL := databaseURL
	for _, scheme := range []string{"postgresql://", "postgres://"} {
		if rest, found := strings.CutPrefix(databaseURL, scheme); found {
			migrateURL = "pgx5://" + rest
			break
		}
	}

	migrator, err := migrate.NewWithSourceInstance("iofs", source, migrateURL)
	if err != nil {
		return false, fmt.Errorf("create migrator: %w", err)
	}
	defer migrator.Close()

	if err := migrator.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return false, nil
		}
		return false, fmt.Errorf("apply migrations: %w", err)
	}
	return true, nil
}
