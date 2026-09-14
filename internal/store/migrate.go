package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgx5migrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/SIDEYS/jobqueue/migrations"
)

// Migrate applies every pending migration in migrations/. It is safe to
// call from multiple processes (API, worker, scheduler) at once on startup:
// golang-migrate's Postgres driver takes a pg_advisory_lock for the
// duration of the run, so concurrent callers serialize instead of racing
// each other to alter the schema.
func Migrate(connString string) error {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return fmt.Errorf("migrate: open: %w", err)
	}
	defer db.Close()

	driver, err := pgx5migrate.WithInstance(db, &pgx5migrate.Config{})
	if err != nil {
		return fmt.Errorf("migrate: driver: %w", err)
	}

	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("migrate: source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("migrate: init: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}
