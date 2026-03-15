package dbschema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func Migrate(ctx context.Context, db *sql.DB, to *int) (Status, error) {
	statusBefore, err := StatusOf(ctx, db)
	if err != nil {
		return Status{}, err
	}

	if to != nil {
		if *to < statusBefore.CurrentVersion {
			return Status{}, fmt.Errorf(
				"invalid --to %d (must be >= current version %d)",
				*to,
				statusBefore.CurrentVersion,
			)
		}
		if *to > statusBefore.HeadVersion {
			return Status{}, fmt.Errorf(
				"invalid --to %d (must be <= head version %d)",
				*to,
				statusBefore.HeadVersion,
			)
		}
	}

	if err := configureGoose(); err != nil {
		return Status{}, err
	}
	if to == nil {
		if err := goose.UpContext(ctx, db, migrationsDir); err != nil {
			return Status{}, fmt.Errorf("migrate up: %w", err)
		}
	} else {
		if err := goose.UpToContext(ctx, db, migrationsDir, int64(*to)); err != nil {
			return Status{}, fmt.Errorf("migrate up to %d: %w", *to, err)
		}
	}

	statusAfter, err := StatusOf(ctx, db)
	if err != nil {
		return Status{}, err
	}
	if err := syncSchemaMeta(ctx, db, statusAfter.CurrentVersion); err != nil {
		return Status{}, err
	}
	if err := syncMigrationAudit(ctx, db); err != nil {
		return Status{}, err
	}

	return statusAfter, nil
}

func configureGoose() error {
	goose.SetBaseFS(migrationsFS)
	goose.SetVerbose(false)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	return nil
}
