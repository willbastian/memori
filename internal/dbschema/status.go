package dbschema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func StatusOf(ctx context.Context, db *sql.DB) (Status, error) {
	headVersion, err := HeadVersion()
	if err != nil {
		return Status{}, err
	}
	currentVersion, err := currentVersion(ctx, db)
	if err != nil {
		return Status{}, err
	}
	pending := headVersion - currentVersion
	if pending < 0 {
		pending = 0
	}
	return Status{
		CurrentVersion:    currentVersion,
		HeadVersion:       headVersion,
		PendingMigrations: pending,
	}, nil
}

func currentVersion(ctx context.Context, db *sql.DB) (int, error) {
	var currentVersion int
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version_id), 0)
		FROM goose_db_version
		WHERE is_applied = 1
	`).Scan(&currentVersion)
	if err != nil {
		// Database has not been migrated yet.
		if strings.Contains(strings.ToLower(err.Error()), "no such table: goose_db_version") {
			return 0, nil
		}
		return 0, fmt.Errorf("read goose_db_version: %w", err)
	}
	return currentVersion, nil
}
