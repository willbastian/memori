package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	DefaultIssueKeyPrefix = "mem"
	MinSupportedVersion   = 1
)

func syncSchemaMeta(ctx context.Context, db *sql.DB, currentVersion int) error {
	if currentVersion <= 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for schema_meta sync: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_meta table: %w", err)
	}

	now := nowUTC()
	meta := []struct {
		key   string
		value string
	}{
		{key: "db_schema_version", value: strconv.Itoa(currentVersion)},
		{key: "min_supported_db_schema_version", value: strconv.Itoa(MinSupportedVersion)},
		{key: "last_migrated_at_utc", value: now},
	}
	for _, item := range meta {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at)
			VALUES(?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
		`, item.key, item.value, now); err != nil {
			return fmt.Errorf("upsert schema_meta %s: %w", item.key, err)
		}
	}

	var existingPrefix sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&existingPrefix)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read issue_key_prefix: %w", err)
	}
	if !existingPrefix.Valid {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at) VALUES('issue_key_prefix', ?, ?)
		`, DefaultIssueKeyPrefix, now); err != nil {
			return fmt.Errorf("insert issue_key_prefix: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema_meta sync tx: %w", err)
	}
	return nil
}

func schemaMetaVersion(ctx context.Context, db *sql.DB) (int, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'db_schema_version'`).Scan(&raw)
	if err != nil {
		return 0, fmt.Errorf("read schema_meta db_schema_version: %w", err)
	}
	version, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse schema_meta db_schema_version %q: %w", raw, err)
	}
	return version, nil
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
