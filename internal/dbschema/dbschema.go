package dbschema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
)

const (
	DefaultIssueKeyPrefix = "mem"
	MinSupportedVersion   = 1
	migrationsDir         = "migrations"
)

type Status struct {
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type VerifyResult struct {
	OK                bool     `json:"ok"`
	CurrentVersion    int      `json:"current_version"`
	HeadVersion       int      `json:"head_version"`
	SchemaMetaVersion int      `json:"schema_meta_version"`
	Checks            []string `json:"checks"`
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

func StatusOf(ctx context.Context, db *sql.DB) (Status, error) {
	headVersion, err := headVersion()
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

	return statusAfter, nil
}

func Verify(ctx context.Context, db *sql.DB) (VerifyResult, error) {
	status, err := StatusOf(ctx, db)
	if err != nil {
		return VerifyResult{}, err
	}

	result := VerifyResult{
		OK:             true,
		CurrentVersion: status.CurrentVersion,
		HeadVersion:    status.HeadVersion,
		Checks:         make([]string, 0, 4),
	}

	metaVersion, err := schemaMetaVersion(ctx, db)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, err.Error())
		return result, nil
	}
	result.SchemaMetaVersion = metaVersion

	if status.CurrentVersion == 0 {
		result.OK = false
		result.Checks = append(result.Checks, "database has no applied migrations")
	}
	if metaVersion != status.CurrentVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("schema_meta db_schema_version=%d does not match goose version=%d", metaVersion, status.CurrentVersion),
		)
	}
	if status.CurrentVersion > status.HeadVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("database version %d is ahead of binary head %d", status.CurrentVersion, status.HeadVersion),
		)
	}

	hashChainFailures, err := verifyEventHashChain(ctx, db)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("event hash-chain verification failed: %v", err))
		return result, nil
	}
	if len(hashChainFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, hashChainFailures...)
	}

	if result.OK {
		result.Checks = append(result.Checks, "schema versions are consistent")
		result.Checks = append(result.Checks, "event hash chain is valid")
	}
	return result, nil
}

func verifyEventHashChain(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			event_order,
			entity_type,
			entity_id,
			entity_seq,
			event_type,
			payload_json,
			actor,
			command_id,
			COALESCE(causation_id, ''),
			COALESCE(correlation_id, ''),
			created_at,
			COALESCE(prev_hash, ''),
			hash,
			event_payload_version
		FROM events
		ORDER BY event_order ASC
	`)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table: events") {
			return []string{"required table missing: events"}, nil
		}
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	failures := make([]string, 0)
	expectedOrder := int64(1)
	previousHash := ""

	for rows.Next() {
		var (
			eventOrder          int64
			entityType          string
			entityID            string
			entitySeq           int64
			eventType           string
			payloadJSON         string
			actor               string
			commandID           string
			causationID         string
			correlationID       string
			createdAt           string
			prevHash            string
			hash                string
			eventPayloadVersion int
		)
		if err := rows.Scan(
			&eventOrder,
			&entityType,
			&entityID,
			&entitySeq,
			&eventType,
			&payloadJSON,
			&actor,
			&commandID,
			&causationID,
			&correlationID,
			&createdAt,
			&prevHash,
			&hash,
			&eventPayloadVersion,
		); err != nil {
			return nil, fmt.Errorf("scan events row: %w", err)
		}

		if eventOrder != expectedOrder {
			failures = append(
				failures,
				fmt.Sprintf("event_order mismatch at row %d: expected %d got %d", expectedOrder, expectedOrder, eventOrder),
			)
			expectedOrder = eventOrder
		}

		if prevHash != previousHash {
			failures = append(
				failures,
				fmt.Sprintf(
					"prev_hash mismatch at event_order %d: expected %q got %q",
					eventOrder,
					previousHash,
					prevHash,
				),
			)
		}

		calculatedHash := computeEventHash(
			eventOrder,
			entityType,
			entityID,
			entitySeq,
			eventType,
			payloadJSON,
			actor,
			commandID,
			causationID,
			correlationID,
			createdAt,
			prevHash,
			eventPayloadVersion,
		)
		if hash != calculatedHash {
			failures = append(
				failures,
				fmt.Sprintf("hash mismatch at event_order %d", eventOrder),
			)
		}

		previousHash = hash
		expectedOrder++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events rows: %w", err)
	}

	return failures, nil
}

func computeEventHash(
	order int64,
	entityType, entityID string,
	seq int64,
	eventType, payloadJSON, actor, commandID, causationID, correlationID, createdAt, prevHash string,
	eventPayloadVersion int,
) string {
	h := sha256.New()
	parts := []string{
		strconv.FormatInt(order, 10),
		entityType,
		entityID,
		strconv.FormatInt(seq, 10),
		eventType,
		payloadJSON,
		actor,
		commandID,
		causationID,
		correlationID,
		createdAt,
		prevHash,
		strconv.Itoa(eventPayloadVersion),
	}
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
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

func headVersion() (int, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return 0, fmt.Errorf("read embedded migrations: %w", err)
	}

	maxVersion := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		prefix, _, hasUnderscore := strings.Cut(name, "_")
		if !hasUnderscore {
			continue
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			continue
		}
		if version > maxVersion {
			maxVersion = version
		}
	}
	return maxVersion, nil
}

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
