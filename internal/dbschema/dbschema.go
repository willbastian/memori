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

type migrationDefinition struct {
	Version  int
	Name     string
	Checksum string
}

var requiredTablesByVersion = []struct {
	version int
	tables  []string
}{
	{
		version: 1,
		tables: []string{
			"schema_meta",
			"events",
			"work_items",
			"gate_templates",
			"gate_sets",
			"gate_set_items",
		},
	},
	{
		version: 3,
		tables: []string{
			"gate_status_projection",
		},
	},
	{
		version: 4,
		tables: []string{
			"sessions",
			"rehydrate_packets",
			"agent_focus",
		},
	},
	{
		version: 5,
		tables: []string{
			"context_chunks",
			"issue_summaries",
			"open_loops",
		},
	},
	{
		version: 6,
		tables: []string{
			"human_auth_credentials",
		},
	},
	{
		version: 12,
		tables: []string{
			"gate_template_approvals",
		},
	},
	{
		version: 15,
		tables: []string{
			"schema_migrations",
		},
	},
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
	if err := syncMigrationAudit(ctx, db); err != nil {
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

	requiredTableFailures, err := verifyRequiredTables(ctx, db, status.CurrentVersion)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("required table verification failed: %v", err))
		return result, nil
	}
	if len(requiredTableFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, requiredTableFailures...)
	}

	migrationAuditFailures, err := verifyMigrationAudit(ctx, db, status.CurrentVersion)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("migration audit verification failed: %v", err))
		return result, nil
	}
	if len(migrationAuditFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, migrationAuditFailures...)
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
		result.Checks = append(result.Checks, "migration audit matches applied migrations")
		result.Checks = append(result.Checks, "event hash chain is valid")
	}
	return result, nil
}

func verifyRequiredTables(ctx context.Context, db *sql.DB, currentVersion int) ([]string, error) {
	if currentVersion <= 0 {
		return nil, nil
	}

	failures := make([]string, 0)
	for _, requirement := range requiredTablesByVersion {
		if currentVersion < requirement.version {
			continue
		}
		for _, table := range requirement.tables {
			exists, err := sqliteTableExists(ctx, db, table)
			if err != nil {
				return nil, err
			}
			if !exists {
				failures = append(failures, fmt.Sprintf("required table missing: %s", table))
			}
		}
	}
	return failures, nil
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
	expectedEntitySeq := make(map[string]int64)
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

		entityKey := entityType + "\x00" + entityID
		expectedSeq := expectedEntitySeq[entityKey]
		if expectedSeq == 0 {
			expectedSeq = 1
		}
		if entitySeq != expectedSeq {
			failures = append(
				failures,
				fmt.Sprintf(
					"entity_seq mismatch for %s:%s at event_order %d: expected %d got %d",
					entityType,
					entityID,
					eventOrder,
					expectedSeq,
					entitySeq,
				),
			)
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
		expectedEntitySeq[entityKey] = entitySeq + 1
		expectedOrder++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events rows: %w", err)
	}

	return failures, nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	var exists int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM sqlite_master
		WHERE type = 'table' AND name = ?
	`, tableName).Scan(&exists); err != nil {
		return false, fmt.Errorf("lookup table %q: %w", tableName, err)
	}
	return exists > 0, nil
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

func readMigrationDefinitions() ([]migrationDefinition, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	definitions := make([]migrationDefinition, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		version, migrationName, ok := parseMigrationFilename(name)
		if !ok {
			continue
		}
		body, err := fs.ReadFile(migrationsFS, migrationsDir+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("read embedded migration %q: %w", name, err)
		}
		sum := sha256.Sum256(body)
		definitions = append(definitions, migrationDefinition{
			Version:  version,
			Name:     migrationName,
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	return definitions, nil
}

func parseMigrationFilename(name string) (int, string, bool) {
	prefix, rest, hasUnderscore := strings.Cut(name, "_")
	if !hasUnderscore {
		return 0, "", false
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, "", false
	}
	migrationName := strings.TrimSuffix(rest, filepathExt(rest))
	if migrationName == "" {
		migrationName = rest
	}
	return version, migrationName, true
}

func filepathExt(name string) string {
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		return name[dot:]
	}
	return ""
}

func migrationDefinitionMap() (map[int]migrationDefinition, error) {
	definitions, err := readMigrationDefinitions()
	if err != nil {
		return nil, err
	}
	byVersion := make(map[int]migrationDefinition, len(definitions))
	for _, definition := range definitions {
		byVersion[definition.Version] = definition
	}
	return byVersion, nil
}

func syncMigrationAudit(ctx context.Context, db *sql.DB) error {
	exists, err := sqliteTableExists(ctx, db, "schema_migrations")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	definitions, err := migrationDefinitionMap()
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT version_id, COALESCE(tstamp, '')
		FROM goose_db_version
		WHERE is_applied = 1
		ORDER BY version_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query applied goose migrations: %w", err)
	}
	applied := make([]struct {
		version   int
		appliedAt string
	}, 0)
	for rows.Next() {
		var item struct {
			version   int
			appliedAt string
		}
		if err := rows.Scan(&item.version, &item.appliedAt); err != nil {
			return fmt.Errorf("scan applied goose migration: %w", err)
		}
		if item.version == 0 {
			continue
		}
		applied = append(applied, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate applied goose migrations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close applied goose migration rows: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for migration audit sync: %w", err)
	}
	defer tx.Rollback()

	for _, item := range applied {
		definition, ok := definitions[item.version]
		if !ok {
			return fmt.Errorf("applied migration version %d missing from embedded catalog", item.version)
		}
		if item.appliedAt == "" {
			item.appliedAt = nowUTC()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_migrations(
				version, name, checksum, applied_at, applied_by, duration_ms, success, error_message
			) VALUES(?, ?, ?, ?, ?, ?, 1, NULL)
			ON CONFLICT(version) DO UPDATE SET
				name=excluded.name,
				checksum=excluded.checksum,
				applied_at=excluded.applied_at,
				applied_by=excluded.applied_by,
				duration_ms=excluded.duration_ms,
				success=excluded.success,
				error_message=NULL
		`, item.version, definition.Name, definition.Checksum, item.appliedAt, "memori/dbschema", 0); err != nil {
			return fmt.Errorf("upsert migration audit for version %d: %w", item.version, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration audit sync tx: %w", err)
	}
	return nil
}

func verifyMigrationAudit(ctx context.Context, db *sql.DB, currentVersion int) ([]string, error) {
	if currentVersion < 15 {
		return nil, nil
	}

	definitions, err := migrationDefinitionMap()
	if err != nil {
		return nil, err
	}

	type auditRow struct {
		Version  int
		Name     string
		Checksum string
		Success  int
	}
	audits := make(map[int]auditRow)
	rows, err := db.QueryContext(ctx, `
		SELECT version, name, checksum, success
		FROM schema_migrations
		ORDER BY version ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query schema_migrations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row auditRow
		if err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.Success); err != nil {
			return nil, fmt.Errorf("scan schema_migrations row: %w", err)
		}
		audits[row.Version] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema_migrations rows: %w", err)
	}

	failures := make([]string, 0)
	gooseRows, err := db.QueryContext(ctx, `
		SELECT version_id
		FROM goose_db_version
		WHERE is_applied = 1
		ORDER BY version_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query goose_db_version for audit verify: %w", err)
	}
	defer gooseRows.Close()

	applied := make(map[int]struct{})
	for gooseRows.Next() {
		var version int
		if err := gooseRows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan goose_db_version row: %w", err)
		}
		if version == 0 {
			continue
		}
		applied[version] = struct{}{}

		definition, ok := definitions[version]
		if !ok {
			failures = append(failures, fmt.Sprintf("embedded migration missing for applied version %d", version))
			continue
		}
		audit, ok := audits[version]
		if !ok {
			failures = append(failures, fmt.Sprintf("schema_migrations missing row for version %d", version))
			continue
		}
		if audit.Name != definition.Name {
			failures = append(failures, fmt.Sprintf("schema_migrations name mismatch for version %d", version))
		}
		if audit.Checksum != definition.Checksum {
			failures = append(failures, fmt.Sprintf("schema_migrations checksum mismatch for version %d", version))
		}
		if audit.Success != 1 {
			failures = append(failures, fmt.Sprintf("schema_migrations marks version %d as unsuccessful", version))
		}
	}
	if err := gooseRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate goose_db_version rows: %w", err)
	}

	for version := range audits {
		if _, ok := applied[version]; !ok {
			failures = append(failures, fmt.Sprintf("schema_migrations has unexpected row for unapplied version %d", version))
		}
	}
	return failures, nil
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
