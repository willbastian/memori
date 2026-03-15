package dbschema

import (
	"context"
	"database/sql"
	"fmt"
)

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
		`, item.version, definition.Name, definition.Checksum, item.appliedAt, "github.com/willbastian/memori/internal/dbschema", 0); err != nil {
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
