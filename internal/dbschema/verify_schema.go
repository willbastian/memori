package dbschema

import (
	"context"
	"database/sql"
	"fmt"
)

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
