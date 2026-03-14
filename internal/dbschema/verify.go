package dbschema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
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
