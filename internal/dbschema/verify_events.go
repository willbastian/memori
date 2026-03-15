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
