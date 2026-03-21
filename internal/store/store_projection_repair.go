package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func issueProjectionLastEventIDTx(ctx context.Context, tx *sql.Tx, issueID string) (string, error) {
	var lastEventID sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT last_event_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&lastEventID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("issue %q not found", issueID)
		}
		return "", fmt.Errorf("read projected last event for issue %q: %w", issueID, err)
	}
	if lastEventID.Valid {
		return lastEventID.String, nil
	}
	return "", nil
}

func activeGateSetIDForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (string, error) {
	var activeGateSetID sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT active_gate_set_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&activeGateSetID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("issue %q not found", issueID)
		}
		return "", fmt.Errorf("read active gate set for issue %q: %w", issueID, err)
	}
	if activeGateSetID.Valid {
		return activeGateSetID.String, nil
	}
	return "", nil
}

func latestIssueProjectionEventTx(ctx context.Context, tx *sql.Tx, issueID string) (Event, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ?
			AND entity_id = ?
			AND event_type IN (?, ?, ?)
		ORDER BY entity_seq DESC
		LIMIT 1
	`, entityTypeIssue, issueID, eventTypeIssueCreate, eventTypeIssueUpdate, eventTypeIssueLink)
	event, err := scanEvent(row)
	if err == nil {
		return event, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	return Event{}, false, fmt.Errorf("query latest issue projection event for %q: %w", issueID, err)
}

func gateSetLifecycleEventsByIDTx(ctx context.Context, tx *sql.Tx, gateSetID string) ([]Event, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ?
			AND entity_id = ?
			AND event_type IN (?, ?)
		ORDER BY event_order ASC, entity_seq ASC
	`, entityTypeGateSet, gateSetID, eventTypeGateSetCreate, eventTypeGateSetLock)
	if err != nil {
		return nil, fmt.Errorf("query gate set lifecycle events for %q: %w", gateSetID, err)
	}
	defer rows.Close()

	events := make([]Event, 0, 2)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate set lifecycle events for %q: %w", gateSetID, err)
	}
	return events, nil
}

func gateSetLifecycleEventsForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) ([]Event, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ?
			AND correlation_id = ?
			AND event_type IN (?, ?)
		ORDER BY event_order ASC, entity_seq ASC
	`, entityTypeGateSet, gateCycleCorrelationID(issueID, cycleNo), eventTypeGateSetCreate, eventTypeGateSetLock)
	if err != nil {
		return nil, fmt.Errorf("query gate set lifecycle events for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	defer rows.Close()

	events := make([]Event, 0, 2)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate set lifecycle events for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	return events, nil
}

func applyGateSetLifecycleEventsTx(ctx context.Context, tx *sql.Tx, events []Event) error {
	for _, event := range events {
		switch event.EventType {
		case eventTypeGateSetCreate:
			if err := applyGateSetInstantiatedProjectionTx(ctx, tx, event); err != nil {
				return err
			}
		case eventTypeGateSetLock:
			if err := applyGateSetLockedProjectionTx(ctx, tx, event); err != nil {
				return err
			}
		}
	}
	return nil
}

func gateSetProjectionNeedsRepairTx(ctx context.Context, tx *sql.Tx, issueID, gateSetID string, requireLocked bool) (bool, error) {
	gateSet, found, err := gateSetByIDTx(ctx, tx, gateSetID)
	if err != nil {
		return false, err
	}
	if !found || len(gateSet.Items) == 0 {
		return true, nil
	}
	if requireLocked && strings.TrimSpace(gateSet.LockedAt) == "" {
		return true, nil
	}
	if strings.TrimSpace(issueID) != "" {
		activeGateSetID, err := activeGateSetIDForIssueTx(ctx, tx, issueID)
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(activeGateSetID) != gateSetID {
			return true, nil
		}
	}
	return false, nil
}

func repairGateSetProjectionByIDTx(ctx context.Context, tx *sql.Tx, issueID, gateSetID string, requireLocked bool) (bool, error) {
	needsRepair, err := gateSetProjectionNeedsRepairTx(ctx, tx, issueID, gateSetID, requireLocked)
	if err != nil {
		return false, err
	}
	if !needsRepair {
		return false, nil
	}
	events, err := gateSetLifecycleEventsByIDTx(ctx, tx, gateSetID)
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	if err := applyGateSetLifecycleEventsTx(ctx, tx, events); err != nil {
		return false, err
	}
	return true, nil
}

func repairGateSetProjectionForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int, requireLocked bool) (bool, error) {
	events, err := gateSetLifecycleEventsForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}

	gateSetID := events[0].EntityID
	needsRepair, err := gateSetProjectionNeedsRepairTx(ctx, tx, issueID, gateSetID, requireLocked)
	if err != nil {
		return false, err
	}
	if !needsRepair {
		return false, nil
	}
	if err := applyGateSetLifecycleEventsTx(ctx, tx, events); err != nil {
		return false, err
	}
	return true, nil
}

func repairIssueProjectionThroughEventTx(ctx context.Context, tx *sql.Tx, issueID string, targetEvent Event) error {
	lastEventID, err := issueProjectionLastEventIDTx(ctx, tx, issueID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(lastEventID) == targetEvent.EventID {
		return nil
	}

	startSeq := int64(0)
	if strings.TrimSpace(lastEventID) != "" {
		currentEvent, found, err := eventByIDTx(ctx, tx, lastEventID)
		if err != nil {
			return err
		}
		if found && currentEvent.EntityType == entityTypeIssue && currentEvent.EntityID == issueID {
			if currentEvent.EventOrder >= targetEvent.EventOrder {
				return nil
			}
			startSeq = currentEvent.EntitySeq
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ?
			AND entity_id = ?
			AND entity_seq > ?
			AND entity_seq <= ?
			AND event_type IN (?, ?, ?)
		ORDER BY entity_seq ASC
	`, entityTypeIssue, issueID, startSeq, targetEvent.EntitySeq, eventTypeIssueCreate, eventTypeIssueUpdate, eventTypeIssueLink)
	if err != nil {
		return fmt.Errorf("query repair events for issue %q through %s: %w", issueID, targetEvent.EventID, err)
	}
	defer rows.Close()

	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return err
		}
		switch event.EventType {
		case eventTypeIssueCreate:
			if err := applyIssueCreatedProjectionTx(ctx, tx, event); err != nil {
				return err
			}
		case eventTypeIssueUpdate:
			if err := applyIssueUpdatedProjectionTx(ctx, tx, event); err != nil {
				return err
			}
		case eventTypeIssueLink:
			if err := applyIssueLinkedProjectionTx(ctx, tx, event); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate repair events for issue %q through %s: %w", issueID, targetEvent.EventID, err)
	}
	return nil
}

func repairGateEvaluationProjectionThroughEventTx(ctx context.Context, tx *sql.Tx, existingEvent Event, payload gateEvaluatedPayload) error {
	var lastEventID sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT last_event_id
		FROM gate_status_projection
		WHERE issue_id = ?
			AND gate_set_id = ?
			AND gate_id = ?
	`, payload.IssueID, payload.GateSetID, payload.GateID).Scan(&lastEventID)
	if err == nil && lastEventID.Valid && strings.TrimSpace(lastEventID.String) == existingEvent.EventID {
		return nil
	}
	if err == nil && lastEventID.Valid && strings.TrimSpace(lastEventID.String) != "" {
		currentOrder, found, orderErr := eventOrderByIDTx(ctx, tx, lastEventID.String)
		if orderErr != nil {
			return orderErr
		}
		if found && currentOrder >= existingEvent.EventOrder {
			return nil
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"read gate evaluation projection for issue %q gate_set %q gate %q: %w",
			payload.IssueID,
			payload.GateSetID,
			payload.GateID,
			err,
		)
	}
	return applyGateEvaluatedProjectionTx(ctx, tx, existingEvent)
}
