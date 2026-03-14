package store

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *Store) ReplayProjections(ctx context.Context) (ReplayResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return ReplayResult{}, fmt.Errorf("defer foreign keys for replay: %w", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		return ReplayResult{}, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_status_projection`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_status_projection: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_set_items`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_set_items: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_sets`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_sets: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_template_approvals`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_template_approvals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_templates`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_templates: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_focus`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear agent_focus: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rehydrate_packets`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear rehydrate_packets: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM issue_summaries`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear issue_summaries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM open_loops`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear open_loops: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_chunks`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear context_chunks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sessions`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear sessions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM work_items`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear work_items: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		ORDER BY event_order ASC
	`)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("query events for replay: %w", err)
	}
	defer rows.Close()

	applied := 0
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return ReplayResult{}, err
		}
		if err := applyEventProjectionTx(ctx, tx, event); err != nil {
			return ReplayResult{}, err
		}
		applied++
	}
	if err := rows.Err(); err != nil {
		return ReplayResult{}, fmt.Errorf("iterate replay events: %w", err)
	}
	if err := syncAllOpenLoopsTx(ctx, tx); err != nil {
		return ReplayResult{}, err
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		return ReplayResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return ReplayResult{}, fmt.Errorf("commit replay tx: %w", err)
	}

	return ReplayResult{EventsApplied: applied}, nil
}

func applyEventProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	switch event.EventType {
	case eventTypeIssueCreate:
		return applyIssueCreatedProjectionTx(ctx, tx, event)
	case eventTypeIssueUpdate:
		return applyIssueUpdatedProjectionTx(ctx, tx, event)
	case eventTypeIssueLink:
		return applyIssueLinkedProjectionTx(ctx, tx, event)
	case eventTypeGateEval:
		return applyGateEvaluatedProjectionTx(ctx, tx, event)
	case eventTypeSessionCheckpoint:
		return applySessionCheckpointedProjectionTx(ctx, tx, event)
	case eventTypeSessionSummarized:
		return applySessionSummarizedProjectionTx(ctx, tx, event)
	case eventTypeSessionClosed:
		return applySessionClosedProjectionTx(ctx, tx, event)
	case eventTypePacketBuilt:
		return applyPacketBuiltProjectionTx(ctx, tx, event)
	case eventTypeFocusUsed:
		return applyFocusUsedProjectionTx(ctx, tx, event)
	case eventTypeGateTemplateCreate:
		return applyGateTemplateCreatedProjectionTx(ctx, tx, event)
	case eventTypeGateTemplateApprove:
		return applyGateTemplateApprovedProjectionTx(ctx, tx, event)
	case eventTypeGateSetCreate:
		return applyGateSetInstantiatedProjectionTx(ctx, tx, event)
	case eventTypeGateSetLock:
		return applyGateSetLockedProjectionTx(ctx, tx, event)
	default:
		return nil
	}
}

func dropReplayProjectionDeleteTriggersTx(ctx context.Context, tx *sql.Tx) error {
	for _, triggerName := range []string{
		"gate_templates_no_delete",
		"gate_sets_no_delete",
		"gate_set_items_no_delete",
	} {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+triggerName); err != nil {
			return fmt.Errorf("drop replay delete trigger %s: %w", triggerName, err)
		}
	}
	return nil
}

func restoreReplayProjectionDeleteTriggersTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TRIGGER IF NOT EXISTS gate_templates_no_delete
			BEFORE DELETE ON gate_templates
		BEGIN
			SELECT RAISE(ABORT, 'gate_templates are immutable');
		END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_no_delete
			BEFORE DELETE ON gate_sets
		BEGIN
			SELECT RAISE(ABORT, 'gate_sets are immutable');
		END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_set_items_no_delete
			BEFORE DELETE ON gate_set_items
		BEGIN
			SELECT RAISE(ABORT, 'gate_set_items are immutable');
		END;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore replay delete trigger: %w", err)
		}
	}
	return nil
}
