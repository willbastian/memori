package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

func applyIssueCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueCreatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.created payload for event %s: %w", event.EventID, err)
	}
	referencesJSON, err := json.Marshal(normalizeReferences(payload.References))
	if err != nil {
		return fmt.Errorf("encode issue.created references payload for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status,
			description, acceptance_criteria, references_json,
			labels_json, current_cycle_no, active_gate_set_id,
			created_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, '[]', 1, NULL, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			type=excluded.type,
			title=excluded.title,
			parent_id=excluded.parent_id,
			status=excluded.status,
			description=excluded.description,
			acceptance_criteria=excluded.acceptance_criteria,
			references_json=excluded.references_json,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.Type,
		payload.Title,
		nullIfEmpty(payload.ParentID),
		payload.Status,
		strings.TrimSpace(payload.Description),
		strings.TrimSpace(payload.AcceptanceCriteria),
		string(referencesJSON),
		payload.CreatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert work_item from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyIssueUpdatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueUpdatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
	}

	setClauses := make([]string, 0, 8)
	args := make([]any, 0, 10)
	reopenTransition := false

	if payload.TitleTo != nil {
		titleTo := strings.TrimSpace(*payload.TitleTo)
		if titleTo == "" {
			return fmt.Errorf("decode issue.updated payload for event %s: --title is required", event.EventID)
		}
		setClauses = append(setClauses, "title = ?")
		args = append(args, titleTo)
	}
	if payload.StatusTo != nil {
		issueStatus, err := normalizeIssueStatus(*payload.StatusTo)
		if err != nil {
			return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "status = ?")
		args = append(args, issueStatus)
		if payload.StatusFrom != nil {
			statusFrom, err := normalizeIssueStatus(*payload.StatusFrom)
			if err != nil {
				return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
			}
			reopenTransition = statusFrom == "Done" && issueStatus != "Done"
		}
	}
	if payload.PriorityTo != nil {
		setClauses = append(setClauses, "priority = ?")
		args = append(args, nullIfEmpty(strings.TrimSpace(*payload.PriorityTo)))
	}
	if payload.LabelsTo != nil {
		labelsJSON, err := json.Marshal(normalizeLabels(*payload.LabelsTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated labels payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "labels_json = ?")
		args = append(args, string(labelsJSON))
	}
	if payload.DescriptionTo != nil {
		setClauses = append(setClauses, "description = ?")
		args = append(args, strings.TrimSpace(*payload.DescriptionTo))
	}
	if payload.AcceptanceCriteriaTo != nil {
		setClauses = append(setClauses, "acceptance_criteria = ?")
		args = append(args, strings.TrimSpace(*payload.AcceptanceCriteriaTo))
	}
	if payload.ReferencesTo != nil {
		referencesJSON, err := json.Marshal(normalizeReferences(*payload.ReferencesTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated references payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "references_json = ?")
		args = append(args, string(referencesJSON))
	}
	if reopenTransition {
		setClauses = append(setClauses, "current_cycle_no = current_cycle_no + 1", "active_gate_set_id = NULL")
	}
	if len(setClauses) == 0 {
		return fmt.Errorf("decode issue.updated payload for event %s: no mutable fields provided", event.EventID)
	}
	setClauses = append(setClauses, "updated_at = ?", "last_event_id = ?")
	args = append(args, event.CreatedAt, event.EventID, payload.IssueID)

	query := `
		UPDATE work_items
		SET ` + strings.Join(setClauses, ", ") + `
		WHERE id = ?
	`
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}
	if reopenTransition {
		if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
			return fmt.Errorf("sync reopened cycle open loops from event %s: %w", event.EventID, err)
		}
	}

	return nil
}

func applyIssueLinkedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueLinkedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	linkedParentID, err := normalizeIssueKey(payload.ParentIDTo)
	if err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET parent_id = ?, updated_at = ?, last_event_id = ?
		WHERE id = ?
	`, linkedParentID, event.CreatedAt, event.EventID, payload.IssueID)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}

	return nil
}

func applyGateEvaluatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateEvaluatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if payload.EvaluatedAt == "" {
		payload.EvaluatedAt = event.CreatedAt
	}

	var gateCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, payload.GateSetID, payload.GateID).Scan(&gateCount); err != nil {
		return fmt.Errorf("validate gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if gateCount == 0 {
		return fmt.Errorf(
			"validate gate.evaluated payload for event %s: gate %q not found in gate_set %q",
			event.EventID,
			payload.GateID,
			payload.GateSetID,
		)
	}

	evidenceJSON, err := json.Marshal(normalizeReferences(payload.EvidenceRefs))
	if err != nil {
		return fmt.Errorf("encode gate.evaluated evidence refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_status_projection(
			issue_id, gate_set_id, gate_id, result,
			evidence_refs_json, evaluated_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(issue_id, gate_set_id, gate_id) DO UPDATE SET
			result=excluded.result,
			evidence_refs_json=excluded.evidence_refs_json,
			evaluated_at=excluded.evaluated_at,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.GateSetID,
		payload.GateID,
		payload.Result,
		string(evidenceJSON),
		payload.EvaluatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert gate status projection from event %s: %w", event.EventID, err)
	}
	if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
		return fmt.Errorf("sync open loops from gate event %s: %w", event.EventID, err)
	}
	return nil
}

func applySessionCheckpointedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionCheckpointedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.checkpointed payload for event %s: %w", event.EventID, err)
	}
	checkpointJSON, err := json.Marshal(payload.Checkpoint)
	if err != nil {
		return fmt.Errorf("encode session.checkpointed checkpoint payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.checkpointed context metadata for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO sessions(
			session_id, trigger, started_at, ended_at, summary_event_id, checkpoint_json, created_by
		) VALUES(?, ?, ?, NULL, NULL, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			trigger=excluded.trigger,
			started_at=excluded.started_at,
			ended_at=NULL,
			checkpoint_json=excluded.checkpoint_json,
			created_by=excluded.created_by
	`, payload.SessionID, payload.Trigger, payload.StartedAt, string(checkpointJSON), payload.CreatedBy)
	if err != nil {
		return fmt.Errorf("upsert session from event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO context_chunks(
			chunk_id, session_id, entity_type, entity_id, kind, content, metadata_json, embedding_ref, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(chunk_id) DO UPDATE SET
			session_id=excluded.session_id,
			entity_type=excluded.entity_type,
			entity_id=excluded.entity_id,
			kind=excluded.kind,
			content=excluded.content,
			metadata_json=excluded.metadata_json,
			created_at=excluded.created_at
	`,
		payload.ContextChunkID,
		payload.SessionID,
		entityTypeSession,
		payload.SessionID,
		payload.ContextChunkKind,
		payload.ContextChunkContent,
		string(contextChunkMetaJSON),
		payload.CheckpointedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert context chunk from event %s: %w", event.EventID, err)
	}

	return nil
}

func applySessionSummarizedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionSummarizedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.summarized payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.summarized context metadata for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE sessions
		SET summary_event_id = ?
		WHERE session_id = ?
	`, event.EventID, payload.SessionID)
	if err != nil {
		return fmt.Errorf("update session summary marker from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check session summary rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update session summary marker from event %s: session %q not found", event.EventID, payload.SessionID)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO context_chunks(
			chunk_id, session_id, entity_type, entity_id, kind, content, metadata_json, embedding_ref, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(chunk_id) DO UPDATE SET
			session_id=excluded.session_id,
			entity_type=excluded.entity_type,
			entity_id=excluded.entity_id,
			kind=excluded.kind,
			content=excluded.content,
			metadata_json=excluded.metadata_json,
			created_at=excluded.created_at
	`,
		payload.ContextChunkID,
		payload.SessionID,
		entityTypeSession,
		payload.SessionID,
		payload.ContextChunkKind,
		payload.ContextChunkContent,
		string(contextChunkMetaJSON),
		payload.SummarizedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert summary context chunk from event %s: %w", event.EventID, err)
	}

	return nil
}

func applySessionClosedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionClosedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.closed payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.closed context metadata for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE sessions
		SET ended_at = ?, summary_event_id = COALESCE(NULLIF(?, ''), summary_event_id)
		WHERE session_id = ?
	`, payload.EndedAt, payload.SummaryEventID, payload.SessionID)
	if err != nil {
		return fmt.Errorf("update session closure markers from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check session closure rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update session closure markers from event %s: session %q not found", event.EventID, payload.SessionID)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO context_chunks(
			chunk_id, session_id, entity_type, entity_id, kind, content, metadata_json, embedding_ref, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(chunk_id) DO UPDATE SET
			session_id=excluded.session_id,
			entity_type=excluded.entity_type,
			entity_id=excluded.entity_id,
			kind=excluded.kind,
			content=excluded.content,
			metadata_json=excluded.metadata_json,
			created_at=excluded.created_at
	`,
		payload.ContextChunkID,
		payload.SessionID,
		entityTypeSession,
		payload.SessionID,
		payload.ContextChunkKind,
		payload.ContextChunkContent,
		string(contextChunkMetaJSON),
		payload.ClosedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert closure context chunk from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyPacketBuiltProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload packetBuiltPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode packet.built payload for event %s: %w", event.EventID, err)
	}
	packetJSON, err := json.Marshal(payload.Packet)
	if err != nil {
		return fmt.Errorf("encode packet.built packet json for event %s: %w", event.EventID, err)
	}
	scopeID := strings.TrimSpace(anyToString(payload.Packet["scope_id"]))
	issueID := strings.TrimSpace(payload.IssueID)
	sessionID := ""
	if payload.Scope == "issue" && issueID == "" {
		issueID = scopeID
	}
	if payload.Scope == "session" {
		sessionID = scopeID
	}
	issueCycleNo := payload.IssueCycleNo
	if issueCycleNo == 0 {
		if provenanceRaw, ok := payload.Packet["provenance"].(map[string]any); ok {
			issueCycleNo = anyToInt(provenanceRaw["issue_cycle_no"])
		}
		if issueCycleNo == 0 {
			if stateRaw, ok := payload.Packet["state"].(map[string]any); ok {
				issueCycleNo = anyToInt(stateRaw["cycle_no"])
			}
		}
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(
			packet_id, scope, scope_id, issue_id, session_id, issue_cycle_no,
			packet_json, packet_schema_version, built_from_event_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(packet_id) DO UPDATE SET
			scope=excluded.scope,
			scope_id=excluded.scope_id,
			issue_id=excluded.issue_id,
			session_id=excluded.session_id,
			issue_cycle_no=excluded.issue_cycle_no,
			packet_json=excluded.packet_json,
			packet_schema_version=excluded.packet_schema_version,
			built_from_event_id=excluded.built_from_event_id,
			created_at=excluded.created_at
	`, payload.PacketID, payload.Scope, nullIfEmpty(scopeID), nullIfEmpty(issueID), nullIfEmpty(sessionID), nullIfZero(issueCycleNo), string(packetJSON), payload.PacketSchemaVersion, nullIfEmpty(payload.BuiltFromEventID), payload.CreatedAt)
	if err != nil {
		return fmt.Errorf("upsert rehydrate packet from event %s: %w", event.EventID, err)
	}

	if strings.TrimSpace(payload.IssueID) != "" && payload.IssueCycleNo > 0 {
		if err := upsertIssueSummaryForPacketTx(ctx, tx, payload.IssueID, payload.IssueCycleNo, payload.Packet, payload.PacketID, payload.PacketSchemaVersion, payload.CreatedAt); err != nil {
			return fmt.Errorf("upsert issue summary from event %s: %w", event.EventID, err)
		}
	}

	return nil
}

func applyFocusUsedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload focusUsedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode focus.used payload for event %s: %w", event.EventID, err)
	}
	payload.AgentID = strings.TrimSpace(payload.AgentID)
	if payload.AgentID == "" {
		return fmt.Errorf("decode focus.used payload for event %s: agent_id is required", event.EventID)
	}
	payload.LastPacketID = strings.TrimSpace(payload.LastPacketID)
	if payload.LastPacketID == "" {
		return fmt.Errorf("decode focus.used payload for event %s: last_packet_id is required", event.EventID)
	}
	if _, err := packetByIDTx(ctx, tx, payload.LastPacketID); err != nil {
		return fmt.Errorf("validate focus.used payload for event %s: %w", event.EventID, err)
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_focus(agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			active_issue_id=excluded.active_issue_id,
			active_cycle_no=excluded.active_cycle_no,
			last_packet_id=excluded.last_packet_id,
			updated_at=excluded.updated_at
	`, payload.AgentID, nullIfEmpty(payload.ActiveIssueID), nullIfZero(payload.ActiveCycleNo), payload.LastPacketID, payload.FocusedAt)
	if err != nil {
		return fmt.Errorf("upsert agent_focus from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyGateTemplateCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateCreatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.created payload for event %s: %w", event.EventID, err)
	}
	appliesToJSON, err := json.Marshal(payload.AppliesTo)
	if err != nil {
		return fmt.Errorf("encode gate_template.created applies_to for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, payload.TemplateID, payload.Version, string(appliesToJSON), payload.DefinitionJSON, payload.DefinitionHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.DefinitionHash != payload.DefinitionHash ||
			existing.DefinitionJSON != payload.DefinitionJSON ||
			!equalStringSlices(existing.AppliesTo, payload.AppliesTo) {
			return fmt.Errorf("insert gate template from event %s: %w", event.EventID, err)
		}
	}
	if gateDefinitionContainsExecutableCommand(payload.DefinitionJSON) && actorIsHumanGoverned(payload.CreatedBy) {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
			VALUES(?, ?, ?, ?)
			ON CONFLICT(template_id, version) DO NOTHING
		`, payload.TemplateID, payload.Version, payload.CreatedAt, payload.CreatedBy); err != nil {
			return fmt.Errorf("auto-approve gate template from event %s: %w", event.EventID, err)
		}
	}
	return nil
}

func applyGateTemplateApprovedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateApprovedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.approved payload for event %s: %w", event.EventID, err)
	}

	var currentDefinitionHash string
	err = tx.QueryRowContext(ctx, `
		SELECT definition_hash
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, payload.TemplateID, payload.Version).Scan(&currentDefinitionHash)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("approve gate template from event %s: template %s@%d not found", event.EventID, payload.TemplateID, payload.Version)
	}
	if err != nil {
		return fmt.Errorf("lookup gate template for approval from event %s: %w", event.EventID, err)
	}
	if currentDefinitionHash != payload.DefinitionHash {
		return fmt.Errorf("approve gate template from event %s: definition hash mismatch for %s@%d", event.EventID, payload.TemplateID, payload.Version)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(template_id, version) DO NOTHING
	`, payload.TemplateID, payload.Version, payload.ApprovedAt, payload.ApprovedBy); err != nil {
		return fmt.Errorf("approve gate template from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetInstantiatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetInstantiatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition(payload.TemplateRefs, payload.Items)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	if len(payload.FrozenDefinition) > 0 {
		payload.FrozenDefinition = frozenObj
	}
	templateRefsJSON, err := json.Marshal(payload.TemplateRefs)
	if err != nil {
		return fmt.Errorf("encode gate_set.instantiated template refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, payload.GateSetID, payload.IssueID, payload.CycleNo, string(templateRefsJSON), frozenJSON, payload.GateSetHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.IssueID != payload.IssueID ||
			existing.CycleNo != payload.CycleNo ||
			existing.GateSetHash != payload.GateSetHash ||
			!equalStringSlices(existing.TemplateRefs, payload.TemplateRefs) {
			return fmt.Errorf("insert gate set from event %s: %w", event.EventID, err)
		}
	}

	for _, item := range payload.Items {
		criteriaJSON, err := json.Marshal(item.Criteria)
		if err != nil {
			return fmt.Errorf("encode gate_set.instantiated criteria for event %s gate %s: %w", event.EventID, item.GateID, err)
		}
		requiredInt := 0
		if item.Required {
			requiredInt = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(gate_set_id, gate_id) DO NOTHING
		`, payload.GateSetID, item.GateID, item.Kind, requiredInt, string(criteriaJSON)); err != nil {
			return fmt.Errorf("insert gate set item from event %s gate %s: %w", event.EventID, item.GateID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.CreatedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetLockedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetLockedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.locked payload for event %s: %w", event.EventID, err)
	}
	current, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
	if err != nil {
		return fmt.Errorf("lookup gate set from event %s: %w", event.EventID, err)
	}
	if !found {
		return fmt.Errorf("lookup gate set from event %s: gate set %q not found", event.EventID, payload.GateSetID)
	}
	if strings.TrimSpace(current.LockedAt) == "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE gate_sets
			SET locked_at = ?
			WHERE gate_set_id = ?
		`, payload.LockedAt, payload.GateSetID); err != nil {
			return fmt.Errorf("lock gate set from event %s: %w", event.EventID, err)
		}
	} else if current.LockedAt != payload.LockedAt {
		return fmt.Errorf("lock gate set from event %s: gate set %q already locked at %s", event.EventID, payload.GateSetID, current.LockedAt)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.LockedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from lock event %s: %w", event.EventID, err)
	}
	return nil
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
