package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

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
