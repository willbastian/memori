package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) BuildRehydratePacket(ctx context.Context, p BuildPacketParams) (RehydratePacket, error) {
	scope := strings.ToLower(strings.TrimSpace(p.Scope))
	if scope != "issue" && scope != "session" {
		return RehydratePacket{}, errors.New("--scope must be issue|session")
	}
	scopeID := strings.TrimSpace(p.ScopeID)
	if scopeID == "" {
		return RehydratePacket{}, errors.New("--id is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return RehydratePacket{}, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	issueIDForSummary := ""
	issueCycleNo := 0

	packetJSON := newBasePacketJSON(scope, scopeID)

	switch scope {
	case "issue":
		issueID, err := normalizeIssueKey(scopeID)
		if err != nil {
			return RehydratePacket{}, err
		}
		issue, err := getIssueTx(ctx, tx, issueID)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["scope_id"] = issueID
		packetJSON["goal"] = issue.Title
		packetJSON["state"] = map[string]any{
			"issue_id": issue.ID,
			"type":     issue.Type,
			"status":   issue.Status,
			"cycle_no": 0,
		}
		if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&issueCycleNo); err != nil {
			return RehydratePacket{}, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
		}
		state := packetJSON["state"].(map[string]any)
		state["cycle_no"] = issueCycleNo
		if strings.TrimSpace(issue.LastEventID) != "" {
			state["last_event_id"] = issue.LastEventID
		}
		issueIDForSummary = issueID
		gates, risks, nextActions, err := gateSnapshotForIssueTx(ctx, tx, issueID)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["gates"] = gates
		packetJSON["risks"] = risks
		if len(nextActions) > 0 {
			packetJSON["next_actions"] = nextActions
		}
		openLoops, err := listOpenLoopsForIssueCycleTx(ctx, tx, issueIDForSummary, issueCycleNo)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["open_loops"] = openLoopsToAny(openLoops)
		linkedWorkItems, err := listLinkedWorkItemsForIssueTx(ctx, tx, issue)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["linked_work_items"] = linkedWorkItems
		worktree, worktreeFound, err := activeWorktreeForIssueTx(ctx, tx, issueID)
		if err != nil {
			return RehydratePacket{}, err
		}
		if worktreeFound {
			packetJSON["workspace"] = buildWorktreePacketValue(worktree)
			state["worktree_id"] = worktree.WorktreeID
		}
		packetJSON["decision_summary"] = buildIssueDecisionSummary(issue, issueCycleNo, gates, openLoops, linkedWorkItems)
		packetJSON["open_questions"] = buildIssueOpenQuestions(gates, openLoops)
		relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "issue", issueIDForSummary, packetRelevantChunkLimit)
		if err != nil {
			return RehydratePacket{}, err
		}
		eventCount, err := countEventsForEntityTx(ctx, tx, entityTypeIssue, issueIDForSummary)
		if err != nil {
			return RehydratePacket{}, err
		}
		continuity := packetJSON["continuity"].(map[string]any)
		continuity["relevant_chunks"] = relevantChunks
		continuity["compaction"] = buildCompactionPolicy("issue", eventCount, countOpenLoops(openLoops), totalChunkCount)
	case "session":
		session, err := sessionByIDTx(ctx, tx, scopeID)
		if err != nil {
			return RehydratePacket{}, err
		}
		relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "session", scopeID, packetRelevantChunkLimit)
		if err != nil {
			return RehydratePacket{}, err
		}
		eventCount, err := countEventsForEntityTx(ctx, tx, entityTypeSession, scopeID)
		if err != nil {
			return RehydratePacket{}, err
		}
		status := sessionLifecycleStatus(session)
		goal := "Resume session context"
		nextActions := []any{
			"Review recent context chunks and continue execution",
		}
		if status == "closed" {
			goal = "Review closed session context"
			nextActions = []any{
				"Review how the session concluded before resuming related work",
				"Start a new session if more execution is needed",
			}
		}
		state := map[string]any{
			"session_id": scopeID,
			"status":     status,
			"started_at": session.StartedAt,
		}
		if strings.TrimSpace(session.EndedAt) != "" {
			state["ended_at"] = session.EndedAt
		}
		if strings.TrimSpace(session.SummaryEventID) != "" {
			state["summary_event_id"] = session.SummaryEventID
		}
		packetJSON["goal"] = goal
		packetJSON["state"] = state
		packetJSON["decision_summary"] = buildSessionDecisionSummary(session, totalChunkCount)
		continuity := packetJSON["continuity"].(map[string]any)
		continuity["relevant_chunks"] = relevantChunks
		continuity["compaction"] = buildCompactionPolicy("session", eventCount, 0, totalChunkCount)
		if len(relevantChunks) > 0 {
			packetJSON["next_actions"] = nextActions
		} else if status == "closed" {
			packetJSON["next_actions"] = nextActions
		}
	}

	packetID := newID("pkt")
	createdAt := nowUTC()
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return RehydratePacket{}, err
	}
	provenance := map[string]any{
		"scope":    scope,
		"scope_id": scopeID,
	}
	if latestEventID != "" {
		packetJSON["built_from_event_id"] = latestEventID
		provenance["built_from_event_id"] = latestEventID
	}
	if issueIDForSummary != "" && issueCycleNo > 0 {
		provenance["issue_id"] = issueIDForSummary
		provenance["issue_cycle_no"] = issueCycleNo
	}
	packetJSON["provenance"] = provenance
	payloadBytes, err := json.Marshal(packetBuiltPayload{
		PacketID:            packetID,
		Scope:               scope,
		Packet:              packetJSON,
		PacketSchemaVersion: packetSchemaVersion,
		BuiltFromEventID:    latestEventID,
		CreatedAt:           createdAt,
		IssueID:             issueIDForSummary,
		IssueCycleNo:        issueCycleNo,
	})
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("marshal packet.built payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypePacket,
		EntityID:            packetID,
		EventType:           eventTypePacketBuilt,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		CausationID:         latestEventID,
		CorrelationID:       packetScopeCorrelationID(scope, scopeID),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return RehydratePacket{}, err
	}
	if appendRes.Event.EventType != eventTypePacketBuilt {
		return RehydratePacket{}, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyPacketBuiltProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return RehydratePacket{}, err
	}

	packet, err := packetByIDTx(ctx, tx, appendRes.Event.EntityID)
	if err != nil {
		return RehydratePacket{}, err
	}

	if err := tx.Commit(); err != nil {
		return RehydratePacket{}, fmt.Errorf("commit tx: %w", err)
	}

	return packet, nil
}

func (s *Store) GetRehydratePacket(ctx context.Context, p GetPacketParams) (RehydratePacket, error) {
	packetID := strings.TrimSpace(p.PacketID)
	if packetID == "" {
		return RehydratePacket{}, errors.New("--packet is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	packet, err := packetByIDTx(ctx, tx, packetID)
	if err != nil {
		return RehydratePacket{}, err
	}

	if err := tx.Commit(); err != nil {
		return RehydratePacket{}, fmt.Errorf("commit tx: %w", err)
	}
	return packet, nil
}

func (s *Store) UseRehydratePacket(ctx context.Context, p UsePacketParams) (AgentFocus, RehydratePacket, bool, error) {
	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		return AgentFocus{}, RehydratePacket{}, false, errors.New("--agent is required")
	}
	packetID := strings.TrimSpace(p.PacketID)
	if packetID == "" {
		return AgentFocus{}, RehydratePacket{}, false, errors.New("--packet is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return AgentFocus{}, RehydratePacket{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	packet, err := packetByIDTx(ctx, tx, packetID)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, false, err
	}
	packetEvent, packetEventFound, err := latestEventForEntityTx(ctx, tx, entityTypePacket, packet.PacketID)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, false, err
	}

	activeIssueID := ""
	activeCycleNo := 0
	if packet.Scope == "issue" {
		if normalizedPacketIssueID := packetIssueID(packet); normalizedPacketIssueID != "" {
			normalizedIssueID, err := normalizeIssueKey(normalizedPacketIssueID)
			if err == nil {
				activeIssueID = normalizedIssueID
				if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, normalizedIssueID).Scan(&activeCycleNo); err != nil && !errors.Is(err, sql.ErrNoRows) {
					return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("read issue cycle for %q: %w", normalizedIssueID, err)
				}
			}
		}
	}

	now := nowUTC()
	payloadBytes, err := json.Marshal(focusUsedPayload{
		AgentID:       agentID,
		ActiveIssueID: activeIssueID,
		ActiveCycleNo: activeCycleNo,
		LastPacketID:  packet.PacketID,
		FocusedAt:     now,
	})
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("marshal agent focus payload: %w", err)
	}

	correlationID := packetScopeCorrelationID(packet.Scope, packetScopeID(packet))
	causationID := ""
	if packetEventFound {
		causationID = packetEvent.EventID
		if strings.TrimSpace(packetEvent.CorrelationID) != "" {
			correlationID = packetEvent.CorrelationID
		}
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeFocus,
		EntityID:            agentID,
		EventType:           eventTypeFocusUsed,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		CausationID:         causationID,
		CorrelationID:       correlationID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, false, err
	}
	if appendRes.Event.EventType != eventTypeFocusUsed {
		return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if !appendRes.AlreadyExists {
		if err := applyFocusUsedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return AgentFocus{}, RehydratePacket{}, false, err
		}
	}

	var (
		focus          AgentFocus
		activeIssueRaw sql.NullString
		activeCycleRaw sql.NullInt64
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at
		FROM agent_focus
		WHERE agent_id = ?
	`, agentID).Scan(
		&focus.AgentID,
		&activeIssueRaw,
		&activeCycleRaw,
		&focus.LastPacketID,
		&focus.UpdatedAt,
	); err != nil {
		return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("query agent_focus for %q: %w", agentID, err)
	}
	if activeIssueRaw.Valid {
		focus.ActiveIssueID = activeIssueRaw.String
	}
	if activeCycleRaw.Valid {
		focus.ActiveCycleNo = int(activeCycleRaw.Int64)
	}

	if err := tx.Commit(); err != nil {
		return AgentFocus{}, RehydratePacket{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return focus, packet, appendRes.AlreadyExists, nil
}
