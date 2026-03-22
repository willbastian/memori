package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func upsertIssueSummaryForPacketTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int, packet map[string]any, packetID string, packetSchemaVersion int, createdAt string) error {
	var maxSeq int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(entity_seq), 0)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, entityTypeIssue, issueID).Scan(&maxSeq); err != nil {
		return fmt.Errorf("query max entity_seq for issue %q summary: %w", issueID, err)
	}

	summaryJSON, err := buildPacketSummaryJSON(packet, packetID, packetSchemaVersion)
	if err != nil {
		return fmt.Errorf("build packet summary for issue %q: %w", issueID, err)
	}
	parentSummaryID, err := latestPacketSummaryIDForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return err
	}
	summaryID := "sum_" + strings.TrimSpace(packetID)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO issue_summaries(
			summary_id, issue_id, cycle_no, summary_level, summary_json,
			from_entity_seq, to_entity_seq, parent_summary_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(summary_id) DO UPDATE SET
			issue_id=excluded.issue_id,
			cycle_no=excluded.cycle_no,
			summary_level=excluded.summary_level,
			summary_json=excluded.summary_json,
			from_entity_seq=excluded.from_entity_seq,
			to_entity_seq=excluded.to_entity_seq,
			parent_summary_id=excluded.parent_summary_id,
			created_at=excluded.created_at
	`,
		summaryID,
		issueID,
		cycleNo,
		"packet",
		summaryJSON,
		1,
		maxSeq,
		nullIfEmpty(parentSummaryID),
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert issue summary for issue %q: %w", issueID, err)
	}
	return nil
}

func buildPacketSummaryJSON(packet map[string]any, packetID string, packetSchemaVersion int) (string, error) {
	summary := map[string]any{
		"packet_id":             packetID,
		"packet_schema_version": packetSchemaVersion,
	}
	for _, key := range []string{
		"scope",
		"scope_id",
		"goal",
		"state",
		"decision_summary",
		"open_questions",
		"linked_work_items",
		"gates",
		"open_loops",
		"next_actions",
		"risks",
		"workspace",
		"continuity",
		"provenance",
	} {
		if value, ok := packet[key]; ok {
			summary[key] = value
		}
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func latestPacketSummaryIDForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) (string, error) {
	var summaryID sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT summary_id
		FROM issue_summaries
		WHERE issue_id = ?
			AND cycle_no = ?
			AND summary_level = 'packet'
		ORDER BY to_entity_seq DESC, created_at DESC, summary_id DESC
		LIMIT 1
	`, issueID, cycleNo).Scan(&summaryID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query latest packet summary for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	if !summaryID.Valid {
		return "", nil
	}
	return summaryID.String, nil
}

func openLoopsToAny(loops []OpenLoop) []any {
	items := make([]any, 0, len(loops))
	for _, loop := range loops {
		items = append(items, map[string]any{
			"loop_id":         loop.LoopID,
			"issue_id":        loop.IssueID,
			"cycle_no":        loop.CycleNo,
			"loop_type":       loop.LoopType,
			"status":          loop.Status,
			"owner":           loop.Owner,
			"priority":        loop.Priority,
			"source_event_id": loop.SourceEventID,
			"updated_at":      loop.UpdatedAt,
		})
	}
	return items
}

func buildIssueDecisionSummary(issue Issue, cycleNo int, gates []any, openLoops []OpenLoop, linkedWorkItems []any) map[string]any {
	gateCounts := map[string]any{
		"pass":                 0,
		"fail":                 0,
		"blocked":              0,
		"missing":              0,
		"required_outstanding": 0,
	}
	closeReady := true
	for _, rawGate := range gates {
		gate, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		result := strings.ToUpper(strings.TrimSpace(anyToString(gate["result"])))
		required, _ := gate["required"].(bool)
		switch result {
		case "PASS":
			gateCounts["pass"] = anyToInt(gateCounts["pass"]) + 1
		case "FAIL":
			gateCounts["fail"] = anyToInt(gateCounts["fail"]) + 1
		case "BLOCKED":
			gateCounts["blocked"] = anyToInt(gateCounts["blocked"]) + 1
		default:
			gateCounts["missing"] = anyToInt(gateCounts["missing"]) + 1
		}
		if required && result != "PASS" {
			gateCounts["required_outstanding"] = anyToInt(gateCounts["required_outstanding"]) + 1
			closeReady = false
		}
	}
	openLoopCount := countOpenLoops(openLoops)
	if openLoopCount > 0 {
		closeReady = false
	}

	summary := map[string]any{
		"issue_id":               issue.ID,
		"issue_type":             issue.Type,
		"status":                 issue.Status,
		"cycle_no":               cycleNo,
		"latest_event_id":        issue.LastEventID,
		"gate_counts":            gateCounts,
		"open_loop_count":        openLoopCount,
		"linked_work_item_count": len(linkedWorkItems),
		"close_ready":            closeReady,
	}
	if strings.TrimSpace(issue.ParentID) != "" {
		summary["parent_id"] = issue.ParentID
	}
	return summary
}

func buildIssueOpenQuestions(gates []any, openLoops []OpenLoop) []any {
	questions := make([]any, 0)
	for _, rawGate := range gates {
		gate, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		result := strings.ToUpper(strings.TrimSpace(anyToString(gate["result"])))
		required, _ := gate["required"].(bool)
		if !required || result == "PASS" {
			continue
		}
		gateID := strings.TrimSpace(anyToString(gate["gate_id"]))
		if gateID == "" {
			continue
		}
		questions = append(questions, map[string]any{
			"kind":     "gate",
			"gate_id":  gateID,
			"status":   result,
			"question": fmt.Sprintf("What is still needed to resolve required gate %s?", gateID),
		})
	}
	for _, loop := range openLoops {
		if !strings.EqualFold(strings.TrimSpace(loop.Status), "Open") {
			continue
		}
		questions = append(questions, map[string]any{
			"kind":       "open_loop",
			"loop_id":    loop.LoopID,
			"loop_type":  loop.LoopType,
			"owner":      loop.Owner,
			"priority":   loop.Priority,
			"updated_at": loop.UpdatedAt,
			"question":   fmt.Sprintf("What closes the %s loop for this issue cycle?", loop.LoopType),
		})
	}
	return questions
}

func buildCompactionPolicy(scope string, eventCount, openLoopCount, contextChunkCount int) map[string]any {
	reasons := make([]any, 0, 3)
	if eventCount >= compactionEventThreshold {
		reasons = append(reasons, "event-threshold")
	}
	if openLoopCount >= compactionOpenLoopThreshold {
		reasons = append(reasons, "open-loop-threshold")
	}
	if contextChunkCount >= compactionContextChunkThreshold {
		reasons = append(reasons, "context-chunk-threshold")
	}
	return map[string]any{
		"policy_version": compactionPolicyVersion,
		"mode":           compactionPolicyMode,
		"build_reason":   compactionPolicyBuildReasonOnDemand,
		"scope":          scope,
		"triggered":      len(reasons) > 0,
		"reasons":        reasons,
		"thresholds": map[string]any{
			"event_count":    compactionEventThreshold,
			"open_loops":     compactionOpenLoopThreshold,
			"context_chunks": compactionContextChunkThreshold,
		},
		"observed": map[string]any{
			"event_count":    eventCount,
			"open_loops":     openLoopCount,
			"context_chunks": contextChunkCount,
		},
	}
}

func listLinkedWorkItemsForIssueTx(ctx context.Context, tx *sql.Tx, issue Issue) ([]any, error) {
	items := make([]any, 0)
	if strings.TrimSpace(issue.ParentID) != "" {
		parent, err := getIssueTx(ctx, tx, issue.ParentID)
		if err != nil {
			return nil, err
		}
		items = append(items, issueToLinkedWorkItem(parent, "parent"))
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, type, title, status
		FROM work_items
		WHERE parent_id = ?
			AND status NOT IN ('Done', 'WontDo')
		ORDER BY id ASC
	`, issue.ID)
	if err != nil {
		return nil, fmt.Errorf("query child work items for %q: %w", issue.ID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var childID, childType, title, status string
		if err := rows.Scan(&childID, &childType, &title, &status); err != nil {
			return nil, fmt.Errorf("scan child work item for %q: %w", issue.ID, err)
		}
		items = append(items, map[string]any{
			"relation": "child",
			"issue_id": childID,
			"type":     childType,
			"title":    title,
			"status":   status,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate child work items for %q: %w", issue.ID, err)
	}
	return items, nil
}

func issueToLinkedWorkItem(issue Issue, relation string) map[string]any {
	item := map[string]any{
		"relation": relation,
		"issue_id": issue.ID,
		"type":     issue.Type,
		"title":    issue.Title,
		"status":   issue.Status,
	}
	if strings.TrimSpace(issue.ParentID) != "" {
		item["parent_id"] = issue.ParentID
	}
	return item
}

func listRelevantContextChunksTx(ctx context.Context, tx *sql.Tx, scope, scopeID string, limit int) ([]any, int, error) {
	if limit <= 0 {
		limit = packetRelevantChunkLimit
	}

	var (
		countQuery string
		listQuery  string
		countArgs  []any
		listArgs   []any
	)
	switch scope {
	case "session":
		countQuery = `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`
		listQuery = `
			SELECT chunk_id, kind, content, metadata_json, created_at
			FROM context_chunks
			WHERE session_id = ?
			ORDER BY created_at DESC, chunk_id DESC
			LIMIT ?
		`
		countArgs = []any{scopeID}
		listArgs = []any{scopeID, limit}
	case "issue":
		countQuery = `SELECT COUNT(1) FROM context_chunks WHERE entity_type = ? AND entity_id = ?`
		listQuery = `
			SELECT chunk_id, kind, content, metadata_json, created_at
			FROM context_chunks
			WHERE entity_type = ? AND entity_id = ?
			ORDER BY created_at DESC, chunk_id DESC
			LIMIT ?
		`
		countArgs = []any{entityTypeIssue, scopeID}
		listArgs = []any{entityTypeIssue, scopeID, limit}
	default:
		return []any{}, 0, nil
	}

	var total int
	if err := tx.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count context chunks for %s %q: %w", scope, scopeID, err)
	}

	rows, err := tx.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query context chunks for %s %q: %w", scope, scopeID, err)
	}
	defer rows.Close()

	chunks := make([]map[string]any, 0)
	for rows.Next() {
		var chunkID, kind, content, metadataJSON, createdAt string
		if err := rows.Scan(&chunkID, &kind, &content, &metadataJSON, &createdAt); err != nil {
			return nil, 0, fmt.Errorf("scan context chunk for %s %q: %w", scope, scopeID, err)
		}
		metadata := make(map[string]any)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return nil, 0, fmt.Errorf("decode context chunk metadata for %s %q: %w", scope, scopeID, err)
		}
		chunks = append(chunks, map[string]any{
			"chunk_id":   chunkID,
			"kind":       kind,
			"content":    content,
			"metadata":   metadata,
			"created_at": createdAt,
			"relevance":  "recent",
			"scope":      scope,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate context chunks for %s %q: %w", scope, scopeID, err)
	}

	items := make([]any, 0, len(chunks))
	for i := len(chunks) - 1; i >= 0; i-- {
		items = append(items, chunks[i])
	}
	return items, total, nil
}

func countEventsForEntityTx(ctx context.Context, tx *sql.Tx, entityType, entityID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, entityType, entityID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count events for %s %q: %w", entityType, entityID, err)
	}
	return count, nil
}
