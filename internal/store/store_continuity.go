package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func gateSnapshotForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) ([]any, []any, []any, error) {
	gates := make([]any, 0)
	risks := make([]any, 0)
	nextActions := make([]any, 0)

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		nextActions = append(nextActions, "Instantiate and lock a gate set for the current cycle")
		return gates, risks, nextActions, nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			i.gate_id,
			i.required,
			COALESCE(gs.result, ''),
			COALESCE(gs.evidence_refs_json, '[]'),
			COALESCE(gs.last_event_id, '')
		FROM gate_set_items i
		LEFT JOIN gate_status_projection gs
			ON gs.issue_id = ?
			AND gs.gate_set_id = i.gate_set_id
			AND gs.gate_id = i.gate_id
		WHERE i.gate_set_id = ?
		ORDER BY i.gate_id ASC
	`, issueID, gateSet.GateSetID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("query gate snapshot for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			gateID       string
			requiredInt  int
			result       string
			evidenceJSON string
			lastEventID  string
			evidenceRefs []string
		)
		if err := rows.Scan(&gateID, &requiredInt, &result, &evidenceJSON, &lastEventID); err != nil {
			return nil, nil, nil, fmt.Errorf("scan gate snapshot row for issue %q: %w", issueID, err)
		}
		if err := json.Unmarshal([]byte(evidenceJSON), &evidenceRefs); err != nil {
			return nil, nil, nil, fmt.Errorf("decode gate snapshot evidence for issue %q: %w", issueID, err)
		}
		normalized := "MISSING"
		if strings.TrimSpace(result) != "" {
			normalized = strings.ToUpper(strings.TrimSpace(result))
		}
		required := requiredInt == 1
		gates = append(gates, map[string]any{
			"gate_id":       gateID,
			"required":      required,
			"result":        normalized,
			"evidence_refs": evidenceRefs,
			"last_event_id": lastEventID,
		})
		if required && normalized != "PASS" {
			risks = append(risks, fmt.Sprintf("Required gate %s is %s", gateID, normalized))
			nextActions = append(nextActions, fmt.Sprintf("Resolve required gate %s (%s)", gateID, normalized))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("iterate gate snapshot rows for issue %q: %w", issueID, err)
	}

	if len(nextActions) == 0 {
		nextActions = append(nextActions, "All required gates are passing")
	}
	return gates, risks, nextActions, nil
}

func syncOpenLoopsForIssueFromGatesTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	cycleNo int,
	gates []any,
	sourceEventID string,
) ([]OpenLoop, error) {
	now := nowUTC()
	rows, err := tx.QueryContext(ctx, `
		SELECT loop_id, status, COALESCE(source_event_id, '')
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND loop_type = 'gate'
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query existing gate loops for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	type existingLoopState struct {
		Status        string
		SourceEventID string
	}
	existing := make(map[string]existingLoopState)
	for rows.Next() {
		var (
			loopID      string
			status      string
			loopEventID string
		)
		if err := rows.Scan(&loopID, &status, &loopEventID); err != nil {
			return nil, fmt.Errorf("scan existing gate loop row for issue %q: %w", issueID, err)
		}
		existing[loopID] = existingLoopState{
			Status:        status,
			SourceEventID: loopEventID,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing gate loops for issue %q: %w", issueID, err)
	}

	expectedOpen := make(map[string]OpenLoop)
	for _, rawGate := range gates {
		gateMap, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		gateID, _ := gateMap["gate_id"].(string)
		if strings.TrimSpace(gateID) == "" {
			continue
		}
		required, _ := gateMap["required"].(bool)
		if !required {
			continue
		}
		result, _ := gateMap["result"].(string)
		normalizedResult := strings.ToUpper(strings.TrimSpace(result))
		if normalizedResult == "PASS" {
			continue
		}
		loopID := deterministicLoopID(issueID, cycleNo, "gate", gateID)
		loopEventID, _ := gateMap["last_event_id"].(string)
		loopEventID = strings.TrimSpace(loopEventID)
		if loopEventID == "" {
			if existingState, ok := existing[loopID]; ok {
				loopEventID = strings.TrimSpace(existingState.SourceEventID)
			}
		}
		if loopEventID == "" && normalizedResult != "MISSING" {
			loopEventID = strings.TrimSpace(sourceEventID)
		}
		expectedOpen[loopID] = OpenLoop{
			LoopID:        loopID,
			IssueID:       issueID,
			CycleNo:       cycleNo,
			LoopType:      "gate",
			Status:        "Open",
			Priority:      "P1",
			SourceEventID: loopEventID,
			UpdatedAt:     now,
		}
	}

	for loopID, loop := range expectedOpen {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO open_loops(
				loop_id, issue_id, cycle_no, loop_type, status, owner, priority, source_event_id, updated_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(loop_id) DO UPDATE SET
				issue_id=excluded.issue_id,
				cycle_no=excluded.cycle_no,
				loop_type=excluded.loop_type,
				status=excluded.status,
				owner=excluded.owner,
				priority=excluded.priority,
				source_event_id=excluded.source_event_id,
				updated_at=excluded.updated_at
		`, loopID, loop.IssueID, loop.CycleNo, loop.LoopType, loop.Status, nullIfEmpty(loop.Owner), nullIfEmpty(loop.Priority), nullIfEmpty(loop.SourceEventID), loop.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("upsert open loop %q: %w", loopID, err)
		}
	}

	for loopID, state := range existing {
		if _, stillOpen := expectedOpen[loopID]; stillOpen {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(state.Status), "Resolved") {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_loops
			SET status = 'Resolved', updated_at = ?
			WHERE loop_id = ?
		`, now, loopID); err != nil {
			return nil, fmt.Errorf("resolve stale open loop %q: %w", loopID, err)
		}
	}

	loopRows, err := tx.QueryContext(ctx, `
		SELECT loop_id, issue_id, cycle_no, loop_type, status,
			COALESCE(owner, ''), COALESCE(priority, ''), COALESCE(source_event_id, ''), updated_at
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
		ORDER BY status ASC, loop_id ASC
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query synchronized loops for issue %q: %w", issueID, err)
	}
	defer loopRows.Close()

	loops := make([]OpenLoop, 0)
	for loopRows.Next() {
		var item OpenLoop
		if err := loopRows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan synchronized loop row for issue %q: %w", issueID, err)
		}
		loops = append(loops, item)
	}
	if err := loopRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate synchronized loop rows for issue %q: %w", issueID, err)
	}
	return loops, nil
}

func syncOpenLoopsForCurrentCycleTx(ctx context.Context, tx *sql.Tx, issueID, sourceEventID string) ([]OpenLoop, error) {
	var (
		cycleNo     int
		lastEventID string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT current_cycle_no, COALESCE(last_event_id, '')
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &lastEventID); err != nil {
		return nil, fmt.Errorf("query current cycle for issue %q: %w", issueID, err)
	}
	if strings.TrimSpace(sourceEventID) == "" {
		sourceEventID = lastEventID
	}
	gates, _, _, err := gateSnapshotForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, err
	}
	return syncOpenLoopsForIssueFromGatesTx(ctx, tx, issueID, cycleNo, gates, sourceEventID)
}

func syncAllOpenLoopsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, COALESCE(last_event_id, '')
		FROM work_items
	`)
	if err != nil {
		return fmt.Errorf("query work_items for open-loop sync: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var issueID string
		var lastEventID string
		if err := rows.Scan(&issueID, &lastEventID); err != nil {
			return fmt.Errorf("scan work_item for open-loop sync: %w", err)
		}
		if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, issueID, lastEventID); err != nil {
			return fmt.Errorf("sync open loops for issue %q: %w", issueID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate work_items for open-loop sync: %w", err)
	}
	return nil
}

func listOpenLoopsForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) ([]OpenLoop, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			loop_id,
			issue_id,
			cycle_no,
			loop_type,
			status,
			COALESCE(owner, ''),
			COALESCE(priority, ''),
			COALESCE(source_event_id, ''),
			updated_at
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
		ORDER BY
			CASE status WHEN 'Open' THEN 0 ELSE 1 END,
			updated_at DESC,
			loop_id ASC
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query open loops for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	defer rows.Close()

	loops := make([]OpenLoop, 0)
	for rows.Next() {
		var item OpenLoop
		if err := rows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan open loop row: %w", err)
		}
		loops = append(loops, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open loops: %w", err)
	}
	return loops, nil
}

func countOpenLoops(loops []OpenLoop) int {
	count := 0
	for _, loop := range loops {
		if strings.EqualFold(strings.TrimSpace(loop.Status), "Open") {
			count++
		}
	}
	return count
}

func deterministicLoopID(issueID string, cycleNo int, loopType, key string) string {
	sum := sha256.Sum256([]byte(issueID + ":" + strconv.Itoa(cycleNo) + ":" + loopType + ":" + key))
	return "loop_" + hex.EncodeToString(sum[:])[:12]
}
