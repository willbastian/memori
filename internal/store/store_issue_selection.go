package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func loadIssueNextContinuitySignalsTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issueLastEventID string,
	currentCycleNo int,
	focus AgentFocus,
) (issueNextContinuitySignals, error) {
	signals := issueNextContinuitySignals{
		CurrentCycleNo: currentCycleNo,
	}
	if focus.ActiveIssueID == issueID && (focus.ActiveCycleNo == 0 || focus.ActiveCycleNo == currentCycleNo) {
		signals.FocusMatch = true
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND status = 'Open'
	`, issueID, currentCycleNo).Scan(&signals.OpenLoopCount); err != nil {
		return issueNextContinuitySignals{}, fmt.Errorf("query open loop count for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}

	lockedGateSet, found, err := lockedGateSetForIssueCycleTx(ctx, tx, issueID, currentCycleNo)
	if err != nil {
		return issueNextContinuitySignals{}, err
	}
	if found {
		rows, err := tx.QueryContext(ctx, `
			SELECT COALESCE(gs.result, '')
			FROM gate_set_items i
			LEFT JOIN gate_status_projection gs
				ON gs.issue_id = ?
				AND gs.gate_set_id = i.gate_set_id
				AND gs.gate_id = i.gate_id
			WHERE i.gate_set_id = ?
				AND i.required = 1
		`, issueID, lockedGateSet.GateSetID)
		if err != nil {
			return issueNextContinuitySignals{}, fmt.Errorf("query gate health for issue %q: %w", issueID, err)
		}
		defer rows.Close()

		for rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				return issueNextContinuitySignals{}, fmt.Errorf("scan gate health for issue %q: %w", issueID, err)
			}
			switch strings.TrimSpace(result) {
			case "FAIL":
				signals.FailingRequiredGates++
			case "BLOCKED":
				signals.BlockedRequiredGates++
			case "", "MISSING":
				signals.MissingRequiredGates++
			}
		}
		if err := rows.Err(); err != nil {
			return issueNextContinuitySignals{}, fmt.Errorf("iterate gate health for issue %q: %w", issueID, err)
		}
	}

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "issue", issueID)
	if err != nil {
		return issueNextContinuitySignals{}, err
	}
	if found {
		packetCycleNo := issuePacketCycleNo(packet)
		if focus.LastPacketID != "" && focus.LastPacketID == packet.PacketID {
			signals.FocusPacketMatch = true
		}
		packetEventOrder, packetEventFound, err := eventOrderByIDTx(ctx, tx, packet.BuiltFromEventID)
		if err != nil {
			return issueNextContinuitySignals{}, err
		}
		latestContinuityOrder, err := latestContinuityEventOrderForIssueCycleTx(ctx, tx, issueID, issueLastEventID, currentCycleNo)
		if err != nil {
			return issueNextContinuitySignals{}, err
		}
		if packetCycleNo == currentCycleNo && ((packetEventFound && packetEventOrder >= latestContinuityOrder) || (!packetEventFound && latestContinuityOrder == 0)) {
			signals.HasFreshPacket = true
		} else {
			signals.HasStalePacket = true
		}
	}

	return signals, nil
}

func issuePacketCycleNo(packet RehydratePacket) int {
	if packet.IssueCycleNo > 0 {
		return packet.IssueCycleNo
	}
	if packet.Packet == nil {
		return 0
	}
	if provenanceRaw, ok := packet.Packet["provenance"].(map[string]any); ok {
		if cycleNo := anyToInt(provenanceRaw["issue_cycle_no"]); cycleNo > 0 {
			return cycleNo
		}
	}
	if stateRaw, ok := packet.Packet["state"].(map[string]any); ok {
		return anyToInt(stateRaw["cycle_no"])
	}
	return 0
}

func anyToInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if v, err := typed.Int64(); err == nil {
			return int(v)
		}
	case string:
		if v, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			return v
		}
	}
	return 0
}

func anyToString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", value)
	}
}

func scoreIssueCandidate(issue Issue, priority string, signals issueNextContinuitySignals) (int, []string) {
	score := 0
	reasons := make([]string, 0, 8)

	switch issue.Status {
	case "InProgress":
		score += 100
		reasons = append(reasons, "in-progress work is prioritized for continuity")
	case "Todo":
		score += 50
		reasons = append(reasons, "todo work is actionable")
	}

	switch issue.Type {
	case "Task":
		score += 40
		reasons = append(reasons, "task is implementation-ready")
	case "Bug":
		score += 35
		reasons = append(reasons, "bug fix has high operational value")
	case "Story":
		score += 20
		reasons = append(reasons, "story provides cross-task scope")
	case "Epic":
		score += 10
		reasons = append(reasons, "epic is planning-level work")
	}

	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "P0":
		score += 30
		reasons = append(reasons, "priority P0")
	case "P1":
		score += 20
		reasons = append(reasons, "priority P1")
	case "P2":
		score += 10
		reasons = append(reasons, "priority P2")
	}

	if issue.ParentID == "" && (issue.Type == "Task" || issue.Type == "Bug") {
		score += 5
		reasons = append(reasons, "standalone item can start immediately")
	}

	if signals.FocusMatch {
		score += 120
		reasons = append(reasons, "matches the agent's active focus for resume")
	}
	if signals.FocusPacketMatch {
		score += 35
		reasons = append(reasons, "agent already holds the latest recovery packet")
	}
	if signals.OpenLoopCount > 0 {
		boost := signals.OpenLoopCount * 15
		if boost > 45 {
			boost = 45
		}
		score += boost
		reasons = append(reasons, fmt.Sprintf("has %d open loop(s) that need continuity", signals.OpenLoopCount))
	}
	if signals.FailingRequiredGates > 0 {
		score += 40
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) are failing", signals.FailingRequiredGates))
	}
	if signals.BlockedRequiredGates > 0 {
		score += 30
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) are blocked", signals.BlockedRequiredGates))
	}
	if signals.MissingRequiredGates > 0 {
		score += 15
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) still need evaluation", signals.MissingRequiredGates))
	}
	if signals.HasFreshPacket {
		score += 20
		reasons = append(reasons, "fresh issue packet is ready for recovery")
	} else if signals.HasStalePacket {
		score -= 5
		reasons = append(reasons, "available issue packet is stale")
	}
	return score, reasons
}
