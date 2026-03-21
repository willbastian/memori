package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) InstantiateGateSet(ctx context.Context, p InstantiateGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetCreate {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetInstantiatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, err := replayInstantiatedGateSetTx(ctx, tx, payload)
		if err != nil {
			return GateSet{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, true, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}
	templateRefs, parsedRefs, err := normalizeGateTemplateRefs(p.TemplateRefs)
	if err != nil {
		return GateSet{}, false, err
	}

	issue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return GateSet{}, false, err
	}

	var cycleNo int
	if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
		return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
	}

	existing, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found || len(existing.Items) == 0 {
		repaired, err := repairGateSetProjectionForIssueCycleTx(ctx, tx, issueID, cycleNo, false)
		if err != nil {
			return GateSet{}, false, err
		}
		if repaired {
			existing, found, err = gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
			if err != nil {
				return GateSet{}, false, err
			}
		}
	}
	if found {
		existingRefs := normalizeReferences(existing.TemplateRefs)
		if equalStringSlices(existingRefs, templateRefs) {
			if err := tx.Commit(); err != nil {
				return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return existing, true, nil
		}
		return GateSet{}, false, fmt.Errorf(
			"gate set already exists for issue %q cycle %d (existing gate_set_id %q)",
			issueID,
			cycleNo,
			existing.GateSetID,
		)
	}

	gates, err := buildGateSetDefinitionsTx(ctx, tx, issue.Type, parsedRefs)
	if err != nil {
		return GateSet{}, false, err
	}
	frozenJSON, _, err := buildFrozenGateDefinition(templateRefs, gates)
	if err != nil {
		return GateSet{}, false, err
	}
	hash := sha256.Sum256([]byte(frozenJSON))
	gateSetHash := hex.EncodeToString(hash[:])
	gateSetID := newID("gset")
	createdAt := nowUTC()

	var frozenObjCopy map[string]any
	if err := json.Unmarshal([]byte(frozenJSON), &frozenObjCopy); err != nil {
		return GateSet{}, false, fmt.Errorf("decode frozen gate definition: %w", err)
	}
	payload := gateSetInstantiatedPayload{
		GateSetID:        gateSetID,
		IssueID:          issueID,
		CycleNo:          cycleNo,
		TemplateRefs:     templateRefs,
		FrozenDefinition: frozenObjCopy,
		GateSetHash:      gateSetHash,
		CreatedAt:        createdAt,
		CreatedBy:        p.Actor,
		Items:            gates,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateSet,
		EntityID:            gateSetID,
		EventType:           eventTypeGateSetCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateSet{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateSetCreate {
		return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if !appendRes.AlreadyExists {
		if err := applyGateSetInstantiatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateSet{}, false, err
		}
	}

	gateSet, found, err := gateSetByIDTx(ctx, tx, gateSetID)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("gate set %q not found after projection", gateSetID)
	}

	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return gateSet, appendRes.AlreadyExists, nil
}

func (s *Store) LockGateSet(ctx context.Context, p LockGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetLockedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, err := replayLockedGateSetTx(ctx, tx, payload)
		if err != nil {
			return GateSet{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, false, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}

	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return GateSet{}, false, err
	}

	cycleNo := 0
	if p.CycleNo != nil {
		if *p.CycleNo <= 0 {
			return GateSet{}, false, errors.New("--cycle must be > 0")
		}
		cycleNo = *p.CycleNo
	} else {
		if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
			return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
		}
	}

	gateSet, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found || len(gateSet.Items) == 0 {
		repaired, err := repairGateSetProjectionForIssueCycleTx(ctx, tx, issueID, cycleNo, false)
		if err != nil {
			return GateSet{}, false, err
		}
		if repaired {
			gateSet, found, err = gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
			if err != nil {
				return GateSet{}, false, err
			}
		}
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("no gate set found for issue %q cycle %d", issueID, cycleNo)
	}
	if len(gateSet.Items) == 0 {
		return GateSet{}, false, fmt.Errorf("cannot lock gate set %q: no gate items defined", gateSet.GateSetID)
	}

	lockedNow := false
	lockTime := strings.TrimSpace(gateSet.LockedAt)
	if lockTime == "" {
		lockTime = nowUTC()
		payloadBytes, err := json.Marshal(gateSetLockedPayload{
			GateSetID: gateSet.GateSetID,
			IssueID:   issueID,
			CycleNo:   cycleNo,
			LockedAt:  lockTime,
		})
		if err != nil {
			return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
		}
		appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
			EntityType:          entityTypeGateSet,
			EntityID:            gateSet.GateSetID,
			EventType:           eventTypeGateSetLock,
			PayloadJSON:         string(payloadBytes),
			Actor:               p.Actor,
			CommandID:           p.CommandID,
			CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
			EventPayloadVersion: 1,
		})
		if err != nil {
			return GateSet{}, false, err
		}
		if appendRes.Event.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
		}
		if !appendRes.AlreadyExists {
			if err := applyGateSetLockedProjectionTx(ctx, tx, appendRes.Event); err != nil {
				return GateSet{}, false, err
			}
		}
		lockedNow = true
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE work_items
			SET active_gate_set_id = ?, updated_at = ?
			WHERE id = ?
		`, gateSet.GateSetID, nowUTC(), issueID); err != nil {
			return GateSet{}, false, fmt.Errorf("set active gate set for issue %q: %w", issueID, err)
		}
	}

	gateSet.LockedAt = lockTime
	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return gateSet, lockedNow, nil
}

func replayInstantiatedGateSetTx(ctx context.Context, tx *sql.Tx, payload gateSetInstantiatedPayload) (GateSet, error) {
	if _, err := repairGateSetProjectionForIssueCycleTx(ctx, tx, payload.IssueID, payload.CycleNo, false); err != nil {
		return GateSet{}, err
	}
	gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
	if err != nil {
		return GateSet{}, err
	}
	if !found {
		return GateSet{}, fmt.Errorf("gate set %q not found after replaying instantiate projection", payload.GateSetID)
	}
	return gateSet, nil
}

func replayLockedGateSetTx(ctx context.Context, tx *sql.Tx, payload gateSetLockedPayload) (GateSet, error) {
	if _, err := repairGateSetProjectionForIssueCycleTx(ctx, tx, payload.IssueID, payload.CycleNo, true); err != nil {
		return GateSet{}, err
	}
	gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
	if err != nil {
		return GateSet{}, err
	}
	if !found {
		return GateSet{}, fmt.Errorf("gate set %q not found after replaying lock projection", payload.GateSetID)
	}
	if strings.TrimSpace(gateSet.LockedAt) == "" {
		return GateSet{}, fmt.Errorf("gate set %q was not locked after replaying lock projection", payload.GateSetID)
	}
	return gateSet, nil
}
