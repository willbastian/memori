package store

import (
	"context"
	"encoding/json"
	"fmt"
)

func DecodeIssueCloseRecordFromEvent(event Event) (*IssueCloseRecord, bool, error) {
	if event.EventType != eventTypeIssueUpdate {
		return nil, false, nil
	}

	var payload issueUpdatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return nil, false, fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
	}
	return decodeIssueCloseRecord(payload, event.CreatedAt)
}

func (s *Store) LatestIssueCloseRecord(ctx context.Context, issueID string) (*IssueCloseRecord, error) {
	events, err := s.ListEventsForEntity(ctx, entityTypeIssue, issueID)
	if err != nil {
		return nil, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		record, found, err := DecodeIssueCloseRecordFromEvent(events[i])
		if err != nil {
			return nil, err
		}
		if found {
			return record, nil
		}
	}
	return nil, nil
}

func decodeIssueCloseRecord(payload issueUpdatedPayload, closedAt string) (*IssueCloseRecord, bool, error) {
	if payload.StatusTo == nil {
		return nil, false, nil
	}
	statusTo, err := normalizeIssueStatus(*payload.StatusTo)
	if err != nil {
		return nil, false, err
	}
	if statusTo != "Done" {
		return nil, false, nil
	}

	mode := payload.CloseMode
	legacyInferred := false
	if mode == "" {
		legacyInferred = true
		if payload.CloseProof != nil {
			mode = IssueCloseModeGated
		} else {
			mode = IssueCloseModeUngated
		}
	}

	return &IssueCloseRecord{
		Mode:           mode,
		Proof:          payload.CloseProof,
		ClosedAt:       closedAt,
		LegacyInferred: legacyInferred,
	}, true, nil
}
