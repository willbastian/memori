package store

import (
	"context"
	"testing"
)

func TestAppendEventAutoLineageDefaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	first, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage"}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-1",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append first lineage event: %v", err)
	}
	if first.Event.CorrelationID != "session:sess-lineage" {
		t.Fatalf("expected default correlation id, got %#v", first.Event)
	}
	if first.Event.CausationID != "" {
		t.Fatalf("expected first lineage event to have empty causation id, got %#v", first.Event)
	}

	second, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage","step":2}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-2",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append second lineage event: %v", err)
	}
	if second.Event.CorrelationID != first.Event.CorrelationID {
		t.Fatalf("expected second event to keep correlation id %q, got %#v", first.Event.CorrelationID, second.Event)
	}
	if second.Event.CausationID != first.Event.EventID {
		t.Fatalf("expected second event causation id %q, got %#v", first.Event.EventID, second.Event)
	}

	replayed, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage","step":2}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-2",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("replay second lineage event: %v", err)
	}
	if !replayed.AlreadyExists {
		t.Fatalf("expected replayed append to be idempotent")
	}
	if replayed.Event.EventID != second.Event.EventID || replayed.Event.CorrelationID != second.Event.CorrelationID || replayed.Event.CausationID != second.Event.CausationID {
		t.Fatalf("expected replayed event to preserve lineage, got %#v want %#v", replayed.Event, second.Event)
	}
}
