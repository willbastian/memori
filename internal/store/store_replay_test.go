package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestReplayProjectionsDeterministicAcrossRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Type:      "epic",
		Title:     "Epic one",
		Actor:     "agent-1",
		CommandID: "cmd-epic-1",
	})
	if err != nil {
		t.Fatalf("create epic_1: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b2c3d4e",
		Type:      "story",
		Title:     "Story one",
		ParentID:  "mem-a1b2c3d",
		Actor:     "agent-1",
		CommandID: "cmd-task-1",
	})
	if err != nil {
		t.Fatalf("create mem-b2c3d4e story: %v", err)
	}

	beforeEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d before replay: %v", err)
	}
	beforeTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e before replay: %v", err)
	}

	replay1, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if replay1.EventsApplied != 2 {
		t.Fatalf("expected first replay to apply 2 events, got %d", replay1.EventsApplied)
	}

	afterFirstEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d after first replay: %v", err)
	}
	afterFirstTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e after first replay: %v", err)
	}
	assertIssueEqual(t, beforeEpic, afterFirstEpic)
	assertIssueEqual(t, beforeTask, afterFirstTask)

	replay2, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if replay2.EventsApplied != 2 {
		t.Fatalf("expected second replay to apply 2 events, got %d", replay2.EventsApplied)
	}

	afterSecondEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d after second replay: %v", err)
	}
	afterSecondTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e after second replay: %v", err)
	}
	assertIssueEqual(t, afterFirstEpic, afterSecondEpic)
	assertIssueEqual(t, afterFirstTask, afterSecondTask)
}

func TestReplayProjectionsSurfacesMalformedEventPayloads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-f1f1f1f",
		Type:      "task",
		Title:     "Replay corruption check",
		Actor:     "agent-1",
		CommandID: "cmd-replay-malformed-1",
	}); err != nil {
		t.Fatalf("create issue for replay corruption test: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_replay_bad_payload", 2, "issue", "mem-f2f2f2f", 1, "issue.created", `{"issue_id":"mem-f2f2f2f","type":"Feature","title":"Bad replay type","status":"Todo","created_at":"2026-03-08T00:00:00Z"}`, "agent-1", "cmd-replay-malformed-2", nowUTC(), "hash_replay_bad_payload", 1); err != nil {
		t.Fatalf("insert invalid projected issue.created event: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "upsert work_item from event") {
		t.Fatalf("expected replay projection failure, got %v", err)
	}
}

func TestReplayProjectionsFailsOnClosedDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("expected replay closed-db error, got %v", err)
	}
}

func TestReplayProjectionsAppliesIssueUpdatedEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-aaaaaaa",
		Type:      "task",
		Title:     "Replay updated fields",
		Actor:     "agent-1",
		CommandID: "cmd-replay-update-create",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	newTitle := "Replay updated fields v2"
	newDescription := "updated during replay"
	newAcceptance := "preserve replayed rich context"
	newPriority := "P1"
	newLabels := []string{"db", "cli"}
	newReferences := []string{"https://example.com/spec", "docs/replay.md"}
	updated, event, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:            "mem-aaaaaaa",
		Title:              &newTitle,
		Description:        &newDescription,
		AcceptanceCriteria: &newAcceptance,
		Priority:           &newPriority,
		Labels:             &newLabels,
		References:         &newReferences,
		Actor:              "agent-1",
		CommandID:          "cmd-replay-update-1",
	})
	if err != nil {
		t.Fatalf("update issue: %v", err)
	}
	if updated.Title != newTitle {
		t.Fatalf("expected updated title, got %q", updated.Title)
	}
	if event.EventType != eventTypeIssueUpdate {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}

	replayed, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replayed.EventsApplied != 2 {
		t.Fatalf("expected replay to apply 2 events, got %d", replayed.EventsApplied)
	}

	reloaded, err := s.GetIssue(ctx, "mem-aaaaaaa")
	if err != nil {
		t.Fatalf("get replayed issue: %v", err)
	}
	if reloaded.Title != newTitle {
		t.Fatalf("expected title after replay %q, got %q", newTitle, reloaded.Title)
	}
	if reloaded.Description != newDescription {
		t.Fatalf("expected description after replay %q, got %q", newDescription, reloaded.Description)
	}
	if reloaded.Acceptance != newAcceptance {
		t.Fatalf("expected acceptance after replay %q, got %q", newAcceptance, reloaded.Acceptance)
	}
	if reloaded.Priority != newPriority {
		t.Fatalf("expected priority after replay %q, got %q", newPriority, reloaded.Priority)
	}
	if !reflect.DeepEqual(reloaded.Labels, newLabels) {
		t.Fatalf("expected labels after replay %#v, got %#v", newLabels, reloaded.Labels)
	}
	if !reflect.DeepEqual(reloaded.References, newReferences) {
		t.Fatalf("expected references after replay %#v, got %#v", newReferences, reloaded.References)
	}
}
