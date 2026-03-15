package store

import (
	"context"
	"strings"
	"testing"
)

func TestGetRehydratePacketEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.GetRehydratePacket(ctx, GetPacketParams{}); err == nil || !strings.Contains(err.Error(), "--packet is required") {
		t.Fatalf("expected missing packet id error, got %v", err)
	}

	if _, err := s.GetRehydratePacket(ctx, GetPacketParams{PacketID: "pkt_missing"}); err == nil || !strings.Contains(err.Error(), `packet "pkt_missing" not found`) {
		t.Fatalf("expected missing packet error, got %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, err := s.GetRehydratePacket(ctx, GetPacketParams{PacketID: "pkt_any"}); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("expected closed DB begin tx error, got %v", err)
	}
}

func TestListOpenLoopsFiltersOrderingAndErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if loops, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{}); err != nil {
		t.Fatalf("list empty open loops: %v", err)
	} else if len(loops) != 0 {
		t.Fatalf("expected no open loops, got %#v", loops)
	}

	if _, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{IssueID: "bad"}); err == nil || !strings.Contains(err.Error(), "invalid issue key") {
		t.Fatalf("expected invalid issue id error, got %v", err)
	}

	zeroCycle := 0
	if _, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{CycleNo: &zeroCycle}); err == nil || !strings.Contains(err.Error(), "--cycle must be > 0") {
		t.Fatalf("expected invalid cycle error, got %v", err)
	}

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Type:      "task",
		Title:     "Open loop issue A",
		Actor:     "agent-1",
		CommandID: "cmd-open-loop-issue-a-1",
	}); err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b2c3d4e",
		Type:      "task",
		Title:     "Open loop issue B",
		Actor:     "agent-1",
		CommandID: "cmd-open-loop-issue-b-1",
	}); err != nil {
		t.Fatalf("create issue B: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO open_loops(loop_id, issue_id, cycle_no, loop_type, status, owner, priority, source_event_id, updated_at)
		VALUES
			('loop-b', 'mem-a1b2c3d', 1, 'question', 'Open', 'agent-1', 'high', 'evt-loop-b', '2026-03-08T12:00:00Z'),
			('loop-a', 'mem-a1b2c3d', 1, 'question', 'Open', 'agent-1', 'high', 'evt-loop-a', '2026-03-08T12:00:00Z'),
			('loop-c', 'mem-a1b2c3d', 2, 'risk', 'Open', 'human:alice', 'medium', 'evt-loop-c', '2026-03-08T12:01:00Z'),
			('loop-d', 'mem-b2c3d4e', 1, 'next_action', 'Open', 'human:bob', 'low', 'evt-loop-d', '2026-03-08T12:02:00Z')
	`); err != nil {
		t.Fatalf("seed open loops: %v", err)
	}

	loops, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{})
	if err != nil {
		t.Fatalf("list all open loops: %v", err)
	}
	if len(loops) != 4 {
		t.Fatalf("expected 4 open loops, got %#v", loops)
	}
	if loops[0].LoopID != "loop-a" || loops[1].LoopID != "loop-b" || loops[2].LoopID != "loop-c" || loops[3].LoopID != "loop-d" {
		t.Fatalf("expected issue/cycle/update ordering, got %#v", loops)
	}

	issueLoops, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{IssueID: "mem-a1b2c3d"})
	if err != nil {
		t.Fatalf("list issue-scoped open loops: %v", err)
	}
	if len(issueLoops) != 3 {
		t.Fatalf("expected 3 issue-scoped loops, got %#v", issueLoops)
	}

	cycleOne := 1
	filtered, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{
		IssueID: "mem-a1b2c3d",
		CycleNo: &cycleOne,
	})
	if err != nil {
		t.Fatalf("list filtered open loops: %v", err)
	}
	if len(filtered) != 2 || filtered[0].LoopID != "loop-a" || filtered[1].LoopID != "loop-b" {
		t.Fatalf("expected filtered loop ordering, got %#v", filtered)
	}

	if _, err := s.db.ExecContext(ctx, `DROP TABLE open_loops`); err != nil {
		t.Fatalf("drop open_loops table: %v", err)
	}
	if _, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{}); err == nil || !strings.Contains(err.Error(), "list open loops:") {
		t.Fatalf("expected list open loops query error, got %v", err)
	}
	if _, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{IssueID: "mem-a1b2c3d"}); err == nil || !strings.Contains(err.Error(), `list open loops for issue "mem-a1b2c3d"`) {
		t.Fatalf("expected issue-scoped list open loops query error, got %v", err)
	}
}
