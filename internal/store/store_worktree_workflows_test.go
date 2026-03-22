package store

import (
	"context"
	"testing"
)

func TestWorktreeLifecycleAndIssuePacketWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-a1b2c3d"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Worktree-backed issue",
		Actor:     "test",
		CommandID: "cmd-worktree-issue-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	worktree, event, _, err := s.RegisterWorktree(ctx, RegisterWorktreeParams{
		Path:      "/tmp/memori/worktrees/a1",
		RepoRoot:  "/tmp/memori",
		Branch:    "feature/a1",
		HeadOID:   "abc1234",
		Actor:     "test",
		CommandID: "cmd-worktree-register-1",
	})
	if err != nil {
		t.Fatalf("register worktree: %v", err)
	}
	if event.EventType != eventTypeWorktreeRegistered || worktree.Status != worktreeStatusActive {
		t.Fatalf("unexpected registered worktree result: %+v event=%+v", worktree, event)
	}

	worktree, _, _, err = s.AttachWorktree(ctx, AttachWorktreeParams{
		WorktreeID: worktree.WorktreeID,
		IssueID:    issueID,
		Actor:      "test",
		CommandID:  "cmd-worktree-attach-1",
	})
	if err != nil {
		t.Fatalf("attach worktree: %v", err)
	}
	if worktree.IssueID != issueID {
		t.Fatalf("expected attached issue %q, got %+v", issueID, worktree)
	}

	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "test",
		CommandID: "cmd-worktree-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	workspace, ok := packet.Packet["workspace"].(map[string]any)
	if !ok {
		t.Fatalf("expected issue packet workspace map, got %#v", packet.Packet["workspace"])
	}
	if workspace["worktree_id"] != worktree.WorktreeID || workspace["path"] != worktree.Path {
		t.Fatalf("unexpected workspace packet payload: %#v", workspace)
	}
	state, ok := packet.Packet["state"].(map[string]any)
	if !ok || state["worktree_id"] != worktree.WorktreeID {
		t.Fatalf("expected issue packet state to include worktree_id, got %#v", packet.Packet["state"])
	}

	worktree, _, _, err = s.DetachWorktree(ctx, DetachWorktreeParams{
		WorktreeID: worktree.WorktreeID,
		Actor:      "test",
		CommandID:  "cmd-worktree-detach-1",
	})
	if err != nil {
		t.Fatalf("detach worktree: %v", err)
	}
	if worktree.IssueID != "" {
		t.Fatalf("expected detached worktree to clear issue_id, got %+v", worktree)
	}

	packet, err = s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "test",
		CommandID: "cmd-worktree-packet-2",
	})
	if err != nil {
		t.Fatalf("build issue packet after detach: %v", err)
	}
	if _, ok := packet.Packet["workspace"]; ok {
		t.Fatalf("expected detached worktree to disappear from packet, got %#v", packet.Packet["workspace"])
	}
}

func TestReplayRebuildsWorktreeProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-b2c3d4e"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Replay worktree issue",
		Actor:     "test",
		CommandID: "cmd-worktree-replay-issue-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	worktree, _, _, err := s.RegisterWorktree(ctx, RegisterWorktreeParams{
		Path:      "/tmp/memori/worktrees/b2",
		RepoRoot:  "/tmp/memori",
		Branch:    "feature/b2",
		HeadOID:   "def5678",
		Actor:     "test",
		CommandID: "cmd-worktree-replay-register-1",
	})
	if err != nil {
		t.Fatalf("register worktree: %v", err)
	}
	if _, _, _, err := s.AttachWorktree(ctx, AttachWorktreeParams{
		WorktreeID: worktree.WorktreeID,
		IssueID:    issueID,
		Actor:      "test",
		CommandID:  "cmd-worktree-replay-attach-1",
	}); err != nil {
		t.Fatalf("attach worktree: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM worktrees WHERE worktree_id = ?`, worktree.WorktreeID); err != nil {
		t.Fatalf("delete worktree projection row: %v", err)
	}
	if result, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	} else if result.EventsApplied == 0 {
		t.Fatal("expected replay to apply events")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for verification: %v", err)
	}
	defer tx.Rollback()

	rebuilt, err := worktreeByIDTx(ctx, tx, worktree.WorktreeID)
	if err != nil {
		t.Fatalf("query rebuilt worktree: %v", err)
	}
	if rebuilt.IssueID != issueID || rebuilt.Path != worktree.Path || rebuilt.Status != worktreeStatusActive {
		t.Fatalf("unexpected rebuilt worktree: %+v", rebuilt)
	}
}

func TestAttachWorktreeRejectsAmbiguousIssueWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-c3d4e5f"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Single workspace issue",
		Actor:     "test",
		CommandID: "cmd-worktree-ambiguous-issue-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	first, _, _, err := s.RegisterWorktree(ctx, RegisterWorktreeParams{
		Path:      "/tmp/memori/worktrees/c1",
		RepoRoot:  "/tmp/memori",
		Actor:     "test",
		CommandID: "cmd-worktree-ambiguous-register-1",
	})
	if err != nil {
		t.Fatalf("register first worktree: %v", err)
	}
	second, _, _, err := s.RegisterWorktree(ctx, RegisterWorktreeParams{
		Path:      "/tmp/memori/worktrees/c2",
		RepoRoot:  "/tmp/memori",
		Actor:     "test",
		CommandID: "cmd-worktree-ambiguous-register-2",
	})
	if err != nil {
		t.Fatalf("register second worktree: %v", err)
	}

	if _, _, _, err := s.AttachWorktree(ctx, AttachWorktreeParams{
		WorktreeID: first.WorktreeID,
		IssueID:    issueID,
		Actor:      "test",
		CommandID:  "cmd-worktree-ambiguous-attach-1",
	}); err != nil {
		t.Fatalf("attach first worktree: %v", err)
	}
	if _, _, _, err := s.AttachWorktree(ctx, AttachWorktreeParams{
		WorktreeID: second.WorktreeID,
		IssueID:    issueID,
		Actor:      "test",
		CommandID:  "cmd-worktree-ambiguous-attach-2",
	}); err == nil || err.Error() == "" {
		t.Fatal("expected second worktree attach to fail")
	} else if got := err.Error(); got != `issue "mem-c3d4e5f" already has active worktree "`+first.WorktreeID+`" attached` {
		t.Fatalf("unexpected ambiguous attach error %q", got)
	}
}
