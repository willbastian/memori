package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type boardEnvelope struct {
	Command string    `json:"command"`
	Data    boardData `json:"data"`
}

func TestBoardCommandHumanOutputShowsWorkBucketsAndLikelyNext(t *testing.T) {
	t.Parallel()

	dbPath := seedBoardTestDB(t)

	stdout, stderr, err := runMemoriForTest("board", "--db", dbPath, "--agent", "agent-board-1")
	if err != nil {
		t.Fatalf("board command: %v\nstderr: %s", err, stderr)
	}

	for _, want := range []string{
		"memori board",
		"Summary:",
		"Next:",
		"Active (1):",
		"Blocked (1):",
		"Ready (2):",
		"mem-a111111 Active implementation",
		"mem-b222222 Waiting on external fix",
		"mem-c333333 Continuity-rich task [s300,focus,packet,loop,+5 more]",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected board output to contain %q, got:\n%s", want, stdout)
		}
	}
}

func TestBoardCommandHumanOutputCapsLongSectionsInNarrowWidth(t *testing.T) {
	t.Parallel()

	snapshot := boardSnapshot{
		GeneratedAt: "2026-03-08T01:00:00Z",
		Summary: boardSummary{
			Total: 7,
			Todo:  5,
		},
		Ready: []boardIssueRow{
			{Issue: store.Issue{ID: "mem-a111111", Title: "First very long ready item title"}},
			{Issue: store.Issue{ID: "mem-b222222", Title: "Second very long ready item title"}},
			{Issue: store.Issue{ID: "mem-c333333", Title: "Third very long ready item title"}},
			{Issue: store.Issue{ID: "mem-d444444", Title: "Fourth very long ready item title"}},
		},
	}

	stdout, err := renderBoardSnapshot(snapshot, boardRenderOptions{Width: 48})
	if err != nil {
		t.Fatalf("render board snapshot: %v", err)
	}

	if !strings.Contains(stdout, "Ready (4):") {
		t.Fatalf("expected ready section count in output, got:\n%s", stdout)
	}
	if !strings.Contains(stdout, "+2 more") {
		t.Fatalf("expected capped ready section in narrow mode, got:\n%s", stdout)
	}
	for _, line := range strings.Split(stdout, "\n") {
		if len(line) > 48 && !strings.Contains(line, "\x1b[") {
			t.Fatalf("expected narrow lines to be truncated, got %q", line)
		}
	}
}

func TestBoardCommandJSONIncludesCountsAndContinuityDrivenLikelyNext(t *testing.T) {
	t.Parallel()

	dbPath := seedBoardTestDB(t)

	stdout, stderr, err := runMemoriForTest("board", "--db", dbPath, "--agent", "agent-board-1", "--json")
	if err != nil {
		t.Fatalf("board json command: %v\nstderr: %s", err, stderr)
	}

	var resp boardEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode board json: %v\nstdout: %s", err, stdout)
	}

	if resp.Command != "board" {
		t.Fatalf("expected command board, got %q", resp.Command)
	}
	if resp.Data.Counts.InProgress != 1 || resp.Data.Counts.Blocked != 1 || resp.Data.Counts.Todo != 2 {
		t.Fatalf("unexpected board counts: %+v", resp.Data.Counts)
	}
	if resp.Data.LikelyNext == nil || resp.Data.LikelyNext.Issue.ID != "mem-c333333" {
		t.Fatalf("expected mem-c333333 as likely next, got %+v", resp.Data.LikelyNext)
	}
	if len(resp.Data.Ready) < 2 {
		t.Fatalf("expected ready work candidates, got %+v", resp.Data.Ready)
	}
	reasons := strings.Join(resp.Data.LikelyNext.Reasons, "\n")
	for _, want := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
	} {
		if !strings.Contains(reasons, want) {
			t.Fatalf("expected likely next reasons to contain %q, got:\n%s", want, reasons)
		}
	}
}

func TestBuildBoardSnapshotIncludesHierarchyMetadata(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-board-hierarchy.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	create := func(id, issueType, title, parent, commandID string) {
		t.Helper()
		if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
			IssueID:   id,
			Type:      issueType,
			Title:     title,
			ParentID:  parent,
			Actor:     "test",
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("create issue %s: %v", id, err)
		}
	}
	update := func(id, status, commandID string) {
		t.Helper()
		if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
			IssueID:   id,
			Status:    status,
			Actor:     "test",
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("update issue %s to %s: %v", id, status, err)
		}
	}

	create("mem-e111111", "epic", "Hierarchy epic", "", "cmd-hierarchy-create-1")
	create("mem-a222222", "story", "Hierarchy story", "mem-e111111", "cmd-hierarchy-create-2")
	create("mem-c333333", "task", "Hierarchy task", "mem-a222222", "cmd-hierarchy-create-3")
	create("mem-b444444", "bug", "Hierarchy bug", "mem-a222222", "cmd-hierarchy-create-4")
	update("mem-b444444", "blocked", "cmd-hierarchy-blocked-1")

	snapshot, err := buildBoardSnapshot(ctx, s, "", time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build board snapshot: %v", err)
	}

	storyRow := mustFindBoardRow(t, snapshot.Ready, "mem-a222222")
	if storyRow.Hierarchy.Depth != 1 {
		t.Fatalf("expected story depth 1, got %+v", storyRow.Hierarchy)
	}
	if storyRow.Hierarchy.ParentID != "mem-e111111" || storyRow.Hierarchy.ParentTitle != "Hierarchy epic" || !storyRow.Hierarchy.HasChildren || storyRow.Hierarchy.ChildCount != 2 {
		t.Fatalf("expected story parent and child metadata, got %+v", storyRow.Hierarchy)
	}
	if got := strings.Join(storyRow.Hierarchy.Path, " > "); got != "mem-e111111 > mem-a222222" {
		t.Fatalf("expected story path to include epic ancestry, got %q", got)
	}

	taskRow := mustFindBoardRow(t, snapshot.Ready, "mem-c333333")
	if taskRow.Hierarchy.Depth != 2 {
		t.Fatalf("expected task depth 2, got %+v", taskRow.Hierarchy)
	}
	if got := strings.Join(taskRow.Hierarchy.AncestorIDs, " > "); got != "mem-e111111 > mem-a222222" {
		t.Fatalf("expected task ancestors to include epic and story, got %q", got)
	}
	if taskRow.Hierarchy.ChildCount != 0 || taskRow.Hierarchy.HasChildren {
		t.Fatalf("expected task leaf hierarchy, got %+v", taskRow.Hierarchy)
	}

	blockedBug := mustFindBoardRow(t, snapshot.Blocked, "mem-b444444")
	if blockedBug.Hierarchy.ParentType != "Story" || blockedBug.Hierarchy.ParentStatus != "Todo" {
		t.Fatalf("expected blocked bug to include parent type/status context, got %+v", blockedBug.Hierarchy)
	}
}

func TestBuildBoardSnapshotCountsWontDoWithoutSurfacingActionableWork(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-board-wontdo.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-a111111",
		Type:      "task",
		Title:     "Declined task",
		Actor:     "agent-1",
		CommandID: "cmd-board-wontdo-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-a111111",
		Status:    "wontdo",
		Actor:     "human:alice",
		CommandID: "cmd-board-wontdo-status-1",
	}); err != nil {
		t.Fatalf("mark WontDo: %v", err)
	}

	snapshot, err := buildBoardSnapshot(ctx, s, "", time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build board snapshot: %v", err)
	}

	if snapshot.Summary.Total != 1 || snapshot.Summary.WontDo != 1 {
		t.Fatalf("expected WontDo count in summary, got %+v", snapshot.Summary)
	}
	if len(snapshot.Active) != 0 || len(snapshot.Blocked) != 0 || len(snapshot.Ready) != 0 {
		t.Fatalf("expected WontDo issue to stay out of actionable lanes, got active=%+v blocked=%+v ready=%+v", snapshot.Active, snapshot.Blocked, snapshot.Ready)
	}
}

func mustFindBoardRow(t *testing.T, rows []boardIssueRow, issueID string) boardIssueRow {
	t.Helper()
	for _, row := range rows {
		if row.Issue.ID == issueID {
			return row
		}
	}
	t.Fatalf("expected to find issue %s in rows: %+v", issueID, rows)
	return boardIssueRow{}
}

func seedBoardTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-board.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	create := func(id, issueType, title, actor, commandID string) {
		t.Helper()
		if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
			IssueID:   id,
			Type:      issueType,
			Title:     title,
			Actor:     actor,
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("create issue %s: %v", id, err)
		}
	}
	update := func(id, status, actor, commandID string) {
		t.Helper()
		if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
			IssueID:   id,
			Status:    status,
			Actor:     actor,
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("update issue %s to %s: %v", id, status, err)
		}
	}

	create("mem-a111111", "task", "Active implementation", "test", "cmd-board-create-1")
	create("mem-b222222", "bug", "Waiting on external fix", "test", "cmd-board-create-2")
	create("mem-c333333", "task", "Continuity-rich task", "test", "cmd-board-create-3")
	create("mem-d444444", "task", "Fresh ready task", "test", "cmd-board-create-4")

	update("mem-a111111", "inprogress", "test", "cmd-board-progress-1")
	update("mem-b222222", "blocked", "test", "cmd-board-blocked-1")

	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "board-close",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo board-close"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-board-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      "mem-c333333",
		TemplateRefs: []string{"board-close@1"},
		Actor:        "test",
		CommandID:    "cmd-board-gset-1",
	}); err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   "mem-c333333",
		Actor:     "test",
		CommandID: "cmd-board-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   "mem-c333333",
		Actor:     "test",
		CommandID: "cmd-board-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      "mem-c333333",
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/board-1"},
		Actor:        "test",
		CommandID:    "cmd-board-gate-1",
	}); err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   "agent-board-1",
		PacketID:  packet.PacketID,
		Actor:     "test",
		CommandID: "cmd-board-focus-1",
	}); err != nil {
		t.Fatalf("use rehydrate packet: %v", err)
	}

	return dbPath
}
