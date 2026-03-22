package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type issueNextEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Workspace struct {
			WorktreeID string `json:"worktree_id"`
			Path       string `json:"path"`
			Branch     string `json:"branch"`
		} `json:"workspace"`
		Next struct {
			Considered int `json:"considered"`
			Candidate  struct {
				Issue struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"issue"`
				Reasons []string `json:"reasons"`
			} `json:"candidate"`
		} `json:"next"`
	} `json:"data"`
}

func TestIssueNextReturnsDeterministicCandidateWithReasons(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-next.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	create := func(key, issueType, title, commandID string) {
		t.Helper()
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", key,
			"--type", issueType,
			"--title", title,
			"--command-id", commandID,
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", key, err, stderr)
		}
	}

	create("mem-a111111", "task", "Todo task", "cmd-next-create-1")
	create("mem-b222222", "bug", "Todo bug", "cmd-next-create-2")
	create("mem-c333333", "task", "InProgress task", "cmd-next-create-3")

	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c333333",
		"--status", "inprogress",
		"--command-id", "cmd-next-progress-3",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest("issue", "next", "--db", dbPath, "--agent", "agent-next-1", "--json")
	if err != nil {
		t.Fatalf("issue next: %v\nstderr: %s", err, stderr)
	}

	var resp issueNextEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue next json: %v\nstdout: %s", err, stdout)
	}
	if resp.Command != "issue next" {
		t.Fatalf("expected issue next command, got %q", resp.Command)
	}
	if resp.Data.Next.Considered != 3 {
		t.Fatalf("expected 3 considered issues, got %d", resp.Data.Next.Considered)
	}
	if resp.Data.Next.Candidate.Issue.ID != "mem-c333333" {
		t.Fatalf("expected in-progress task selected first, got %q", resp.Data.Next.Candidate.Issue.ID)
	}
	if len(resp.Data.Next.Candidate.Reasons) == 0 {
		t.Fatalf("expected reason strings in issue next response")
	}
}

func TestIssueNextPrefersContinuitySignalsForAgentResume(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-next-continuity.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	createIssue := func(id, issueType, title, actor, commandID string) {
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

	updateStatus := func(id, status, actor, commandID string) {
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

	createIssue("mem-a444444", "task", "Baseline active task", "test", "cmd-next-cont-create-1")
	createIssue("mem-b555555", "task", "Continuity-heavy task", "test", "cmd-next-cont-create-2")
	updateStatus("mem-a444444", "inprogress", "test", "cmd-next-cont-progress-1")
	worktree, _, _, err := s.RegisterWorktree(ctx, store.RegisterWorktreeParams{
		Path:      filepath.Join(t.TempDir(), "issue-next-worktree"),
		RepoRoot:  t.TempDir(),
		Branch:    "feature/issue-next-worktree",
		Actor:     "test",
		CommandID: "cmd-next-cont-worktree-register-1",
	})
	if err != nil {
		t.Fatalf("register worktree: %v", err)
	}
	if _, _, _, err := s.AttachWorktree(ctx, store.AttachWorktreeParams{
		WorktreeID: worktree.WorktreeID,
		IssueID:    "mem-b555555",
		Actor:      "test",
		CommandID:  "cmd-next-cont-worktree-attach-1",
	}); err != nil {
		t.Fatalf("attach worktree: %v", err)
	}

	definitionJSON := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo resume-quality"}}]}`
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "resume-quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definitionJSON,
		Actor:          "human:alice",
		CommandID:      "cmd-next-cont-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}
	gateSet, _, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      "mem-b555555",
		TemplateRefs: []string{"resume-quality@1"},
		Actor:        "test",
		CommandID:    "cmd-next-cont-gset-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   "mem-b555555",
		Actor:     "test",
		CommandID: "cmd-next-cont-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   "mem-b555555",
		Actor:     "test",
		CommandID: "cmd-next-cont-packet-1",
	})
	if err != nil {
		t.Fatalf("build rehydrate packet: %v", err)
	}

	if _, _, _, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID: "mem-b555555",
		GateID:  "build",
		Result:  "FAIL",
		EvidenceRefs: []string{
			"ci://run/issue-next-continuity-1",
		},
		Actor:     "test",
		CommandID: "cmd-next-cont-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}

	if _, _, _, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   "agent-next-continuity",
		PacketID:  packet.PacketID,
		Actor:     "test",
		CommandID: "cmd-next-cont-packet-use-1",
	}); err != nil {
		t.Fatalf("use rehydrate packet: %v", err)
	}

	if gateSet.GateSetID == "" {
		t.Fatalf("expected instantiated gate set id")
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "next",
		"--db", dbPath,
		"--agent", "agent-next-continuity",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue next: %v\nstderr: %s", err, stderr)
	}

	var resp issueNextEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue next json: %v\nstdout: %s", err, stdout)
	}
	if resp.Data.Next.Considered != 2 {
		t.Fatalf("expected 2 considered issues, got %d", resp.Data.Next.Considered)
	}
	if resp.Data.Next.Candidate.Issue.ID != "mem-b555555" {
		t.Fatalf("expected continuity-rich issue selected first, got %q", resp.Data.Next.Candidate.Issue.ID)
	}
	if resp.Data.Workspace.WorktreeID != worktree.WorktreeID || resp.Data.Workspace.Branch != "feature/issue-next-worktree" {
		t.Fatalf("expected issue next workspace payload, got %+v", resp.Data.Workspace)
	}

	reasons := strings.Join(resp.Data.Next.Candidate.Reasons, "\n")
	for _, want := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
		"available issue packet is stale",
	} {
		if !strings.Contains(reasons, want) {
			t.Fatalf("expected reasons to contain %q, got:\n%s", want, reasons)
		}
	}
}
