package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestHelpHumanOutputSeparatesHumanAndAgentWorkflows(t *testing.T) {
	t.Parallel()

	stdout, stderr, err := runMemoriForTest("help")
	if err != nil {
		t.Fatalf("run help command: %v\nstderr: %s", err, stderr)
	}

	mustContain(t, stdout, "Human Workflows:")
	mustContain(t, stdout, "Agent Workflows:")
	mustContain(t, stdout, "Create And Update Work:")
	mustContain(t, stdout, "memori board [--db <path>] [--agent <id>] [--watch] [--interval <duration>] [--json]")
	mustContain(t, stdout, "memori context start --issue <prefix-shortSHA> [--agent <id>] [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]")
	mustContain(t, stdout, "memori context save [--session <id>] [--note <text>] [--close] [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]")
	mustContain(t, stdout, "memori context checkpoint [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]")
	mustContain(t, stdout, "memori context rehydrate [--session <id>] [--json]")
	mustContain(t, stdout, "memori context packet show --packet <id> [--json]")
	mustContain(t, stdout, "MEMORI_COLOR=auto|always|never")
}

func TestIssueCreateHumanOutputShowsNextSteps(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-output-readable.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-e111111",
		"--type", "task",
		"--title", "Readable output task",
		"--command-id", "cmd-readable-create-1",
	)
	if err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	mustContain(t, stdout, "OK Created issue mem-e111111")
	mustContain(t, stdout, "Continuity:")
	mustContain(t, stdout, "Capture continuity in-product as soon as work starts or you hand this issue to another worker.")
	mustContain(t, stdout, "memori context checkpoint")
	mustContain(t, stdout, "memori context packet build --scope issue --id mem-e111111")
	mustContain(t, stdout, "Next:")
	mustContain(t, stdout, "memori issue show --key mem-e111111")
	mustContain(t, stdout, "memori issue update --key mem-e111111 --status inprogress")
}

func TestIssueUpdateAndShowHumanOutputSurfaceStateAwareContinuity(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-output-issue-continuity.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-c0ffee1",
		"--type", "task",
		"--title", "Continuity-rich task",
		"--command-id", "cmd-readable-issue-continuity-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c0ffee1",
		"--status", "inprogress",
		"--agent", "agent-readable-issue-1",
		"--command-id", "cmd-readable-issue-continuity-update-1",
	)
	if err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Started:")
	mustContain(t, stdout, "Captured session ")
	mustContain(t, stdout, "Refreshed issue packet ")
	mustContain(t, stdout, "Updated agent agent-readable-issue-1 focus to mem-c0ffee1 via packet ")
	mustContain(t, stdout, "Continuity:")
	mustContain(t, stdout, "This issue is active work; keep continuity current so pause, resume, and handoff stay lightweight.")
	mustContain(t, stdout, "memori context checkpoint")
	mustContain(t, stdout, "memori context summarize")
	mustContain(t, stdout, "memori context packet build --scope issue --id mem-c0ffee1")

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-c0ffee1")
	if err != nil {
		t.Fatalf("issue show inprogress: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity State:")
	mustContain(t, stdout, "Latest open session sess_")
	mustContain(t, stdout, "Latest issue packet")
	mustContain(t, stdout, "is fresh for mem-c0ffee1 cycle 1.")
	mustContain(t, stdout, "Continuity:")
	mustContain(t, stdout, "This issue is active work; keep continuity current so pause, resume, and handoff stay lightweight.")
	mustContain(t, stdout, "memori context summarize")

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c0ffee1",
		"--status", "blocked",
		"--note", "waiting on review",
		"--command-id", "cmd-readable-issue-continuity-update-2",
	)
	if err != nil {
		t.Fatalf("issue update blocked: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Summarized session ")
	mustContain(t, stdout, "Saved session packet ")
	mustContain(t, stdout, "This issue is blocked; preserve the current state before waiting or handing it off.")
	mustContain(t, stdout, "memori context loops --issue mem-c0ffee1")
}

func TestIssueShowHumanOutputSkipsContinuityForEpic(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-output-issue-epic.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-e11c001",
		"--type", "epic",
		"--title", "Coordination epic",
		"--command-id", "cmd-readable-issue-epic-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create epic: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-e11c001")
	if err != nil {
		t.Fatalf("issue show epic: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "mem-e11c001 [Epic/Todo]")
	if strings.Contains(stdout, "Continuity:") {
		t.Fatalf("did not expect continuity guidance for epic show, got:\n%s", stdout)
	}
}

func TestIssueNextHumanOutputShowsReasonSection(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-output-next.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-f111111",
		"--type", "task",
		"--title", "Readable next issue",
		"--command-id", "cmd-readable-next-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest("issue", "next", "--db", dbPath, "--agent", "agent-readable-1")
	if err != nil {
		t.Fatalf("issue next: %v\nstderr: %s", err, stderr)
	}

	mustContain(t, stdout, "Recommended issue")
	mustContain(t, stdout, "Why This Issue:")
	mustContain(t, stdout, "Continuity:")
	mustContain(t, stdout, "No saved focus, recovery packet, or open-loop continuity is shaping recommendations for agent-readable-1 yet.")
	mustContain(t, stdout, "memori context checkpoint")
	mustContain(t, stdout, "memori context packet build --scope issue --id mem-f111111")
	mustContain(t, stdout, "memori context loops --issue mem-f111111")
	mustContain(t, stdout, "memori issue show --key mem-f111111")
	mustContain(t, stdout, "memori issue update --key mem-f111111 --status inprogress --agent agent-readable-1")
}

func TestIssueNextHumanOutputShowsContinuityStateWhenResumeContextExists(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-output-next-resume.db")
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
		IssueID:   "mem-f222222",
		Type:      "task",
		Title:     "Readable resume issue",
		Actor:     "test",
		CommandID: "cmd-readable-resume-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   "mem-f222222",
		Actor:     "test",
		CommandID: "cmd-readable-resume-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: "sess-readable-next-1",
		Trigger:   "manual",
		Actor:     "test",
		CommandID: "cmd-readable-resume-checkpoint-1",
	}); err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if _, err := s.SummarizeSession(ctx, store.SummarizeSessionParams{
		SessionID: "sess-readable-next-1",
		Note:      "paused before handoff",
		Actor:     "test",
		CommandID: "cmd-readable-resume-summarize-1",
	}); err != nil {
		t.Fatalf("summarize session: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   "agent-readable-resume-1",
		PacketID:  packet.PacketID,
		Actor:     "test",
		CommandID: "cmd-readable-resume-use-1",
	}); err != nil {
		t.Fatalf("use packet: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("issue", "next", "--db", dbPath, "--agent", "agent-readable-resume-1")
	if err != nil {
		t.Fatalf("issue next: %v\nstderr: %s", err, stderr)
	}

	mustContain(t, stdout, "Continuity State:")
	mustContain(t, stdout, "Agent agent-readable-resume-1 focus points to mem-f222222 cycle 1 via packet")
	mustContain(t, stdout, "Latest open session sess-readable-next-1 has summary")
	mustContain(t, stdout, "Latest issue packet")
	mustContain(t, stdout, "is fresh for mem-f222222 cycle 1.")
}

func TestBacklogColorModeAlwaysAndNever(t *testing.T) {
	dbPath := seedBacklogTestDB(t)

	t.Setenv("MEMORI_COLOR", "always")
	stdout, stderr, err := runMemoriForTest("backlog", "--db", dbPath)
	if err != nil {
		t.Fatalf("run backlog with color always: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "\x1b[") {
		t.Fatalf("expected ANSI color codes in backlog output, got:\n%s", stdout)
	}

	t.Setenv("MEMORI_COLOR", "never")
	stdout, stderr, err = runMemoriForTest("backlog", "--db", dbPath)
	if err != nil {
		t.Fatalf("run backlog with color never: %v\nstderr: %s", err, stderr)
	}
	if strings.Contains(stdout, "\x1b[") {
		t.Fatalf("did not expect ANSI color codes in backlog output, got:\n%s", stdout)
	}
}
