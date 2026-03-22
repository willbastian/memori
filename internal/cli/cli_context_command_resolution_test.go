package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextCommandsResolveOmittedSessionLifecycle(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-auto-session.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-auto-session-checkpoint-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint auto session: %v\nstderr: %s", err, stderr)
	}
	var checkpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &checkpoint); err != nil {
		t.Fatalf("decode checkpoint json: %v\nstdout: %s", err, stdout)
	}
	sessionID := checkpoint.Data.Session.SessionID
	if !checkpoint.Data.Created || !strings.HasPrefix(sessionID, "sess_") {
		t.Fatalf("expected auto-created session id, got %+v", checkpoint)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-auto-session-checkpoint-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint replay auto session: %v\nstderr: %s", err, stderr)
	}
	var replayedCheckpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &replayedCheckpoint); err != nil {
		t.Fatalf("decode replay checkpoint json: %v\nstdout: %s", err, stdout)
	}
	if replayedCheckpoint.Data.Session.SessionID != sessionID {
		t.Fatalf("expected replayed checkpoint to reuse session %q, got %+v", sessionID, replayedCheckpoint)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--note", "paused for handoff",
		"--command-id", "cmd-cli-auto-session-summarize-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context summarize auto session: %v\nstderr: %s", err, stderr)
	}
	var summarized contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &summarized); err != nil {
		t.Fatalf("decode summarize json: %v\nstdout: %s", err, stdout)
	}
	if summarized.Data.Session.SessionID != sessionID || summarized.Data.Session.SummaryEventID == "" {
		t.Fatalf("expected summarize to use session %q, got %+v", sessionID, summarized)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate auto session: %v\nstderr: %s", err, stderr)
	}
	var activeRehydrate contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &activeRehydrate); err != nil {
		t.Fatalf("decode active rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if activeRehydrate.Data.SessionID != sessionID {
		t.Fatalf("expected active rehydrate to use session %q, got %+v", sessionID, activeRehydrate)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "close",
		"--db", dbPath,
		"--reason", "handoff complete",
		"--command-id", "cmd-cli-auto-session-close-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context close auto session: %v\nstderr: %s", err, stderr)
	}
	var closed contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &closed); err != nil {
		t.Fatalf("decode close json: %v\nstdout: %s", err, stdout)
	}
	if closed.Data.Session.SessionID != sessionID || closed.Data.Session.EndedAt == "" {
		t.Fatalf("expected close to use session %q, got %+v", sessionID, closed)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate latest session: %v\nstderr: %s", err, stderr)
	}
	var closedRehydrate contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &closedRehydrate); err != nil {
		t.Fatalf("decode closed rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if closedRehydrate.Data.SessionID != sessionID || closedRehydrate.Data.Source != "closed-session-summary" {
		t.Fatalf("expected closed latest-session rehydrate for %q, got %+v", sessionID, closedRehydrate)
	}
}

func TestContextCommandsKeepReplaySelectionStableWithoutExplicitSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-session-replay.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-session-replay-checkpoint-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint auto session: %v\nstderr: %s", err, stderr)
	}
	var autoCheckpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &autoCheckpoint); err != nil {
		t.Fatalf("decode auto checkpoint json: %v\nstdout: %s", err, stdout)
	}
	autoSessionID := autoCheckpoint.Data.Session.SessionID

	stdout, stderr, err = runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--note", "first summary",
		"--command-id", "cmd-cli-session-replay-summarize-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context summarize auto session: %v\nstderr: %s", err, stderr)
	}
	var firstSummary contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &firstSummary); err != nil {
		t.Fatalf("decode first summarize json: %v\nstdout: %s", err, stdout)
	}
	if firstSummary.Data.Session.SessionID != autoSessionID {
		t.Fatalf("expected first summary to use %q, got %+v", autoSessionID, firstSummary)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-manual-later",
		"--command-id", "cmd-cli-session-replay-checkpoint-2",
		"--json",
	); err != nil {
		t.Fatalf("context checkpoint explicit session: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--note", "first summary",
		"--command-id", "cmd-cli-session-replay-summarize-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context summarize replay after later session: %v\nstderr: %s", err, stderr)
	}
	var replayedSummary contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &replayedSummary); err != nil {
		t.Fatalf("decode replayed summarize json: %v\nstdout: %s", err, stdout)
	}
	if replayedSummary.Data.Session.SessionID != autoSessionID {
		t.Fatalf("expected replayed summary to stay on %q, got %+v", autoSessionID, replayedSummary)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate latest open session: %v\nstderr: %s", err, stderr)
	}
	var latestOpen contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &latestOpen); err != nil {
		t.Fatalf("decode latest open rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if latestOpen.Data.SessionID != "sess-manual-later" {
		t.Fatalf("expected rehydrate without --session to use latest open session, got %+v", latestOpen)
	}
}

func TestContextResumeUsesSavedPacketAndOptionalAgentFocus(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-resume.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--type", "task",
		"--title", "Resume target",
		"--command-id", "cmd-cli-resume-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	stdout, stderr, err := runMemoriForTest(
		"worktree", "register",
		"--db", dbPath,
		"--path", filepath.Join(t.TempDir(), "resume-worktree"),
		"--repo-root", t.TempDir(),
		"--branch", "feature/resume-worktree",
		"--command-id", "cmd-cli-resume-worktree-register-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("worktree register: %v\nstderr: %s", err, stderr)
	}
	var registered worktreeEnvelope
	if err := json.Unmarshal([]byte(stdout), &registered); err != nil {
		t.Fatalf("decode worktree register json: %v\nstdout: %s", err, stdout)
	}
	if _, stderr, err := runMemoriForTest(
		"worktree", "attach",
		"--db", dbPath,
		"--worktree", registered.Data.Worktree.WorktreeID,
		"--issue", "mem-a111111",
		"--command-id", "cmd-cli-resume-worktree-attach-1",
		"--json",
	); err != nil {
		t.Fatalf("worktree attach: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--command-id", "cmd-cli-resume-start-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context start: %v\nstderr: %s", err, stderr)
	}
	var started contextStartEnvelope
	if err := json.Unmarshal([]byte(stdout), &started); err != nil {
		t.Fatalf("decode context start json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "save",
		"--db", dbPath,
		"--session", started.Data.Session.SessionID,
		"--note", "ready to resume",
		"--command-id", "cmd-cli-resume-save-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context save: %v\nstderr: %s", err, stderr)
	}
	var saved contextSaveEnvelope
	if err := json.Unmarshal([]byte(stdout), &saved); err != nil {
		t.Fatalf("decode context save json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", started.Data.Session.SessionID,
		"--agent", "agent-resume-1",
		"--command-id", "cmd-cli-resume-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume: %v\nstderr: %s", err, stderr)
	}
	var resumed contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode context resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Command != "context resume" || resumed.Data.SessionID != started.Data.Session.SessionID {
		t.Fatalf("unexpected context resume response: %+v", resumed)
	}
	if resumed.Data.Source != "packet" {
		t.Fatalf("expected packet-first resume, got %+v", resumed)
	}
	if resumed.Data.Packet.Scope != "issue" {
		t.Fatalf("expected issue-scoped focus packet for resume, got %+v", resumed)
	}
	if resumed.Data.Packet.PacketID == "" || resumed.Data.Packet.PacketID == saved.Data.Packet.PacketID {
		t.Fatalf("expected resume focus to build or select an issue packet distinct from the saved session packet, got %+v", resumed)
	}
	if !resumed.Data.FocusUsed ||
		resumed.Data.Focus.AgentID != "agent-resume-1" ||
		resumed.Data.Focus.ActiveIssueID != "mem-a111111" ||
		resumed.Data.Focus.LastPacketID != resumed.Data.Packet.PacketID {
		t.Fatalf("expected focused resume response, got %+v", resumed)
	}
	if resumed.Data.Workspace.WorktreeID != registered.Data.Worktree.WorktreeID || resumed.Data.Workspace.Branch != "feature/resume-worktree" {
		t.Fatalf("expected resumed workspace context, got %+v", resumed.Data.Workspace)
	}
}

func TestContextResumeBuildsFocusedPacketWhenOnlyFallbackContextExists(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-resume-fallback-focus.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111112",
		"--type", "task",
		"--title", "Resume fallback target",
		"--command-id", "cmd-cli-resume-fallback-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a111112",
		"--session", "sess-resume-fallback-1",
		"--command-id", "cmd-cli-resume-fallback-start-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context start: %v\nstderr: %s", err, stderr)
	}
	var started contextStartEnvelope
	if err := json.Unmarshal([]byte(stdout), &started); err != nil {
		t.Fatalf("decode context start json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", started.Data.Session.SessionID,
		"--agent", "agent-resume-fallback-1",
		"--command-id", "cmd-cli-resume-fallback-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume fallback focus: %v\nstderr: %s", err, stderr)
	}
	var resumed contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode fallback-focused resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Data.Source != "relevant-chunks-fallback" {
		t.Fatalf("expected fallback source, got %+v", resumed)
	}
	if !resumed.Data.FocusUsed || resumed.Data.Packet.PacketID == "" {
		t.Fatalf("expected synthesized packet-backed focus, got %+v", resumed)
	}
	if resumed.Data.Focus.AgentID != "agent-resume-fallback-1" || resumed.Data.Focus.ActiveIssueID != "mem-a111112" {
		t.Fatalf("expected issue-focused resume, got %+v", resumed)
	}
}

func TestContextResumeReplayKeepsFocusedPacketStable(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-resume-replay-stable.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-resume-replay-stable-1",
		"--command-id", "cmd-cli-resume-replay-checkpoint-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}
	var checkpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &checkpoint); err != nil {
		t.Fatalf("decode checkpoint json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", checkpoint.Data.Session.SessionID,
		"--agent", "agent-resume-replay-stable-1",
		"--command-id", "cmd-cli-resume-replay-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("first context resume: %v\nstderr: %s", err, stderr)
	}
	var firstResume contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &firstResume); err != nil {
		t.Fatalf("decode first resume json: %v\nstdout: %s", err, stdout)
	}
	if !firstResume.Data.FocusUsed || firstResume.Data.Packet.PacketID == "" {
		t.Fatalf("expected first resume to focus a packet, got %+v", firstResume)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", checkpoint.Data.Session.SessionID,
		"--command-id", "cmd-cli-resume-replay-checkpoint-2",
		"--json",
	); err != nil {
		t.Fatalf("later checkpoint on same session: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", checkpoint.Data.Session.SessionID,
		"--agent", "agent-resume-replay-stable-1",
		"--command-id", "cmd-cli-resume-replay-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("replayed context resume: %v\nstderr: %s", err, stderr)
	}
	var replayedResume contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &replayedResume); err != nil {
		t.Fatalf("decode replayed resume json: %v\nstdout: %s", err, stdout)
	}
	if !replayedResume.Data.FocusIdempotent {
		t.Fatalf("expected replayed resume to be idempotent, got %+v", replayedResume)
	}
	if replayedResume.Data.Packet.PacketID != firstResume.Data.Packet.PacketID {
		t.Fatalf("expected replayed resume packet %q to stay stable, got %+v", firstResume.Data.Packet.PacketID, replayedResume)
	}
	if replayedResume.Data.Focus.LastPacketID != firstResume.Data.Packet.PacketID {
		t.Fatalf("expected replayed focus to keep last packet %q, got %+v", firstResume.Data.Packet.PacketID, replayedResume)
	}
}

func TestContextResumeWithoutSessionHonorsAgentFocusBeforeLatestOpen(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-resume-agent-focus.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	for _, issueKey := range []string{"mem-a111113", "mem-a111114"} {
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", issueKey,
			"--type", "task",
			"--title", "Resume agent focus target",
			"--command-id", "cmd-"+issueKey+"-create-1",
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", issueKey, err, stderr)
		}
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a111113",
		"--session", "sess-agent-focus-a",
		"--agent", "agent-focus-resume-1",
		"--command-id", "cmd-cli-resume-agent-focus-start-a-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context start issue A: %v\nstderr: %s", err, stderr)
	}
	var startedA contextStartEnvelope
	if err := json.Unmarshal([]byte(stdout), &startedA); err != nil {
		t.Fatalf("decode issue A context start json: %v\nstdout: %s", err, stdout)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a111114",
		"--session", "sess-agent-focus-b",
		"--command-id", "cmd-cli-resume-agent-focus-start-b-1",
		"--json",
	); err != nil {
		t.Fatalf("context start issue B: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--agent", "agent-focus-resume-1",
		"--command-id", "cmd-cli-resume-agent-focus-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume via agent focus: %v\nstderr: %s", err, stderr)
	}
	var resumed contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode agent-focus resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Data.SessionID != startedA.Data.Session.SessionID {
		t.Fatalf("expected resume without --session to honor focused session %q, got %+v", startedA.Data.Session.SessionID, resumed)
	}
	if resumed.Data.Focus.ActiveIssueID != "mem-a111113" {
		t.Fatalf("expected focused resume to stay on issue A, got %+v", resumed)
	}
}

func TestContextResumeBuildsIssuePacketForLegacyIssueSessionFocus(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-resume-legacy-issue-focus.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111115",
		"--type", "task",
		"--title", "Resume legacy issue target",
		"--command-id", "cmd-cli-resume-legacy-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a111115",
		"--session", "sess-resume-legacy-1",
		"--command-id", "cmd-cli-resume-legacy-start-1",
		"--json",
	); err != nil {
		t.Fatalf("context start: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "save",
		"--db", dbPath,
		"--session", "sess-resume-legacy-1",
		"--command-id", "cmd-cli-resume-legacy-save-1",
		"--json",
	); err != nil {
		t.Fatalf("context save: %v\nstderr: %s", err, stderr)
	}

	ctx := context.Background()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		UPDATE sessions
		SET checkpoint_json = json_remove(checkpoint_json, '$.issue_id')
		WHERE session_id = ?
	`, "sess-resume-legacy-1"); err != nil {
		t.Fatalf("strip checkpoint issue id for legacy session: %v", err)
	}

	stdout, stderr := "", ""
	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", "sess-resume-legacy-1",
		"--agent", "agent-resume-legacy-1",
		"--command-id", "cmd-cli-resume-legacy-run-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume legacy focus: %v\nstderr: %s", err, stderr)
	}
	var resumed contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode legacy-focused resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Data.Packet.Scope != "issue" {
		t.Fatalf("expected legacy issue session resume to rebuild an issue packet, got %+v", resumed)
	}
	if !resumed.Data.FocusUsed || resumed.Data.Focus.ActiveIssueID != "mem-a111115" {
		t.Fatalf("expected legacy issue session focus to preserve issue scope, got %+v", resumed)
	}
	if resumed.Data.Focus.LastPacketID != resumed.Data.Packet.PacketID {
		t.Fatalf("expected focused legacy resume to point at returned packet, got %+v", resumed)
	}
}

func TestContextStartMissingIssueDoesNotPersistIssueSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-start-missing-issue.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-deadbee",
		"--session", "sess-missing-issue-start-1",
		"--command-id", "cmd-cli-context-start-missing-issue-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), `issue "mem-deadbee" not found`) {
		t.Fatalf("expected missing issue error from context start, got %v", err)
	}

	ctx := context.Background()
	s, _, err := openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store after failed context start: %v", err)
	}
	defer s.Close()

	if _, err := s.GetSession(ctx, "sess-missing-issue-start-1"); err == nil || !strings.Contains(err.Error(), `session "sess-missing-issue-start-1" not found`) {
		t.Fatalf("expected failed context start to leave no session, got %v", err)
	}
}
