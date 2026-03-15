package cli

import (
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
