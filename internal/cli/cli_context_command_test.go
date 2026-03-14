package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

type contextCheckpointEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Created bool `json:"created"`
		Session struct {
			SessionID string `json:"session_id"`
		} `json:"session"`
	} `json:"data"`
}

type contextSessionEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Session struct {
			SessionID      string `json:"session_id"`
			EndedAt        string `json:"ended_at"`
			SummaryEventID string `json:"summary_event_id"`
		} `json:"session"`
	} `json:"data"`
}

type contextPacketEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
	} `json:"data"`
}

type contextPacketUseEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Focus struct {
			AgentID       string `json:"agent_id"`
			LastPacketID  string `json:"last_packet_id"`
			ActiveIssueID string `json:"active_issue_id"`
		} `json:"focus"`
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
		Idempotent bool `json:"idempotent"`
	} `json:"data"`
}

type contextRehydrateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		SessionID string `json:"session_id"`
		Source    string `json:"source"`
		Packet    struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
	} `json:"data"`
}

type contextLoopsEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count int `json:"count"`
		Loops []struct {
			IssueID string `json:"issue_id"`
			Status  string `json:"status"`
		} `json:"loops"`
	} `json:"data"`
}

type eventLogEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		EntityType string `json:"entity_type"`
		EntityID   string `json:"entity_id"`
		Events     []struct {
			EventID       string `json:"event_id"`
			EntityType    string `json:"entity_type"`
			EntityID      string `json:"entity_id"`
			EventType     string `json:"event_type"`
			CommandID     string `json:"command_id"`
			CausationID   string `json:"causation_id"`
			CorrelationID string `json:"correlation_id"`
		} `json:"events"`
	} `json:"data"`
}

func TestContextSessionLifecycleCommands(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-lifecycle.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--command-id", "cmd-cli-life-checkpoint-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}
	var checkpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &checkpoint); err != nil {
		t.Fatalf("decode checkpoint json: %v\nstdout: %s", err, stdout)
	}
	if !checkpoint.Data.Created {
		t.Fatalf("expected created checkpoint, got %+v", checkpoint)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "build",
		"--db", dbPath,
		"--scope", "session",
		"--id", "sess-cli-life-1",
		"--command-id", "cmd-cli-life-packet-active-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet build active session: %v\nstderr: %s", err, stderr)
	}
	var activePacket contextPacketEnvelope
	if err := json.Unmarshal([]byte(stdout), &activePacket); err != nil {
		t.Fatalf("decode active packet json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--note", "paused after triage",
		"--command-id", "cmd-cli-life-summary-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context summarize: %v\nstderr: %s", err, stderr)
	}
	var summarized contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &summarized); err != nil {
		t.Fatalf("decode summarize json: %v\nstdout: %s", err, stdout)
	}
	if summarized.Command != "context summarize" || summarized.Data.Session.SummaryEventID == "" || summarized.Data.Session.EndedAt != "" {
		t.Fatalf("unexpected summarize response: %+v", summarized)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "close",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--reason", "handoff complete",
		"--command-id", "cmd-cli-life-close-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context close: %v\nstderr: %s", err, stderr)
	}
	var closed contextSessionEnvelope
	if err := json.Unmarshal([]byte(stdout), &closed); err != nil {
		t.Fatalf("decode close json: %v\nstdout: %s", err, stdout)
	}
	if closed.Command != "context close" || closed.Data.Session.EndedAt == "" || closed.Data.Session.SummaryEventID != summarized.Data.Session.SummaryEventID {
		t.Fatalf("unexpected close response: %+v", closed)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:sess-cli-life-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log lifecycle session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode lifecycle event log json: %v\nstdout: %s", err, stdout)
	}
	if len(sessionEvents.Data.Events) != 3 {
		t.Fatalf("expected three session lifecycle events, got %+v", sessionEvents)
	}
	if sessionEvents.Data.Events[1].EventType != "session.summarized" || sessionEvents.Data.Events[2].EventType != "session.closed" {
		t.Fatalf("unexpected lifecycle events: %+v", sessionEvents.Data.Events)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate closed fallback: %v\nstderr: %s", err, stderr)
	}
	var closedFallback contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &closedFallback); err != nil {
		t.Fatalf("decode closed fallback json: %v\nstdout: %s", err, stdout)
	}
	if closedFallback.Data.Source != "closed-session-summary" {
		t.Fatalf("expected closed-session-summary source, got %+v", closedFallback)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "build",
		"--db", dbPath,
		"--scope", "session",
		"--id", "sess-cli-life-1",
		"--command-id", "cmd-cli-life-packet-closed-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet build closed session: %v\nstderr: %s", err, stderr)
	}
	var closedPacket contextPacketEnvelope
	if err := json.Unmarshal([]byte(stdout), &closedPacket); err != nil {
		t.Fatalf("decode closed packet json: %v\nstdout: %s", err, stdout)
	}
	if closedPacket.Data.Packet.PacketID == activePacket.Data.Packet.PacketID {
		t.Fatalf("expected a new closed session packet, got %+v", closedPacket)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate closed packet: %v\nstderr: %s", err, stderr)
	}
	var packetFirst contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &packetFirst); err != nil {
		t.Fatalf("decode packet-first closed rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if packetFirst.Data.Source != "packet" || packetFirst.Data.Packet.PacketID != closedPacket.Data.Packet.PacketID {
		t.Fatalf("expected closed session packet-first rehydrate, got %+v", packetFirst)
	}

	if _, stderr, err = runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-cli-life-1",
		"--command-id", "cmd-cli-life-checkpoint-2",
		"--json",
	); err == nil || !strings.Contains(err.Error(), "is closed") {
		t.Fatalf("expected closed session checkpoint failure, err=%v stderr=%s", err, stderr)
	}
}

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

func TestContextHumanOutputAutoResolvesSessionWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-auto-session.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-human-auto-session-checkpoint-1",
	)
	if err != nil {
		t.Fatalf("context checkpoint auto session: %v\nstderr: %s", err, stderr)
	}
	sessionID := sessionIDFromHumanOutput(t, stdout)
	mustContain(t, stdout, "Note No --session supplied; started new session "+sessionID+".")
	mustContain(t, stdout, "OK Created session checkpoint "+sessionID)
	mustContain(t, stdout, "memori context rehydrate --session "+sessionID)

	stdout, stderr, err = runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-human-auto-session-checkpoint-2",
	)
	if err != nil {
		t.Fatalf("context checkpoint latest open session: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Note No --session supplied; continuing latest open session "+sessionID+". Pass --session to start a parallel session.")
	mustContain(t, stdout, "OK Updated session checkpoint "+sessionID)

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
	)
	if err != nil {
		t.Fatalf("context rehydrate latest open session: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Note No --session supplied; rehydrating latest open session "+sessionID+".")
	mustContain(t, stdout, "OK Rehydrated session "+sessionID+" via relevant-chunks-fallback")
}

func TestContextSummarizeAndCloseHumanOutputShowLifecycleGuidance(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-lifecycle.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--command-id", "cmd-cli-human-life-checkpoint-1",
	); err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--note", "paused after review",
		"--command-id", "cmd-cli-human-life-summarize-1",
	)
	if err != nil {
		t.Fatalf("context summarize text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Summarized session sess-human-life-1",
		"Summary Event:",
		"memori context rehydrate --session sess-human-life-1",
		"memori context close --session sess-human-life-1",
	} {
		mustContain(t, stdout, want)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "close",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--reason", "handoff complete",
		"--command-id", "cmd-cli-human-life-close-1",
	)
	if err != nil {
		t.Fatalf("context close text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Closed session sess-human-life-1",
		"Ended At:",
		"memori context rehydrate --session sess-human-life-1",
		"memori context checkpoint",
	} {
		mustContain(t, stdout, want)
	}
}

func sessionIDFromHumanOutput(t *testing.T, stdout string) string {
	t.Helper()

	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"OK Created session checkpoint ", "OK Updated session checkpoint "} {
			if strings.HasPrefix(line, prefix) {
				rest := strings.TrimPrefix(line, prefix)
				if idx := strings.Index(rest, " "); idx > 0 {
					return rest[:idx]
				}
				return rest
			}
		}
	}
	t.Fatalf("expected session id in output, got:\n%s", stdout)
	return ""
}
