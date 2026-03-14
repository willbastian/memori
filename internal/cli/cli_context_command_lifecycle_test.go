package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

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
