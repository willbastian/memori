package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
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
			EntityType string `json:"entity_type"`
			EntityID   string `json:"entity_id"`
			EventType  string `json:"event_type"`
		} `json:"events"`
	} `json:"data"`
}

func TestContextCheckpointPacketAndRehydrateCommands(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-ccccc11",
		"--type", "task",
		"--title", "Context CLI test issue",
		"--command-id", "cmd-cli-context-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-ccccc11",
		"--status", "inprogress",
		"--command-id", "cmd-cli-context-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	gateDefPath := filepath.Join(t.TempDir(), "context-gates.json")
	if err := os.WriteFile(gateDefPath, []byte(`{"gates":[{"id":"build","kind":"check","required":true}]}`), 0o644); err != nil {
		t.Fatalf("write gate template file: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "context-ci",
		"--version", "1",
		"--applies-to", "task",
		"--file", gateDefPath,
		"--json",
	); err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-ccccc11",
		"--template", "context-ci@1",
		"--json",
	); err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "set", "lock",
		"--db", dbPath,
		"--issue", "mem-ccccc11",
		"--json",
	); err != nil {
		t.Fatalf("gate set lock: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-ccccc11",
		"--gate", "build",
		"--result", "FAIL",
		"--evidence", "ci://run/context-cli-1",
		"--command-id", "cmd-cli-context-gate-fail-1",
		"--json",
	); err != nil {
		t.Fatalf("gate evaluate fail: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}
	var checkpoint contextCheckpointEnvelope
	if err := json.Unmarshal([]byte(stdout), &checkpoint); err != nil {
		t.Fatalf("decode context checkpoint json: %v\nstdout: %s", err, stdout)
	}
	if checkpoint.Command != "context checkpoint" || !checkpoint.Data.Created || checkpoint.Data.Session.SessionID != "sess-cli-1" {
		t.Fatalf("unexpected checkpoint response: %+v", checkpoint)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:sess-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode event log session json: %v\nstdout: %s", err, stdout)
	}
	if sessionEvents.Command != "event log" || sessionEvents.Data.EntityType != "session" || sessionEvents.Data.EntityID != "sess-cli-1" {
		t.Fatalf("unexpected session event log response: %+v", sessionEvents)
	}
	if len(sessionEvents.Data.Events) != 1 {
		t.Fatalf("expected one session event, got %+v", sessionEvents)
	}
	event := sessionEvents.Data.Events[0]
	if event.EntityType != "session" || event.EntityID != "sess-cli-1" || event.EventType != "session.checkpointed" {
		t.Fatalf("unexpected session event: %+v", event)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "build",
		"--db", dbPath,
		"--scope", "issue",
		"--id", "mem-ccccc11",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet build issue: %v\nstderr: %s", err, stderr)
	}
	var issuePacket contextPacketEnvelope
	if err := json.Unmarshal([]byte(stdout), &issuePacket); err != nil {
		t.Fatalf("decode issue packet json: %v\nstdout: %s", err, stdout)
	}
	if issuePacket.Command != "context packet build" || issuePacket.Data.Packet.PacketID == "" || issuePacket.Data.Packet.Scope != "issue" {
		t.Fatalf("unexpected issue packet response: %+v", issuePacket)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "packet:"+issuePacket.Data.Packet.PacketID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log packet: %v\nstderr: %s", err, stderr)
	}
	var packetEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &packetEvents); err != nil {
		t.Fatalf("decode event log packet json: %v\nstdout: %s", err, stdout)
	}
	if packetEvents.Command != "event log" || packetEvents.Data.EntityType != "packet" || packetEvents.Data.EntityID != issuePacket.Data.Packet.PacketID {
		t.Fatalf("unexpected packet event log response: %+v", packetEvents)
	}
	if len(packetEvents.Data.Events) != 1 || packetEvents.Data.Events[0].EventType != "packet.built" {
		t.Fatalf("expected packet.built event, got %+v", packetEvents)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "loops",
		"--db", dbPath,
		"--issue", "mem-ccccc11",
		"--json",
	)
	if err != nil {
		t.Fatalf("context loops: %v\nstderr: %s", err, stderr)
	}
	var loopsResp contextLoopsEnvelope
	if err := json.Unmarshal([]byte(stdout), &loopsResp); err != nil {
		t.Fatalf("decode context loops json: %v\nstdout: %s", err, stdout)
	}
	if loopsResp.Command != "context loops" || loopsResp.Data.Count == 0 {
		t.Fatalf("expected context loops to report persisted loops, got %+v", loopsResp)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "show",
		"--db", dbPath,
		"--packet", issuePacket.Data.Packet.PacketID,
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet show: %v\nstderr: %s", err, stderr)
	}
	var shownPacket contextPacketEnvelope
	if err := json.Unmarshal([]byte(stdout), &shownPacket); err != nil {
		t.Fatalf("decode shown packet json: %v\nstdout: %s", err, stdout)
	}
	if shownPacket.Data.Packet.PacketID != issuePacket.Data.Packet.PacketID {
		t.Fatalf("expected shown packet id %q, got %q", issuePacket.Data.Packet.PacketID, shownPacket.Data.Packet.PacketID)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "use",
		"--db", dbPath,
		"--agent", "agent-cli-1",
		"--packet", issuePacket.Data.Packet.PacketID,
		"--command-id", "cmd-cli-context-packet-use-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet use: %v\nstderr: %s", err, stderr)
	}
	var useResp contextPacketUseEnvelope
	if err := json.Unmarshal([]byte(stdout), &useResp); err != nil {
		t.Fatalf("decode context packet use json: %v\nstdout: %s", err, stdout)
	}
	if useResp.Command != "context packet use" || useResp.Data.Focus.AgentID != "agent-cli-1" || useResp.Data.Focus.LastPacketID != issuePacket.Data.Packet.PacketID {
		t.Fatalf("unexpected packet use response: %+v", useResp)
	}
	if useResp.Data.Packet.PacketID != issuePacket.Data.Packet.PacketID || useResp.Data.Packet.Scope != "issue" {
		t.Fatalf("expected packet use response to include packet metadata, got %+v", useResp)
	}
	if useResp.Data.Idempotent {
		t.Fatalf("expected first packet use to be non-idempotent")
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "focus:agent-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log focus: %v\nstderr: %s", err, stderr)
	}
	var focusEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &focusEvents); err != nil {
		t.Fatalf("decode event log focus json: %v\nstdout: %s", err, stdout)
	}
	if focusEvents.Command != "event log" || focusEvents.Data.EntityType != "focus" || focusEvents.Data.EntityID != "agent-cli-1" {
		t.Fatalf("unexpected focus event log response: %+v", focusEvents)
	}
	if len(focusEvents.Data.Events) != 1 {
		t.Fatalf("expected one focus event, got %+v", focusEvents)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "use",
		"--db", dbPath,
		"--agent", "agent-cli-1",
		"--packet", issuePacket.Data.Packet.PacketID,
		"--command-id", "cmd-cli-context-packet-use-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet use replay: %v\nstderr: %s", err, stderr)
	}
	var replayUseResp contextPacketUseEnvelope
	if err := json.Unmarshal([]byte(stdout), &replayUseResp); err != nil {
		t.Fatalf("decode replayed context packet use json: %v\nstdout: %s", err, stdout)
	}
	if !replayUseResp.Data.Idempotent {
		t.Fatalf("expected replayed packet use to be idempotent")
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--session", "sess-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate fallback: %v\nstderr: %s", err, stderr)
	}
	var fallback contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &fallback); err != nil {
		t.Fatalf("decode context rehydrate fallback json: %v\nstdout: %s", err, stdout)
	}
	if fallback.Data.Source != "raw-events-fallback" {
		t.Fatalf("expected raw-events-fallback source, got %+v", fallback)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "packet", "build",
		"--db", dbPath,
		"--scope", "session",
		"--id", "sess-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context packet build session: %v\nstderr: %s", err, stderr)
	}
	var sessionPacket contextPacketEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionPacket); err != nil {
		t.Fatalf("decode session packet json: %v\nstdout: %s", err, stdout)
	}
	if sessionPacket.Data.Packet.Scope != "session" {
		t.Fatalf("expected session scope packet, got %+v", sessionPacket)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--session", "sess-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate packet-first: %v\nstderr: %s", err, stderr)
	}
	var packetFirst contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &packetFirst); err != nil {
		t.Fatalf("decode context rehydrate packet-first json: %v\nstdout: %s", err, stdout)
	}
	if packetFirst.Data.Source != "packet" || packetFirst.Data.Packet.PacketID != sessionPacket.Data.Packet.PacketID {
		t.Fatalf("expected packet-first source with latest packet, got %+v", packetFirst)
	}
}
