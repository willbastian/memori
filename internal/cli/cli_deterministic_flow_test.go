package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDeterministicReplayJSONAndRehydrateContinuity(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-deterministic.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	run := func(args ...string) string {
		t.Helper()
		stdout, stderr, err := runMemoriForTest(args...)
		if err != nil {
			t.Fatalf("command failed: %v\nargs: %v\nstderr: %s", err, args, stderr)
		}
		return stdout
	}

	run("issue", "create",
		"--db", dbPath,
		"--key", "mem-d111111",
		"--type", "task",
		"--title", "Deterministic flow test",
		"--description", "Initial description",
		"--acceptance-criteria", "Replay keeps projections stable",
		"--reference", "plan:deterministic-flow-test",
		"--command-id", "cmd-det-create-1",
		"--json",
	)
	run("issue", "update",
		"--db", dbPath,
		"--key", "mem-d111111",
		"--status", "inprogress",
		"--priority", "p1",
		"--label", "deterministic",
		"--label", "ci",
		"--description", "Updated once before replay",
		"--command-id", "cmd-det-update-1",
		"--json",
	)

	gateDefPath := filepath.Join(t.TempDir(), "deterministic-gates.json")
	if err := os.WriteFile(gateDefPath, []byte(`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`), 0o644); err != nil {
		t.Fatalf("write gate template file: %v", err)
	}
	run("gate", "template", "create",
		"--db", dbPath,
		"--id", "deterministic",
		"--version", "1",
		"--applies-to", "task",
		"--file", gateDefPath,
		"--json",
	)
	run("gate", "set", "instantiate", "--db", dbPath, "--issue", "mem-d111111", "--template", "deterministic@1", "--json")
	run("gate", "set", "lock", "--db", dbPath, "--issue", "mem-d111111", "--json")
	run("gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-d111111",
		"--gate", "build",
		"--result", "PASS",
		"--evidence", "ci://deterministic/1",
		"--command-id", "cmd-det-gate-eval-1",
		"--json",
	)

	run("context", "checkpoint", "--db", dbPath, "--session", "sess-det-1", "--trigger", "manual", "--json")
	packetBuildStdout := run("context", "packet", "build", "--db", dbPath, "--scope", "session", "--id", "sess-det-1", "--json")
	var built struct {
		Data struct {
			Packet struct {
				PacketID string `json:"packet_id"`
			} `json:"packet"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(packetBuildStdout), &built); err != nil {
		t.Fatalf("decode packet build json: %v\nstdout: %s", err, packetBuildStdout)
	}
	if built.Data.Packet.PacketID == "" {
		t.Fatalf("expected packet build to return packet_id")
	}

	rehydrateBeforeStdout := run("context", "rehydrate", "--db", dbPath, "--session", "sess-det-1", "--json")
	var rehydrateBefore struct {
		Data struct {
			Source string `json:"source"`
			Packet struct {
				PacketID string `json:"packet_id"`
			} `json:"packet"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(rehydrateBeforeStdout), &rehydrateBefore); err != nil {
		t.Fatalf("decode rehydrate before json: %v\nstdout: %s", err, rehydrateBeforeStdout)
	}
	if rehydrateBefore.Data.Source != "packet" || rehydrateBefore.Data.Packet.PacketID != built.Data.Packet.PacketID {
		t.Fatalf("expected packet-first rehydrate before replay, got %+v", rehydrateBefore)
	}

	showBeforeStdout := run("issue", "show", "--db", dbPath, "--key", "mem-d111111", "--json")
	var showBefore struct {
		Data struct {
			Issue struct {
				ID          string   `json:"id"`
				Status      string   `json:"status"`
				Priority    string   `json:"priority"`
				Labels      []string `json:"labels"`
				Description string   `json:"description"`
			} `json:"issue"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(showBeforeStdout), &showBefore); err != nil {
		t.Fatalf("decode issue show before replay json: %v\nstdout: %s", err, showBeforeStdout)
	}

	run("db", "replay", "--db", dbPath, "--json")
	run("db", "replay", "--db", dbPath, "--json")

	showAfterStdout := run("issue", "show", "--db", dbPath, "--key", "mem-d111111", "--json")
	var showAfter struct {
		Data struct {
			Issue struct {
				ID          string   `json:"id"`
				Status      string   `json:"status"`
				Priority    string   `json:"priority"`
				Labels      []string `json:"labels"`
				Description string   `json:"description"`
			} `json:"issue"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(showAfterStdout), &showAfter); err != nil {
		t.Fatalf("decode issue show after replay json: %v\nstdout: %s", err, showAfterStdout)
	}
	if !reflect.DeepEqual(showBefore.Data.Issue, showAfter.Data.Issue) {
		t.Fatalf("issue show JSON contract changed across replay\nbefore=%+v\nafter=%+v", showBefore.Data.Issue, showAfter.Data.Issue)
	}

	rehydrateAfterStdout := run("context", "rehydrate", "--db", dbPath, "--session", "sess-det-1", "--json")
	var rehydrateAfter struct {
		Data struct {
			Source string `json:"source"`
			Packet struct {
				PacketID string `json:"packet_id"`
			} `json:"packet"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(rehydrateAfterStdout), &rehydrateAfter); err != nil {
		t.Fatalf("decode rehydrate after json: %v\nstdout: %s", err, rehydrateAfterStdout)
	}
	if rehydrateAfter.Data.Source != "packet" || rehydrateAfter.Data.Packet.PacketID != built.Data.Packet.PacketID {
		t.Fatalf("expected packet-first rehydrate after replay, got %+v", rehydrateAfter)
	}
}
