package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"memori/internal/store"
)

type gateTemplateCreateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Template   store.GateTemplate `json:"template"`
		Idempotent bool               `json:"idempotent"`
	} `json:"data"`
}

type gateTemplateListEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count     int                  `json:"count"`
		Templates []store.GateTemplate `json:"templates"`
	} `json:"data"`
}

type gateSetInstantiateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet    store.GateSet `json:"gate_set"`
		Idempotent bool          `json:"idempotent"`
	} `json:"data"`
}

type gateSetLockEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet   store.GateSet `json:"gate_set"`
		LockedNow bool          `json:"locked_now"`
	} `json:"data"`
}

func TestGateTemplateCreateListInstantiateAndLockFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-set.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--type", "task",
		"--title", "Gate set flow issue",
		"--command-id", "cmd-cli-gset-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "quality-gates.json")
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false,"criteria":{"command":"golangci-lint run"}}]}`
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "quality",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	var created gateTemplateCreateEnvelope
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode gate template create json: %v\nstdout: %s", err, stdout)
	}
	if created.Command != "gate template create" {
		t.Fatalf("expected gate template create command, got %q", created.Command)
	}
	if created.Data.Idempotent {
		t.Fatalf("expected first template create to be non-idempotent")
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "list", "--db", dbPath, "--type", "task", "--json")
	if err != nil {
		t.Fatalf("gate template list: %v\nstderr: %s", err, stderr)
	}
	var listed gateTemplateListEnvelope
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode gate template list json: %v\nstdout: %s", err, stdout)
	}
	if listed.Command != "gate template list" {
		t.Fatalf("expected gate template list command, got %q", listed.Command)
	}
	if listed.Data.Count != 1 || len(listed.Data.Templates) != 1 {
		t.Fatalf("expected one template in list response, got count=%d templates=%d", listed.Data.Count, len(listed.Data.Templates))
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--template", "quality@1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}
	var instantiated gateSetInstantiateEnvelope
	if err := json.Unmarshal([]byte(stdout), &instantiated); err != nil {
		t.Fatalf("decode gate set instantiate json: %v\nstdout: %s", err, stdout)
	}
	if instantiated.Command != "gate set instantiate" {
		t.Fatalf("expected gate set instantiate command, got %q", instantiated.Command)
	}
	if instantiated.Data.Idempotent {
		t.Fatalf("expected first gate set instantiate to be non-idempotent")
	}
	if len(instantiated.Data.GateSet.Items) != 2 {
		t.Fatalf("expected 2 instantiated gate items, got %d", len(instantiated.Data.GateSet.Items))
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set lock: %v\nstderr: %s", err, stderr)
	}
	var locked gateSetLockEnvelope
	if err := json.Unmarshal([]byte(stdout), &locked); err != nil {
		t.Fatalf("decode gate set lock json: %v\nstdout: %s", err, stdout)
	}
	if locked.Command != "gate set lock" {
		t.Fatalf("expected gate set lock command, got %q", locked.Command)
	}
	if !locked.Data.LockedNow {
		t.Fatalf("expected first gate set lock call to lock now")
	}
	if strings.TrimSpace(locked.Data.GateSet.LockedAt) == "" {
		t.Fatalf("expected locked_at to be set")
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--json",
	)
	if err != nil {
		t.Fatalf("second gate set lock: %v\nstderr: %s", err, stderr)
	}
	var relock gateSetLockEnvelope
	if err := json.Unmarshal([]byte(stdout), &relock); err != nil {
		t.Fatalf("decode second gate set lock json: %v\nstdout: %s", err, stdout)
	}
	if relock.Data.LockedNow {
		t.Fatalf("expected second lock call to report already locked")
	}
}

func TestGateSetInstantiateRejectsTemplateTypeMismatch(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-mismatch.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b111111",
		"--type", "task",
		"--title", "Type mismatch issue",
		"--command-id", "cmd-cli-gset-mismatch-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "story-only-gates.json")
	if err := os.WriteFile(defPath, []byte(`{"gates":[{"id":"story-check"}]}`), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "storyonly",
		"--version", "1",
		"--applies-to", "story",
		"--file", defPath,
		"--json",
	); err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-b111111",
		"--template", "storyonly@1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "does not apply to issue type Task") {
		t.Fatalf("expected template type mismatch error, got: %v", err)
	}
}
