package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

type dbStatusEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		CurrentVersion    int `json:"current_version"`
		HeadVersion       int `json:"head_version"`
		PendingMigrations int `json:"pending_migrations"`
	} `json:"data"`
}

type dbVerifyEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		OK bool `json:"ok"`
	} `json:"data"`
}

func TestDBStatusMigrateAndVerifyJSON(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli.db")

	before := runDBStatusJSONForTest(t, dbPath)
	if before.Command != "db status" {
		t.Fatalf("expected db status command, got %q", before.Command)
	}
	if before.Data.CurrentVersion != 0 {
		t.Fatalf("expected current version 0 before migrate, got %d", before.Data.CurrentVersion)
	}
	if before.Data.HeadVersion == 0 {
		t.Fatalf("expected non-zero head version")
	}
	if before.Data.PendingMigrations == 0 {
		t.Fatalf("expected pending migrations before migrate")
	}

	if _, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json"); err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}

	after := runDBStatusJSONForTest(t, dbPath)
	if after.Data.CurrentVersion != after.Data.HeadVersion {
		t.Fatalf("expected current=head after migrate, got current=%d head=%d", after.Data.CurrentVersion, after.Data.HeadVersion)
	}
	if after.Data.PendingMigrations != 0 {
		t.Fatalf("expected no pending migrations after migrate, got %d", after.Data.PendingMigrations)
	}

	stdout, stderr, err := runMemoriForTest("db", "verify", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db verify: %v\nstderr: %s", err, stderr)
	}
	var verify dbVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verify); err != nil {
		t.Fatalf("decode db verify json output: %v\nstdout: %s", err, stdout)
	}
	if verify.Command != "db verify" || !verify.Data.OK {
		t.Fatalf("expected db verify ok, got %+v", verify)
	}

	_, stderr, err = runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--type", "task",
		"--title", "post migrate create",
		"--command-id", "cmd-db-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("expected issue create after db migrate to succeed: %v\nstderr: %s", err, stderr)
	}
}

func TestDBMigrateRejectsInvalidToVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-invalid.db")
	_, _, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--to", "999")
	if err == nil || !strings.Contains(err.Error(), "must be <= head version") {
		t.Fatalf("expected invalid --to error, got: %v", err)
	}
}

func runDBStatusJSONForTest(t *testing.T, dbPath string) dbStatusEnvelope {
	t.Helper()

	stdout, stderr, err := runMemoriForTest("db", "status", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db status json command: %v\nstderr: %s", err, stderr)
	}

	var resp dbStatusEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode db status json output: %v\nstdout: %s", err, stdout)
	}
	return resp
}
