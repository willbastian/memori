package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAuthStatusJSONReportsUnconfiguredCredential(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close auth test store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("auth", "status", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run auth status json: %v\nstderr: %s", err, stderr)
	}

	var resp struct {
		Command string         `json:"command"`
		Data    authStatusData `json:"data"`
	}
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode auth status json: %v\nstdout: %s", err, stdout)
	}
	if resp.Command != "auth status" {
		t.Fatalf("expected command %q, got %q", "auth status", resp.Command)
	}
	if resp.Data.Configured {
		t.Fatalf("expected unconfigured auth status, got %+v", resp.Data)
	}
	if resp.Data.Algorithm != "" || resp.Data.Iterations != 0 {
		t.Fatalf("expected no stored credential details, got %+v", resp.Data)
	}
}

func TestAuthStatusHumanOutputShowsConfiguredCredential(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	seedCLIHumanCredential(t, s, "correct horse battery")
	if err := s.Close(); err != nil {
		t.Fatalf("close seeded auth test store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("auth", "status", "--db", dbPath)
	if err != nil {
		t.Fatalf("run auth status text: %v\nstderr: %s", err, stderr)
	}

	for _, want := range []string{
		"Human auth: configured",
		"Algorithm:",
		"Iterations:",
		"Updated:",
		"Rotated By: human:setup",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected auth status output to contain %q, got:\n%s", want, stdout)
		}
	}
}
