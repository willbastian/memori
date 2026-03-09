package cli

import (
	"path/filepath"
	"strings"
	"testing"
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
	mustContain(t, stdout, "Next:")
	mustContain(t, stdout, "memori issue show --key mem-e111111")
	mustContain(t, stdout, "memori issue update --key mem-e111111 --status inprogress")
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
	mustContain(t, stdout, "memori context packet build --scope issue --id mem-f111111")
	mustContain(t, stdout, "memori context loops --issue mem-f111111")
	mustContain(t, stdout, "memori issue show --key mem-f111111")
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
