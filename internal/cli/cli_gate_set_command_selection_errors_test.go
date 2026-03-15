package cli

import (
	"strings"
	"testing"
)

func TestGateSetInstantiateRejectsTemplateTypeMismatch(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-mismatch.db")
	fixture.createIssue("mem-b111111", "task", "Type mismatch issue", "cmd-cli-gset-mismatch-create-1")

	defPath := fixture.writeTemplateDefinition("story-only-gates.json", `{"gates":[{"id":"story-check"}]}`)
	fixture.createTemplateCLI("storyonly", 1, "story", defPath, "cmd-cli-gtemplate-create-mismatch-1")

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-b111111",
		"--template", "storyonly@1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "does not apply to issue type Task") {
		t.Fatalf("expected template type mismatch error, got: %v", err)
	}
}

func TestGateSetInstantiateWithoutTemplateRejectsAmbiguousEligibleTemplates(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-ambiguous.db")
	fixture.createIssue("mem-a311111", "task", "Ambiguous close template issue", "cmd-cli-gset-ambiguous-create-1")

	qualityPath := fixture.writeTemplateDefinition(
		"quality.json",
		`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-build"}}]}`,
	)
	fixture.createTemplateCLI("quality", 1, "task", qualityPath, "cmd-cli-gtemplate-ambiguous-quality-1")

	provenancePath := fixture.writeTemplateDefinition(
		"provenance.json",
		`{"gates":[{"id":"audit","kind":"check","required":true,"criteria":{"ref":"manual-audit"}}]}`,
	)
	fixture.createTemplateCLI("provenance", 1, "task", provenancePath, "cmd-cli-gtemplate-ambiguous-provenance-1")

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a311111",
		"--command-id", "cmd-cli-gset-ambiguous-instantiate-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "multiple eligible gate templates apply to issue type Task; specify --template explicitly: provenance@1, quality@1") {
		t.Fatalf("expected ambiguous template error, got: %v", err)
	}
}

func TestGateSetInstantiateWithoutTemplateExplainsPendingApproval(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-pending.db")
	fixture.createIssue("mem-a411111", "story", "Pending approval close template issue", "cmd-cli-gset-pending-create-1")

	defPath := fixture.writeTemplateDefinition(
		"pending-close-story.json",
		`{"gates":[{"id":"verify","kind":"check","required":true,"criteria":{"command":"echo pending"}}]}`,
	)
	fixture.createTemplateCLI("close-story", 1, "story", defPath, "cmd-cli-gtemplate-pending-create-1")

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a411111",
		"--command-id", "cmd-cli-gset-pending-instantiate-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "no eligible gate templates apply to issue type Story; pending approval: close-story@1") {
		t.Fatalf("expected pending approval error, got: %v", err)
	}
}
