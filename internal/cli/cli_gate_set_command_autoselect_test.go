package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestGateSetInstantiateAutoSelectsLatestEligibleTemplateVersion(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-autoselect.db")
	fixture.createIssue("mem-a211111", "story", "Auto-select story close template", "cmd-cli-gset-autoselect-create-1")

	defV1Path := fixture.writeTemplateDefinition(
		"close-story-v1.json",
		`{"gates":[{"id":"verify-v1","kind":"check","required":true,"criteria":{"command":"echo v1"}}]}`,
	)
	fixture.createTemplateCLI("close-story", 1, "story", defV1Path, "cmd-cli-gtemplate-autoselect-create-v1")

	s := fixture.openStore()
	defer s.Close()
	ctx := context.Background()
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story",
		Version:    1,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gtemplate-autoselect-approve-v1",
	}); err != nil {
		t.Fatalf("approve gate template v1 via store: %v", err)
	}

	defV2Path := fixture.writeTemplateDefinition(
		"close-story-v2.json",
		`{"gates":[{"id":"verify-v2","kind":"check","required":true,"criteria":{"command":"echo v2"}}]}`,
	)
	fixture.createTemplateCLI("close-story", 2, "story", defV2Path, "cmd-cli-gtemplate-autoselect-create-v2")
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story",
		Version:    2,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gtemplate-autoselect-approve-v2",
	}); err != nil {
		t.Fatalf("approve gate template v2 via store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a211111",
		"--command-id", "cmd-cli-gset-autoselect-instantiate-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}

	var instantiated gateSetInstantiateEnvelope
	if err := json.Unmarshal([]byte(stdout), &instantiated); err != nil {
		t.Fatalf("decode gate set instantiate json: %v\nstdout: %s", err, stdout)
	}
	if !instantiated.Data.AutoSelected {
		t.Fatalf("expected gate set instantiate to report auto_selected")
	}
	if len(instantiated.Data.GateSet.TemplateRefs) != 1 || instantiated.Data.GateSet.TemplateRefs[0] != "close-story@2" {
		t.Fatalf("expected auto-selected latest eligible template, got %+v", instantiated.Data.GateSet.TemplateRefs)
	}
}

func TestGateSetInstantiateAutoSelectedHumanOutputShowsSelectedTemplate(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gset-autoselect-text.db")
	fixture.createIssue("mem-a818181", "story", "Auto-select close template text issue", "cmd-cli-gset-autoselect-text-issue-1")

	fixture.createTemplateStore(
		"close-story-text",
		1,
		[]string{"story"},
		`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo v1"}}]}`,
		"cmd-cli-gset-autoselect-text-template-v1",
	)
	fixture.approveTemplateStore("close-story-text", 1, "cmd-cli-gset-autoselect-text-approve-v1")
	fixture.createTemplateStore(
		"close-story-text",
		2,
		[]string{"story"},
		`{"gates":[{"id":"verify","kind":"check","required":true,"criteria":{"command":"echo v2"}}]}`,
		"cmd-cli-gset-autoselect-text-template-v2",
	)
	fixture.approveTemplateStore("close-story-text", 2, "cmd-cli-gset-autoselect-text-approve-v2")

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a818181",
		"--command-id", "cmd-cli-gset-autoselect-text-instantiate-1",
	)
	if err != nil {
		t.Fatalf("gate set instantiate auto-selected text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Auto-selected templates: close-story-text@2",
		"Instantiated gate set ",
		"Templates: close-story-text@2",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected auto-selected instantiate text output to contain %q, got:\n%s", want, stdout)
		}
	}
}
