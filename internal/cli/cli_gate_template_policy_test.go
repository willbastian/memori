package cli

import (
	"testing"
)

func TestGateDefinitionHasExecutableCommandDetectsCommand(t *testing.T) {
	t.Parallel()

	ok, err := gateDefinitionHasExecutableCommand(
		`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
	)
	if err != nil {
		t.Fatalf("detect executable command: %v", err)
	}
	if !ok {
		t.Fatalf("expected executable command to be detected")
	}
}

func TestGateDefinitionHasExecutableCommandIgnoresNonExecutableDefinition(t *testing.T) {
	t.Parallel()

	ok, err := gateDefinitionHasExecutableCommand(
		`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-review"}}]}`,
	)
	if err != nil {
		t.Fatalf("inspect non-executable definition: %v", err)
	}
	if ok {
		t.Fatalf("expected non-executable definition to be ignored")
	}
}

func TestGateDefinitionHasExecutableCommandRejectsEmptyJSON(t *testing.T) {
	t.Parallel()

	if _, err := gateDefinitionHasExecutableCommand(""); err == nil {
		t.Fatalf("expected empty json to fail")
	}
}
