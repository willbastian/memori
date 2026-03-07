package cli

import (
	"strings"
	"testing"

	"memori/internal/provenance"
)

func TestValidateExecutableGateTemplatePolicyRejectsLLMPrincipal(t *testing.T) {
	t.Parallel()

	err := validateExecutableGateTemplatePolicy(
		`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		provenance.Principal{Kind: provenance.PrincipalLLM, Actor: "llm:test:model"},
	)
	if err == nil || !strings.Contains(err.Error(), "require a human principal") {
		t.Fatalf("expected executable-template governance error, got: %v", err)
	}
}

func TestValidateExecutableGateTemplatePolicyAllowsHumanPrincipal(t *testing.T) {
	t.Parallel()

	err := validateExecutableGateTemplatePolicy(
		`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		provenance.Principal{Kind: provenance.PrincipalHuman, Actor: "human:alice"},
	)
	if err != nil {
		t.Fatalf("expected human principal to be allowed, got: %v", err)
	}
}

func TestValidateExecutableGateTemplatePolicyAllowsNonExecutableDefinitionForLLM(t *testing.T) {
	t.Parallel()

	err := validateExecutableGateTemplatePolicy(
		`{"gates":[{"id":"build","kind":"check","required":true}]}`,
		provenance.Principal{Kind: provenance.PrincipalLLM, Actor: "llm:test:model"},
	)
	if err != nil {
		t.Fatalf("expected non-executable definition to be allowed, got: %v", err)
	}
}
