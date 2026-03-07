package provenance

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestResolvePrincipalHumanCanonicalizesUsername(t *testing.T) {
	t.Parallel()

	principal, err := resolvePrincipal(
		func(string) string { return "" },
		func() (string, error) { return "Will Smith", nil },
	)
	if err != nil {
		t.Fatalf("resolve principal: %v", err)
	}
	if principal.Kind != PrincipalHuman {
		t.Fatalf("expected human principal, got %q", principal.Kind)
	}
	if principal.Actor != "human:will-smith" {
		t.Fatalf("unexpected actor: %q", principal.Actor)
	}
}

func TestResolvePrincipalLLMRequiresMetadata(t *testing.T) {
	t.Parallel()

	_, err := resolvePrincipal(func(key string) string {
		switch key {
		case EnvPrincipal:
			return "llm"
		case EnvLLMProvider:
			return "openai"
		default:
			return ""
		}
	}, func() (string, error) {
		return "ignored", nil
	})
	if err == nil || !strings.Contains(err.Error(), EnvLLMModel) {
		t.Fatalf("expected missing llm model error, got: %v", err)
	}
}

func TestResolveCommandIDGatesManualOverride(t *testing.T) {
	t.Parallel()

	_, err := resolveCommandID("issue-create", "manual-1", func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), EnvAllowManualCommandID) {
		t.Fatalf("expected manual command-id gate error, got: %v", err)
	}
}

func TestResolveCommandIDGeneratedFormat(t *testing.T) {
	t.Parallel()

	originalNow := nowUTC
	originalRandomRead := randomRead
	defer func() {
		nowUTC = originalNow
		randomRead = originalRandomRead
	}()

	nowUTC = func() time.Time {
		return time.Date(2026, 3, 7, 15, 4, 5, 123456789, time.UTC)
	}
	randomRead = func(buf []byte) error {
		copy(buf, []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff})
		return nil
	}

	commandID, err := resolveCommandID("Issue Create", "", func(string) string { return "" })
	if err != nil {
		t.Fatalf("resolve command id: %v", err)
	}
	matched, err := regexp.MatchString(`^cmdv1-issue-create-20260307t150405000000000z-aabbccddeeff$`, commandID)
	if err != nil {
		t.Fatalf("match regex: %v", err)
	}
	if !matched {
		t.Fatalf("unexpected generated command id: %q", commandID)
	}
}

func TestPasswordCredentialRoundTrip(t *testing.T) {
	t.Parallel()

	originalRandomRead := randomRead
	defer func() { randomRead = originalRandomRead }()
	randomRead = func(buf []byte) error {
		for i := range buf {
			buf[i] = byte(i + 1)
		}
		return nil
	}

	credential, err := DerivePasswordCredential("correct horse battery")
	if err != nil {
		t.Fatalf("derive credential: %v", err)
	}
	ok, err := VerifyPassword("correct horse battery", credential)
	if err != nil {
		t.Fatalf("verify password: %v", err)
	}
	if !ok {
		t.Fatalf("expected password verification success")
	}
}
