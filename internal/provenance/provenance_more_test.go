package provenance

import (
	"errors"
	"os"
	"os/user"
	"strings"
	"testing"
)

func TestResolvePrincipalLLMSucceeds(t *testing.T) {
	t.Parallel()

	principal, err := resolvePrincipal(func(key string) string {
		switch key {
		case EnvPrincipal:
			return "llm"
		case EnvLLMProvider:
			return " OpenAI "
		case EnvLLMModel:
			return " GPT-5 "
		default:
			return ""
		}
	}, func() (string, error) {
		return "ignored", nil
	})
	if err != nil {
		t.Fatalf("resolve llm principal: %v", err)
	}
	if principal.Actor != "llm:openai:gpt-5" {
		t.Fatalf("unexpected llm actor: %q", principal.Actor)
	}
}

func TestResolvePrincipalRejectsInvalidKindAndUsernameFailure(t *testing.T) {
	t.Parallel()

	_, err := resolvePrincipal(func(string) string { return "robot" }, func() (string, error) {
		return "ignored", nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid "+EnvPrincipal) {
		t.Fatalf("expected invalid principal kind error, got: %v", err)
	}

	expected := errors.New("no username")
	_, err = resolvePrincipal(func(string) string { return "human" }, func() (string, error) {
		return "", expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected username lookup error, got: %v", err)
	}

	_, err = resolvePrincipal(func(string) string { return "human" }, func() (string, error) {
		return "!!!", nil
	})
	if err == nil || !strings.Contains(err.Error(), "canonical human username") {
		t.Fatalf("expected canonical username error, got: %v", err)
	}

	_, err = resolvePrincipal(func(key string) string {
		if key == EnvPrincipal {
			return "llm"
		}
		return ""
	}, func() (string, error) {
		return "ignored", nil
	})
	if err == nil || !strings.Contains(err.Error(), EnvLLMProvider) {
		t.Fatalf("expected missing provider error, got: %v", err)
	}
}

func TestResolveCommandIDManualAllowedAndEntropyFailure(t *testing.T) {
	id, err := resolveCommandID("issue-create", "manual-1", func(string) string { return "1" })
	if err != nil {
		t.Fatalf("resolve manual command id: %v", err)
	}
	if id != "manual-1" {
		t.Fatalf("unexpected manual command id: %q", id)
	}

	originalRandomRead := randomRead
	defer func() { randomRead = originalRandomRead }()
	randomRead = func([]byte) error { return errors.New("entropy unavailable") }

	_, err = resolveCommandID("", "", func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "generate command id entropy") {
		t.Fatalf("expected entropy failure, got: %v", err)
	}
}

func TestCurrentUsernameFallbacksAndErrors(t *testing.T) {
	originalCurrentUser := currentUser
	originalUser := os.Getenv("USER")
	t.Cleanup(func() {
		currentUser = originalCurrentUser
		if originalUser == "" {
			_ = os.Unsetenv("USER")
		} else {
			_ = os.Setenv("USER", originalUser)
		}
	})

	currentUser = func() (*user.User, error) {
		return nil, errors.New("lookup failed")
	}
	_ = os.Setenv("USER", "fallback-user")
	got, err := currentUsername()
	if err != nil {
		t.Fatalf("current username from env: %v", err)
	}
	if got != "fallback-user" {
		t.Fatalf("expected env fallback username, got %q", got)
	}

	_ = os.Unsetenv("USER")
	_, err = currentUsername()
	if err == nil || !strings.Contains(err.Error(), "unable to determine system username") {
		t.Fatalf("expected username lookup failure, got: %v", err)
	}
}

func TestCurrentUsernameReturnsSystemUserWhenAvailable(t *testing.T) {
	originalCurrentUser := currentUser
	originalUser := os.Getenv("USER")
	t.Cleanup(func() {
		currentUser = originalCurrentUser
		if originalUser == "" {
			_ = os.Unsetenv("USER")
		} else {
			_ = os.Setenv("USER", originalUser)
		}
	})
	currentUser = func() (*user.User, error) {
		return &user.User{Username: "system-user"}, nil
	}

	got, err := currentUsername()
	if err != nil {
		t.Fatalf("current username from system user: %v", err)
	}
	if got != "system-user" {
		t.Fatalf("expected system username, got %q", got)
	}

	currentUser = func() (*user.User, error) {
		return &user.User{Username: "   "}, nil
	}
	_ = os.Setenv("USER", "blank-fallback")
	got, err = currentUsername()
	if err != nil {
		t.Fatalf("current username from blank system user fallback: %v", err)
	}
	if got != "blank-fallback" {
		t.Fatalf("expected blank-username env fallback, got %q", got)
	}
}

func TestResolvePrincipalAndResolveCommandIDExportedWrappers(t *testing.T) {
	originalPrincipal := os.Getenv(EnvPrincipal)
	originalProvider := os.Getenv(EnvLLMProvider)
	originalModel := os.Getenv(EnvLLMModel)
	originalAllowManual := os.Getenv(EnvAllowManualCommandID)
	t.Cleanup(func() {
		_ = os.Setenv(EnvPrincipal, originalPrincipal)
		_ = os.Setenv(EnvLLMProvider, originalProvider)
		_ = os.Setenv(EnvLLMModel, originalModel)
		_ = os.Setenv(EnvAllowManualCommandID, originalAllowManual)
	})

	_ = os.Setenv(EnvPrincipal, PrincipalLLM)
	_ = os.Setenv(EnvLLMProvider, "openai")
	_ = os.Setenv(EnvLLMModel, "gpt-5")
	_ = os.Setenv(EnvAllowManualCommandID, "1")

	principal, err := ResolvePrincipal()
	if err != nil {
		t.Fatalf("ResolvePrincipal: %v", err)
	}
	if principal.Actor != "llm:openai:gpt-5" {
		t.Fatalf("unexpected exported principal actor: %q", principal.Actor)
	}

	commandID, err := ResolveCommandID("issue-create", "manual-id")
	if err != nil {
		t.Fatalf("ResolveCommandID: %v", err)
	}
	if commandID != "manual-id" {
		t.Fatalf("unexpected exported command id: %q", commandID)
	}
}

func TestCanonicalTokenAndEnvEnabled(t *testing.T) {
	t.Parallel()

	if got := canonicalToken("  Hello, World!  "); got != "hello-world" {
		t.Fatalf("unexpected canonical token: %q", got)
	}
	for _, raw := range []string{"1", "true", "yes", "on", "TRUE"} {
		if !envEnabled(raw) {
			t.Fatalf("expected envEnabled true for %q", raw)
		}
	}
	if envEnabled("0") {
		t.Fatal("expected envEnabled false for 0")
	}
}

func TestDerivePasswordCredentialAndVerifyRejectInvalidInputs(t *testing.T) {
	if _, err := DerivePasswordCredential("too short"); err == nil || !strings.Contains(err.Error(), "at least 12") {
		t.Fatalf("expected short password error, got: %v", err)
	}

	originalRandomRead := randomRead
	defer func() { randomRead = originalRandomRead }()
	randomRead = func([]byte) error { return errors.New("random broken") }
	if _, err := DerivePasswordCredential("correct horse battery"); err == nil || !strings.Contains(err.Error(), "generate password salt") {
		t.Fatalf("expected derive random error, got: %v", err)
	}

	cases := []struct {
		name       string
		credential PasswordCredential
		want       string
	}{
		{
			name: "algorithm",
			credential: PasswordCredential{
				Algorithm:  "bcrypt",
				Iterations: DefaultPasswordIterations,
				SaltHex:    strings.Repeat("a", 32),
				HashHex:    strings.Repeat("b", 64),
			},
			want: "unsupported password algorithm",
		},
		{
			name: "iterations",
			credential: PasswordCredential{
				Algorithm:  PasswordAlgorithm,
				Iterations: DefaultPasswordIterations - 1,
				SaltHex:    strings.Repeat("a", 32),
				HashHex:    strings.Repeat("b", 64),
			},
			want: "below minimum",
		},
		{
			name: "salt hex",
			credential: PasswordCredential{
				Algorithm:  PasswordAlgorithm,
				Iterations: DefaultPasswordIterations,
				SaltHex:    "zz",
				HashHex:    strings.Repeat("b", 64),
			},
			want: "decode password salt",
		},
		{
			name: "hash hex",
			credential: PasswordCredential{
				Algorithm:  PasswordAlgorithm,
				Iterations: DefaultPasswordIterations,
				SaltHex:    strings.Repeat("a", 32),
				HashHex:    "zz",
			},
			want: "decode password hash",
		},
		{
			name: "hash length",
			credential: PasswordCredential{
				Algorithm:  PasswordAlgorithm,
				Iterations: DefaultPasswordIterations,
				SaltHex:    strings.Repeat("a", 32),
				HashHex:    strings.Repeat("b", 2),
			},
			want: "invalid password hash length",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := VerifyPassword("correct horse battery", tc.credential); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got: %v", tc.want, err)
			}
		})
	}
}
