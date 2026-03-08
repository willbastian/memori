package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"memori/internal/provenance"
	"memori/internal/store"
)

func TestResolveMutationIdentityRequiresConfiguredHumanAuth(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	defer s.Close()

	_, err := resolveMutationIdentity(context.Background(), s, dbPath, "issue-create", "", "", mutationAuthDeps{
		resolvePrincipal: func() (provenance.Principal, error) {
			return provenance.Principal{Kind: provenance.PrincipalHuman, Actor: "human:alice"}, nil
		},
		promptPassword: func(string) (string, error) {
			t.Fatalf("password prompt should not be called when auth is unconfigured")
			return "", nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "human auth is not configured") {
		t.Fatalf("expected human auth configuration error, got: %v", err)
	}
}

func TestResolveMutationIdentityRejectsBadHumanPassword(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	defer s.Close()
	seedCLIHumanCredential(t, s, "correct horse battery")

	_, err := resolveMutationIdentity(context.Background(), s, dbPath, "issue-create", "", "", mutationAuthDeps{
		resolvePrincipal: func() (provenance.Principal, error) {
			return provenance.Principal{Kind: provenance.PrincipalHuman, Actor: "human:alice"}, nil
		},
		promptPassword: func(string) (string, error) {
			return "wrong password", nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "human auth verification failed") {
		t.Fatalf("expected password verification error, got: %v", err)
	}
}

func TestResolveMutationIdentityAllowsValidHumanPassword(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	defer s.Close()
	seedCLIHumanCredential(t, s, "correct horse battery")

	identity, err := resolveMutationIdentity(context.Background(), s, dbPath, "issue-create", "ignored", "", mutationAuthDeps{
		resolvePrincipal: func() (provenance.Principal, error) {
			return provenance.Principal{Kind: provenance.PrincipalHuman, Actor: "human:alice-smith"}, nil
		},
		promptPassword: func(string) (string, error) {
			return "correct horse battery", nil
		},
	})
	if err != nil {
		t.Fatalf("resolve mutation identity: %v", err)
	}
	if identity.Actor != "human:alice-smith" {
		t.Fatalf("expected canonical human actor, got %q", identity.Actor)
	}
	if !strings.HasPrefix(identity.CommandID, "cmdv1-issue-create-") {
		t.Fatalf("expected generated command id, got %q", identity.CommandID)
	}
}

func TestResolveMutationIdentityPropagatesNonInteractivePromptError(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	defer s.Close()
	seedCLIHumanCredential(t, s, "correct horse battery")

	expected := errors.New("human mutation auth requires an interactive terminal")
	_, err := resolveMutationIdentity(context.Background(), s, dbPath, "issue-create", "", "", mutationAuthDeps{
		resolvePrincipal: func() (provenance.Principal, error) {
			return provenance.Principal{Kind: provenance.PrincipalHuman, Actor: "human:alice"}, nil
		},
		promptPassword: func(string) (string, error) {
			return "", expected
		},
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected non-interactive prompt error, got: %v", err)
	}
}

func TestRunAuthSetPasswordAllowsInteractiveThinkTime(t *testing.T) {
	s, dbPath := newCLIAuthTestStore(t)
	s.Close()
	t.Setenv(provenance.EnvPrincipal, provenance.PrincipalHuman)

	originalPrompter := passwordPrompter
	originalTimeout := authCommandTimeout
	t.Cleanup(func() {
		passwordPrompter = originalPrompter
		authCommandTimeout = originalTimeout
	})

	authCommandTimeout = 20 * time.Millisecond
	passwordPrompter = func(prompt string) (string, error) {
		time.Sleep(40 * time.Millisecond)
		return "correct horse battery", nil
	}

	if err := runAuthSetPassword([]string{"--db", dbPath}, io.Discard); err != nil {
		t.Fatalf("run auth set-password: %v", err)
	}

	verifyStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store for verification: %v", err)
	}
	defer verifyStore.Close()

	credential, configured, err := verifyStore.GetHumanAuthCredential(context.Background())
	if err != nil {
		t.Fatalf("get human auth credential: %v", err)
	}
	if !configured {
		t.Fatal("expected human auth credential to be configured")
	}
	if credential.HashHex == "" {
		t.Fatal("expected stored password hash")
	}
}

func newCLIAuthTestStore(t *testing.T) (*store.Store, string) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-provenance-auth.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Initialize(context.Background(), store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	return s, dbPath
}

func seedCLIHumanCredential(t *testing.T, s *store.Store, password string) {
	t.Helper()

	credential, err := provenance.DerivePasswordCredential(password)
	if err != nil {
		t.Fatalf("derive password credential: %v", err)
	}
	if _, _, err := s.UpsertHumanAuthCredential(context.Background(), store.UpsertHumanAuthCredentialParams{
		Algorithm:  credential.Algorithm,
		Iterations: credential.Iterations,
		SaltHex:    credential.SaltHex,
		HashHex:    credential.HashHex,
		Actor:      "human:setup",
	}); err != nil {
		t.Fatalf("upsert human auth credential: %v", err)
	}
}
