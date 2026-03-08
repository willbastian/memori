package store

import (
	"context"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
)

func TestSchemaVersionMatchesHeadVersion(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	got, err := s.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}

	head, err := dbschema.HeadVersion()
	if err != nil {
		t.Fatalf("schema head version: %v", err)
	}
	if got != head {
		t.Fatalf("expected schema version %d, got %d", head, got)
	}
}

func TestHumanAuthCredentialRoundTripAndRotation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, configured, err := s.GetHumanAuthCredential(ctx); err != nil {
		t.Fatalf("get human auth credential before configure: %v", err)
	} else if configured {
		t.Fatal("expected human auth credential to start unconfigured")
	}

	first, rotated, err := s.UpsertHumanAuthCredential(ctx, UpsertHumanAuthCredentialParams{
		Algorithm:  "pbkdf2-sha256",
		Iterations: 600000,
		SaltHex:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		HashHex:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Actor:      "human:alice",
	})
	if err != nil {
		t.Fatalf("first upsert human auth credential: %v", err)
	}
	if rotated {
		t.Fatal("expected first credential write to configure rather than rotate")
	}
	if first.RotatedBy != "human:alice" {
		t.Fatalf("expected first credential actor %q, got %q", "human:alice", first.RotatedBy)
	}
	if first.CreatedAt == "" || first.UpdatedAt == "" {
		t.Fatalf("expected timestamps on stored credential, got created=%q updated=%q", first.CreatedAt, first.UpdatedAt)
	}

	stored, configured, err := s.GetHumanAuthCredential(ctx)
	if err != nil {
		t.Fatalf("get configured human auth credential: %v", err)
	}
	if !configured {
		t.Fatal("expected human auth credential to be configured after first upsert")
	}
	if stored.HashHex != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" || stored.Algorithm != "pbkdf2-sha256" {
		t.Fatalf("unexpected stored credential after first upsert: %+v", stored)
	}

	second, rotated, err := s.UpsertHumanAuthCredential(ctx, UpsertHumanAuthCredentialParams{
		Algorithm:  "pbkdf2-sha256",
		Iterations: 700000,
		SaltHex:    "cccccccccccccccccccccccccccccccc",
		HashHex:    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		Actor:      "human:bob",
	})
	if err != nil {
		t.Fatalf("second upsert human auth credential: %v", err)
	}
	if !rotated {
		t.Fatal("expected second credential write to report rotation")
	}
	if second.CreatedAt != first.CreatedAt {
		t.Fatalf("expected credential created_at to remain stable across rotation, got %q vs %q", second.CreatedAt, first.CreatedAt)
	}
	if second.HashHex != "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" || second.Iterations != 700000 {
		t.Fatalf("unexpected rotated credential fields: %+v", second)
	}
	if second.RotatedBy != "human:bob" {
		t.Fatalf("expected rotated credential actor %q, got %q", "human:bob", second.RotatedBy)
	}
}
