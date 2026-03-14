package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestOpenCreatesParentDirectoryAndSchemaVersionHandlesUnsetAndInvalidValues(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "nested", "state", "memori.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open nested store path: %v", err)
	}
	defer s.Close()

	if _, err := s.SchemaVersion(context.Background()); err == nil || !strings.Contains(err.Error(), "query schema version") {
		t.Fatalf("expected missing schema_meta query error before initialize, got %v", err)
	}

	if err := s.Initialize(context.Background(), InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	if _, err := s.db.ExecContext(context.Background(), `
		DELETE FROM schema_meta WHERE key = 'db_schema_version'
	`); err != nil {
		t.Fatalf("delete schema version: %v", err)
	}
	if got, err := s.SchemaVersion(context.Background()); err != nil || got != 0 {
		t.Fatalf("expected missing schema version row to read as 0, got %d err=%v", got, err)
	}

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO schema_meta(key, value, updated_at) VALUES('db_schema_version', 'not-a-number', ?)
	`, nowUTC()); err != nil {
		t.Fatalf("insert invalid schema version: %v", err)
	}

	if _, err := s.SchemaVersion(context.Background()); err == nil || !strings.Contains(err.Error(), "parse schema version") {
		t.Fatalf("expected invalid schema version parse error, got %v", err)
	}
}

func TestOpenAndInitializeSurfaceFilesystemAndClosedDBErrors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	parentFile := filepath.Join(root, "not-a-dir")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write parent file: %v", err)
	}

	if _, err := Open(filepath.Join(parentFile, "memori.db")); err == nil || !strings.Contains(err.Error(), "create db directory") {
		t.Fatalf("expected open mkdir error, got %v", err)
	}

	s, err := Open(filepath.Join(root, "closed.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := s.Initialize(context.Background(), InitializeParams{IssueKeyPrefix: "mem"}); err == nil || !strings.Contains(err.Error(), "migrate schema") {
		t.Fatalf("expected initialize on closed db to fail during migrate, got %v", err)
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

func TestHumanAuthCredentialValidationAndClosedDBErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	cases := []struct {
		name string
		p    UpsertHumanAuthCredentialParams
		want string
	}{
		{
			name: "missing algorithm",
			p: UpsertHumanAuthCredentialParams{
				Iterations: 1,
				SaltHex:    "aa",
				HashHex:    "bb",
			},
			want: "algorithm is required",
		},
		{
			name: "non-positive iterations",
			p: UpsertHumanAuthCredentialParams{
				Algorithm:  "pbkdf2-sha256",
				Iterations: 0,
				SaltHex:    "aa",
				HashHex:    "bb",
			},
			want: "iterations must be > 0",
		},
		{
			name: "missing salt",
			p: UpsertHumanAuthCredentialParams{
				Algorithm:  "pbkdf2-sha256",
				Iterations: 1,
				HashHex:    "bb",
			},
			want: "salt_hex is required",
		},
		{
			name: "missing hash",
			p: UpsertHumanAuthCredentialParams{
				Algorithm:  "pbkdf2-sha256",
				Iterations: 1,
				SaltHex:    "aa",
			},
			want: "hash_hex is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := s.UpsertHumanAuthCredential(ctx, tc.p); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected validation error %q, got %v", tc.want, err)
			}
		})
	}

	closed, err := Open(filepath.Join(t.TempDir(), "closed-auth.db"))
	if err != nil {
		t.Fatalf("open closed auth store: %v", err)
	}
	if err := closed.Initialize(ctx, InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize closed auth store: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close auth store: %v", err)
	}

	if _, _, err := closed.GetHumanAuthCredential(ctx); err == nil || !strings.Contains(err.Error(), "query human auth credential") {
		t.Fatalf("expected get credential closed-db error, got %v", err)
	}
	if _, _, err := closed.UpsertHumanAuthCredential(ctx, UpsertHumanAuthCredentialParams{
		Algorithm:  "pbkdf2-sha256",
		Iterations: 1,
		SaltHex:    "aa",
		HashHex:    "bb",
	}); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("expected upsert closed-db error, got %v", err)
	}
}
