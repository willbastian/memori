package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitializeAllowsPrefixChangeBeforeEventsAndRejectsAfterEvents(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-store-init-edge.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize mem prefix: %v", err)
	}
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "ops"}); err != nil {
		t.Fatalf("initialize ops prefix before events: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	prefix, err := s.projectIssueKeyPrefixTx(ctx, tx)
	if err != nil {
		t.Fatalf("project issue key prefix: %v", err)
	}
	_ = tx.Rollback()
	if prefix != "ops" {
		t.Fatalf("expected updated prefix %q, got %q", "ops", prefix)
	}

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "ops-a1b2c3d",
		Type:      "task",
		Title:     "Freeze prefix",
		Actor:     "agent-1",
		CommandID: "cmd-init-prefix-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "wrk"}); err == nil || !strings.Contains(err.Error(), "cannot change issue key prefix") {
		t.Fatalf("expected prefix change rejection after events, got %v", err)
	}
}

func TestInitializeRejectsInvalidIssueKeyPrefixes(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-store-init-invalid.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "1bad"}); err == nil || !strings.Contains(err.Error(), "invalid issue key prefix") {
		t.Fatalf("expected invalid prefix format error, got %v", err)
	}
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "task"}); err == nil || !strings.Contains(err.Error(), "issue key prefix") {
		t.Fatalf("expected embedded type prefix error, got %v", err)
	}
}
