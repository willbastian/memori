package store

import (
	"context"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
)

func TestCreateIssueKeyPolicyValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task-a1b2c3d",
		Type:      "task",
		Title:     "Invalid key prefix",
		Actor:     "agent-1",
		CommandID: "cmd-key-1",
	})
	if err == nil || !strings.Contains(err.Error(), "type must be in --type") {
		t.Fatalf("expected type-in-prefix validation error, got: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-nothexx",
		Type:      "task",
		Title:     "Invalid short sha",
		Actor:     "agent-1",
		CommandID: "cmd-key-2",
	})
	if err == nil || !strings.Contains(err.Error(), "shortSHA must be hex") {
		t.Fatalf("expected shortSHA validation error, got: %v", err)
	}
}

func TestCreateIssueGeneratedKeysFollowPrefixShortSHAPattern(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStoreWithPrefix(t, "ops")

	issue, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		Type:      "task",
		Title:     "Generated key",
		Actor:     "agent-1",
		CommandID: "cmd-key-3",
	})
	if err != nil {
		t.Fatalf("create with generated key: %v", err)
	}

	pattern := regexp.MustCompile(`^ops-[0-9a-f]{7}$`)
	if !pattern.MatchString(issue.ID) {
		t.Fatalf("expected generated key to match ops-shortSHA, got %q", issue.ID)
	}
}

func TestCreateIssuePersistsRichContextFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:            "mem-0f0f0f0",
		Type:               "task",
		Title:              "Rich context",
		Description:        "Implement richer ticket context rendering",
		AcceptanceCriteria: "Shows description and acceptance criteria in issue show",
		References:         []string{"https://example.com/spec", " https://example.com/spec ", "notes.md"},
		Actor:              "agent-1",
		CommandID:          "cmd-rich-create-1",
	})
	if err != nil {
		t.Fatalf("create issue with rich context: %v", err)
	}

	if created.Description != "Implement richer ticket context rendering" {
		t.Fatalf("unexpected description: %q", created.Description)
	}
	if created.Acceptance != "Shows description and acceptance criteria in issue show" {
		t.Fatalf("unexpected acceptance criteria: %q", created.Acceptance)
	}
	expectedRefs := []string{"https://example.com/spec", "notes.md"}
	if !reflect.DeepEqual(created.References, expectedRefs) {
		t.Fatalf("unexpected references: %#v", created.References)
	}
}

func TestUpdateIssueAllowsContextOnlyMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1f1f1f1",
		Type:      "task",
		Title:     "Context updates",
		Actor:     "agent-1",
		CommandID: "cmd-rich-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	title := "Context updates renamed"
	description := "Track rich context as first-class metadata"
	acceptance := "Issue show surfaces context fields"
	references := []string{"https://example.com/rfc", "adr-001.md"}
	updated, event, idempotent, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:            "mem-1f1f1f1",
		Title:              &title,
		Description:        &description,
		AcceptanceCriteria: &acceptance,
		References:         &references,
		Actor:              "agent-1",
		CommandID:          "cmd-rich-update-1",
	})
	if err != nil {
		t.Fatalf("update issue context: %v", err)
	}
	if idempotent {
		t.Fatalf("first context update should not be idempotent")
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}
	if updated.Status != "Todo" {
		t.Fatalf("status should remain Todo when only context changes, got %s", updated.Status)
	}
	if updated.Title != title {
		t.Fatalf("unexpected title: %q", updated.Title)
	}
	if updated.Description != description {
		t.Fatalf("unexpected description: %q", updated.Description)
	}
	if updated.Acceptance != acceptance {
		t.Fatalf("unexpected acceptance criteria: %q", updated.Acceptance)
	}
	if !reflect.DeepEqual(updated.References, references) {
		t.Fatalf("unexpected references: %#v", updated.References)
	}
}

func TestUpdateIssueRejectsBlankTitle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Type:      "task",
		Title:     "Valid title",
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	blank := "   "
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Title:     &blank,
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-update-1",
	}); err == nil || !strings.Contains(err.Error(), "--title is required") {
		t.Fatalf("expected blank title validation error, got: %v", err)
	}
}

func TestUpdateIssueStatusValidTransitionsAndIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1111111",
		Type:      "task",
		Title:     "Status transition test",
		Actor:     "agent-1",
		CommandID: "cmd-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if created.Status != "Todo" {
		t.Fatalf("expected initial status Todo, got %s", created.Status)
	}

	updated, event, idempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("update issue status: %v", err)
	}
	if idempotent {
		t.Fatalf("first update should not be idempotent")
	}
	if updated.Status != "InProgress" {
		t.Fatalf("expected status InProgress, got %s", updated.Status)
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}

	retryIssue, retryEvent, retryIdempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("retry update with same command id should succeed idempotently: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected idempotent retry")
	}
	if retryEvent.EventID != event.EventID {
		t.Fatalf("expected same event id on retry, got %s vs %s", retryEvent.EventID, event.EventID)
	}
	if retryIssue.Status != "InProgress" {
		t.Fatalf("expected status to remain InProgress on idempotent retry, got %s", retryIssue.Status)
	}
}

func TestInitializeMatchesMigratedSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	initStore := newTestStore(t)
	initSchema := sqliteSchemaObjectsForTest(t, initStore.DB())

	migratedPath := filepath.Join(t.TempDir(), "memori-migrated.db")
	migratedStore, err := Open(migratedPath)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() {
		_ = migratedStore.Close()
	})
	if _, err := dbschema.Migrate(ctx, migratedStore.DB(), nil); err != nil {
		t.Fatalf("migrate schema: %v", err)
	}
	migratedSchema := sqliteSchemaObjectsForTest(t, migratedStore.DB())

	if !reflect.DeepEqual(initSchema, migratedSchema) {
		t.Fatalf("expected Initialize schema to match migrated schema\ninit=%v\nmigrate=%v", initSchema, migratedSchema)
	}
}
