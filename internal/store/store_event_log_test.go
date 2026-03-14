package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestCreateIssueIdempotencyByActorAndCommandID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	firstIssue, firstEvent, firstIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Type:      "task",
		Title:     "First title",
		Actor:     "agent-1",
		CommandID: "cmd-001",
	})
	if err != nil {
		t.Fatalf("first create issue: %v", err)
	}
	if firstIdempotent {
		t.Fatalf("first create should not be idempotent")
	}
	if firstEvent.EventOrder != 1 || firstEvent.EntitySeq != 1 {
		t.Fatalf("unexpected first event ordering: order=%d seq=%d", firstEvent.EventOrder, firstEvent.EntitySeq)
	}

	secondIssue, secondEvent, secondIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b1c2d3e",
		Type:      "task",
		Title:     "Second title should not apply",
		Actor:     "agent-1",
		CommandID: "cmd-001",
	})
	if err != nil {
		t.Fatalf("second create issue (idempotent retry): %v", err)
	}
	if !secondIdempotent {
		t.Fatalf("second create with same actor+command_id should be idempotent")
	}
	if secondEvent.EventID != firstEvent.EventID {
		t.Fatalf("expected same event id, got %s vs %s", secondEvent.EventID, firstEvent.EventID)
	}
	if secondIssue.ID != firstIssue.ID {
		t.Fatalf("expected same issue id on idempotent retry, got %s vs %s", secondIssue.ID, firstIssue.ID)
	}

	alphaEvents, err := s.ListEventsForEntity(ctx, "issue", "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("list events for mem-a1b2c3d: %v", err)
	}
	if len(alphaEvents) != 1 {
		t.Fatalf("expected 1 event for mem-a1b2c3d, got %d", len(alphaEvents))
	}

	betaEvents, err := s.ListEventsForEntity(ctx, "issue", "mem-b1c2d3e")
	if err != nil {
		t.Fatalf("list events for mem-b1c2d3e: %v", err)
	}
	if len(betaEvents) != 0 {
		t.Fatalf("expected 0 events for mem-b1c2d3e, got %d", len(betaEvents))
	}

	storedIssue, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get issue mem-a1b2c3d: %v", err)
	}
	if storedIssue.Title != "First title" {
		t.Fatalf("idempotent retry should not mutate title, got %q", storedIssue.Title)
	}
}

func TestMutatingCommandsRequireCommandID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID: "mem-a0a0a0a",
		Type:    "task",
		Title:   "Missing command id create",
		Actor:   "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for create, got: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a0a0a0b",
		Type:      "epic",
		Title:     "Parent",
		Actor:     "agent-1",
		CommandID: "cmd-required-1",
	})
	if err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a0a0a0c",
		Type:      "story",
		Title:     "Child",
		Actor:     "agent-1",
		CommandID: "cmd-required-2",
	})
	if err != nil {
		t.Fatalf("create child issue: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID: "mem-a0a0a0c",
		Status:  "inprogress",
		Actor:   "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for update, got: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-a0a0a0c",
		ParentIssueID: "mem-a0a0a0b",
		Actor:         "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for link, got: %v", err)
	}
}

func TestEventOrderingAndEntitySequenceContracts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1010101",
		Type:      "task",
		Title:     "A",
		Actor:     "agent-1",
		CommandID: "cmd-order-1",
	})
	if err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2020202",
		Type:      "task",
		Title:     "B",
		Actor:     "agent-1",
		CommandID: "cmd-order-2",
	})
	if err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1010101",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-order-3",
	})
	if err != nil {
		t.Fatalf("update issue A status inprogress: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2020202",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-order-4",
	})
	if err != nil {
		t.Fatalf("update issue B status inprogress: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1010101",
		Status:    "blocked",
		Actor:     "agent-1",
		CommandID: "cmd-order-5",
	})
	if err != nil {
		t.Fatalf("update issue A status blocked: %v", err)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT event_order FROM events ORDER BY event_order ASC`)
	if err != nil {
		t.Fatalf("query event ordering: %v", err)
	}
	defer rows.Close()

	var (
		prevOrder int64
		count     int64
	)
	for rows.Next() {
		var order int64
		if err := rows.Scan(&order); err != nil {
			t.Fatalf("scan event_order: %v", err)
		}
		if order != prevOrder+1 {
			t.Fatalf("expected contiguous event_order sequence, prev=%d got=%d", prevOrder, order)
		}
		prevOrder = order
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event ordering rows: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 events, got %d", count)
	}

	eventsA, err := s.ListEventsForEntity(ctx, "issue", "mem-1010101")
	if err != nil {
		t.Fatalf("list events for issue A: %v", err)
	}
	if len(eventsA) != 3 {
		t.Fatalf("expected 3 events for issue A, got %d", len(eventsA))
	}
	for i, event := range eventsA {
		if event.EntitySeq != int64(i+1) {
			t.Fatalf("issue A entity_seq mismatch at index %d: got %d", i, event.EntitySeq)
		}
	}

	eventsB, err := s.ListEventsForEntity(ctx, "issue", "mem-2020202")
	if err != nil {
		t.Fatalf("list events for issue B: %v", err)
	}
	if len(eventsB) != 2 {
		t.Fatalf("expected 2 events for issue B, got %d", len(eventsB))
	}
	for i, event := range eventsB {
		if event.EntitySeq != int64(i+1) {
			t.Fatalf("issue B entity_seq mismatch at index %d: got %d", i, event.EntitySeq)
		}
	}
}

func TestCommandIDScopedByActorAndMutationKind(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	first, _, firstIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1a1a1a",
		Type:      "task",
		Title:     "First issue",
		Actor:     "agent-1",
		CommandID: "cmd-shared",
	})
	if err != nil {
		t.Fatalf("create first issue: %v", err)
	}
	if firstIdempotent {
		t.Fatalf("first create should not be idempotent")
	}

	second, _, secondIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b2b2b2b",
		Type:      "task",
		Title:     "Second issue",
		Actor:     "agent-2",
		CommandID: "cmd-shared",
	})
	if err != nil {
		t.Fatalf("create with same command id for different actor should succeed: %v", err)
	}
	if secondIdempotent {
		t.Fatalf("different actor should not be treated as idempotent replay")
	}
	if second.ID == first.ID {
		t.Fatalf("expected different issue ids for separate actor command")
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   first.ID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-shared",
	})
	if err == nil || !strings.Contains(err.Error(), `command id already used by "issue.created"`) {
		t.Fatalf("expected mutation kind conflict for reused command id, got: %v", err)
	}
}

func TestEnumCheckConstraintsRejectInvalidDirectWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status, labels_json,
			current_cycle_no, active_gate_set_id, created_at, updated_at, last_event_id
		) VALUES(?, ?, ?, NULL, ?, '[]', 1, NULL, ?, ?, ?)
	`, "mem-c3c3c3c", "Feature", "invalid type", "Todo", nowUTC(), nowUTC(), "evt_test_work_item")
	if err == nil {
		t.Fatalf("expected invalid work item type to fail check constraint")
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_test_entity_type", 1, "unknown", "mem-d4d4d4d", 1, "issue.created", "{}", "agent-1", "cmd-enum-1", nowUTC(), "hash_test_1", 1)
	if err == nil {
		t.Fatalf("expected invalid events.entity_type to fail check constraint")
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_test_event_type", 2, "issue", "mem-e5e5e5e", 1, "issue.closed", "{}", "agent-1", "cmd-enum-2", nowUTC(), "hash_test_2", 1)
	if err == nil {
		t.Fatalf("expected invalid events.event_type to fail check constraint")
	}
}

func TestEventsTableAppendOnlyTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, event, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c3d4e5f",
		Type:      "task",
		Title:     "Trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-trigger",
	})
	if err != nil {
		t.Fatalf("create mem-c3d4e5f: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `UPDATE events SET event_type = 'issue.updated' WHERE event_id = ?`, event.EventID)
	if err == nil {
		t.Fatalf("expected update on events to fail due to append-only trigger")
	}
	if !strings.Contains(err.Error(), "events are append-only") {
		t.Fatalf("expected append-only trigger error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `DELETE FROM events WHERE event_id = ?`, event.EventID)
	if err == nil {
		t.Fatalf("expected delete on events to fail due to append-only trigger")
	}
	if !strings.Contains(err.Error(), "events are append-only") {
		t.Fatalf("expected append-only trigger error on delete, got: %v", err)
	}
}

func TestConcurrentCreateIssuePreservesEventOrdering(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const writers = 12
	start := make(chan struct{})
	errCh := make(chan error, writers)
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
				IssueID:   fmt.Sprintf("mem-a%06x", i),
				Type:      "task",
				Title:     fmt.Sprintf("Concurrent %d", i),
				Actor:     "agent-1",
				CommandID: fmt.Sprintf("cmd-concur-create-%03d", i),
			})
			errCh <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent create issue failed: %v", err)
		}
	}

	rows, err := s.db.QueryContext(ctx, `SELECT event_order FROM events ORDER BY event_order ASC`)
	if err != nil {
		t.Fatalf("query event ordering after concurrency: %v", err)
	}
	defer rows.Close()

	var (
		prevOrder int64
		count     int64
	)
	for rows.Next() {
		var order int64
		if err := rows.Scan(&order); err != nil {
			t.Fatalf("scan concurrent event_order: %v", err)
		}
		if order != prevOrder+1 {
			t.Fatalf("expected contiguous event_order under concurrency, prev=%d got=%d", prevOrder, order)
		}
		prevOrder = order
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate concurrent event_order rows: %v", err)
	}
	if count != writers {
		t.Fatalf("expected %d events, got %d", writers, count)
	}
}

func TestConcurrentDuplicateCreateIssueCommandIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const callers = 8
	start := make(chan struct{})
	errCh := make(chan error, callers)
	idempotentCh := make(chan bool, callers)
	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, idempotent, err := s.CreateIssue(ctx, CreateIssueParams{
				IssueID:   "mem-a1d0e0f",
				Type:      "task",
				Title:     "Concurrent idempotent create",
				Actor:     "agent-1",
				CommandID: "cmd-concur-idem-1",
			})
			errCh <- err
			idempotentCh <- idempotent
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	close(idempotentCh)

	idempotentCount := 0
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent idempotent create failed: %v", err)
		}
	}
	for idem := range idempotentCh {
		if idem {
			idempotentCount++
		}
	}
	if idempotentCount == 0 {
		t.Fatalf("expected at least one idempotent replay result under concurrency")
	}

	events, err := s.ListEventsForEntity(ctx, "issue", "mem-a1d0e0f")
	if err != nil {
		t.Fatalf("list events after concurrent idempotent create: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected a single persisted event, got %d", len(events))
	}
}
