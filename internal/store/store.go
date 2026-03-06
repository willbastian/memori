package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const DBSchemaVersion = 1
const DefaultIssueKeyPrefix = "mem"

const (
	entityTypeIssue      = "issue"
	eventTypeIssueCreate = "issue.created"
	eventTypeIssueUpdate = "issue.updated"
	eventTypeIssueLink   = "issue.linked"
	eventTypeGateEval    = "gate.evaluated"
)

type Store struct {
	db *sql.DB
}

type Issue struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Title       string `json:"title"`
	ParentID    string `json:"parent_id,omitempty"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	LastEventID string `json:"last_event_id"`
}

type Event struct {
	EventID             string `json:"event_id"`
	EventOrder          int64  `json:"event_order"`
	EntityType          string `json:"entity_type"`
	EntityID            string `json:"entity_id"`
	EntitySeq           int64  `json:"entity_seq"`
	EventType           string `json:"event_type"`
	PayloadJSON         string `json:"payload_json"`
	Actor               string `json:"actor"`
	CommandID           string `json:"command_id"`
	CausationID         string `json:"causation_id,omitempty"`
	CorrelationID       string `json:"correlation_id,omitempty"`
	CreatedAt           string `json:"created_at"`
	Hash                string `json:"hash"`
	PrevHash            string `json:"prev_hash,omitempty"`
	EventPayloadVersion int    `json:"event_payload_version"`
}

type CreateIssueParams struct {
	IssueID   string
	Type      string
	Title     string
	ParentID  string
	Actor     string
	CommandID string
}

type UpdateIssueStatusParams struct {
	IssueID   string
	Status    string
	Actor     string
	CommandID string
}

type LinkIssueParams struct {
	ChildIssueID  string
	ParentIssueID string
	Actor         string
	CommandID     string
}

type ListIssuesParams struct {
	Type     string
	Status   string
	ParentID string
}

type InitializeParams struct {
	IssueKeyPrefix string
}

type ReplayResult struct {
	EventsApplied int `json:"events_applied"`
}

type appendEventRequest struct {
	EntityType          string
	EntityID            string
	EventType           string
	PayloadJSON         string
	Actor               string
	CommandID           string
	CausationID         string
	CorrelationID       string
	CreatedAt           string
	EventPayloadVersion int
}

type appendEventResult struct {
	Event         Event
	AlreadyExists bool
}

type issueCreatedPayload struct {
	IssueID   string `json:"issue_id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	ParentID  string `json:"parent_id,omitempty"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type issueUpdatedPayload struct {
	IssueID    string `json:"issue_id"`
	StatusFrom string `json:"status_from"`
	StatusTo   string `json:"status_to"`
	UpdatedAt  string `json:"updated_at"`
}

type issueLinkedPayload struct {
	IssueID      string `json:"issue_id"`
	ParentIDFrom string `json:"parent_id_from,omitempty"`
	ParentIDTo   string `json:"parent_id_to"`
	LinkedAt     string `json:"linked_at"`
}

func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	stmts := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("apply pragma %q: %w", stmt, err)
		}
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Initialize(ctx context.Context, p InitializeParams) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	issueKeyPrefix, err := normalizeIssueKeyPrefix(p.IssueKeyPrefix)
	if err != nil {
		return fmt.Errorf("invalid issue key prefix: %w", err)
	}
	if err := validateIssueTypeNotEmbeddedInKeyPrefix(issueKeyPrefix + "-a1b2c3d"); err != nil {
		return fmt.Errorf("invalid issue key prefix: %w", err)
	}

	schema := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS events (
			event_id TEXT PRIMARY KEY,
			event_order INTEGER NOT NULL CHECK(event_order > 0),
			entity_type TEXT NOT NULL CHECK(entity_type IN ('issue')),
			entity_id TEXT NOT NULL,
			entity_seq INTEGER NOT NULL CHECK(entity_seq > 0),
			event_type TEXT NOT NULL CHECK(event_type IN ('issue.created','issue.updated','issue.linked','gate.evaluated')),
			payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
			actor TEXT NOT NULL,
			command_id TEXT NOT NULL CHECK(length(command_id) > 0),
			causation_id TEXT,
			correlation_id TEXT,
			created_at TEXT NOT NULL,
			hash TEXT NOT NULL,
			prev_hash TEXT,
			event_payload_version INTEGER NOT NULL DEFAULT 1 CHECK(event_payload_version > 0),
			UNIQUE(event_order),
			UNIQUE(entity_type, entity_id, entity_seq),
			UNIQUE(hash),
			UNIQUE(actor, command_id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_events_entity_time ON events(entity_type, entity_id, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_events_correlation_time ON events(correlation_id, created_at);`,
		`CREATE TRIGGER IF NOT EXISTS events_no_update
			BEFORE UPDATE ON events
			BEGIN
				SELECT RAISE(ABORT, 'events are append-only');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS events_no_delete
			BEFORE DELETE ON events
			BEGIN
				SELECT RAISE(ABORT, 'events are append-only');
			END;`,
		`CREATE TABLE IF NOT EXISTS work_items (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL CHECK(type IN ('Epic','Story','Task','Bug')),
			title TEXT NOT NULL,
			parent_id TEXT,
			status TEXT NOT NULL CHECK(status IN ('Todo','InProgress','Blocked','Done')),
			priority TEXT,
			labels_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(labels_json)),
			current_cycle_no INTEGER NOT NULL DEFAULT 1,
			active_gate_set_id TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			last_event_id TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_work_items_type_status ON work_items(type, status);`,
		`CREATE INDEX IF NOT EXISTS idx_work_items_parent ON work_items(parent_id);`,
		`CREATE TABLE IF NOT EXISTS gate_templates (
			template_id TEXT NOT NULL,
			version INTEGER NOT NULL CHECK(version > 0),
			applies_to_json TEXT NOT NULL CHECK(json_valid(applies_to_json)),
			definition_json TEXT NOT NULL CHECK(json_valid(definition_json)),
			definition_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			created_by TEXT NOT NULL,
			PRIMARY KEY(template_id, version),
			UNIQUE(definition_hash)
		);`,
		`CREATE TRIGGER IF NOT EXISTS gate_templates_no_update
			BEFORE UPDATE ON gate_templates
			BEGIN
				SELECT RAISE(ABORT, 'gate_templates are immutable');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_templates_no_delete
			BEFORE DELETE ON gate_templates
			BEGIN
				SELECT RAISE(ABORT, 'gate_templates are immutable');
			END;`,
		`CREATE TABLE IF NOT EXISTS gate_sets (
			gate_set_id TEXT PRIMARY KEY,
			issue_id TEXT NOT NULL,
			cycle_no INTEGER NOT NULL CHECK(cycle_no > 0),
			template_refs_json TEXT NOT NULL CHECK(json_valid(template_refs_json)),
			frozen_definition_json TEXT NOT NULL CHECK(json_valid(frozen_definition_json)),
			gate_set_hash TEXT NOT NULL,
			locked_at TEXT,
			created_at TEXT NOT NULL,
			created_by TEXT NOT NULL,
			UNIQUE(issue_id, cycle_no),
			UNIQUE(issue_id, gate_set_hash),
			FOREIGN KEY(issue_id) REFERENCES work_items(id)
		);`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_no_delete
			BEFORE DELETE ON gate_sets
			BEGIN
				SELECT RAISE(ABORT, 'gate_sets are immutable');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_frozen_definition_no_update
			BEFORE UPDATE ON gate_sets
			WHEN NEW.template_refs_json IS NOT OLD.template_refs_json
				OR NEW.frozen_definition_json IS NOT OLD.frozen_definition_json
				OR NEW.gate_set_hash IS NOT OLD.gate_set_hash
			BEGIN
				SELECT RAISE(ABORT, 'gate_set definitions are immutable');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_lock_noop_rejected
			BEFORE UPDATE OF locked_at ON gate_sets
			WHEN OLD.locked_at IS NOT NULL
			BEGIN
				SELECT RAISE(ABORT, 'gate_set is already locked');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_locked_row_no_update
			BEFORE UPDATE ON gate_sets
			WHEN OLD.locked_at IS NOT NULL
				AND NEW.locked_at IS OLD.locked_at
			BEGIN
				SELECT RAISE(ABORT, 'locked gate_sets are immutable');
			END;`,
		`CREATE TABLE IF NOT EXISTS gate_set_items (
			gate_set_id TEXT NOT NULL,
			gate_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			required INTEGER NOT NULL CHECK(required IN (0,1)),
			criteria_json TEXT NOT NULL CHECK(json_valid(criteria_json)),
			PRIMARY KEY(gate_set_id, gate_id),
			FOREIGN KEY(gate_set_id) REFERENCES gate_sets(gate_set_id)
		);`,
		`CREATE TRIGGER IF NOT EXISTS gate_set_items_no_update
			BEFORE UPDATE ON gate_set_items
			BEGIN
				SELECT RAISE(ABORT, 'gate_set_items are immutable');
			END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_set_items_no_delete
			BEFORE DELETE ON gate_set_items
			BEGIN
				SELECT RAISE(ABORT, 'gate_set_items are immutable');
			END;`,
	}

	for _, stmt := range schema {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute schema statement: %w", err)
		}
	}

	now := nowUTC()
	meta := []struct {
		key   string
		value string
	}{
		{key: "db_schema_version", value: strconv.Itoa(DBSchemaVersion)},
		{key: "min_supported_db_schema_version", value: strconv.Itoa(DBSchemaVersion)},
		{key: "last_migrated_at_utc", value: now},
	}
	for _, item := range meta {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at)
			VALUES(?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
		`, item.key, item.value, now); err != nil {
			return fmt.Errorf("upsert schema_meta %s: %w", item.key, err)
		}
	}

	var existingPrefix sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&existingPrefix)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read issue_key_prefix: %w", err)
	}

	if !existingPrefix.Valid {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at) VALUES('issue_key_prefix', ?, ?)
		`, issueKeyPrefix, now); err != nil {
			return fmt.Errorf("insert issue_key_prefix: %w", err)
		}
	} else if existingPrefix.String != issueKeyPrefix {
		var eventCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM events`).Scan(&eventCount); err != nil {
			return fmt.Errorf("count events for issue_key_prefix update: %w", err)
		}
		if eventCount > 0 {
			return fmt.Errorf(
				"cannot change issue key prefix from %q to %q after events exist",
				existingPrefix.String,
				issueKeyPrefix,
			)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE schema_meta SET value = ?, updated_at = ? WHERE key = 'issue_key_prefix'
		`, issueKeyPrefix, now); err != nil {
			return fmt.Errorf("update issue_key_prefix: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'db_schema_version'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("query schema version: %w", err)
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse schema version %q: %w", raw, err)
	}
	return v, nil
}

func (s *Store) CreateIssue(ctx context.Context, p CreateIssueParams) (Issue, Event, bool, error) {
	if strings.TrimSpace(p.Title) == "" {
		return Issue{}, Event{}, false, errors.New("--title is required")
	}

	issueType, err := normalizeIssueType(p.Type)
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return Issue{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	projectPrefix, err := s.projectIssueKeyPrefixTx(ctx, tx)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if p.IssueID == "" {
		p.IssueID = newIssueKey(projectPrefix)
	} else {
		issueID, err := normalizeIssueKey(p.IssueID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		p.IssueID = issueID
		if err := validateIssueTypeNotEmbeddedInKeyPrefix(p.IssueID); err != nil {
			return Issue{}, Event{}, false, err
		}
		if err := validateIssueKeyPrefixMatchesProject(p.IssueID, projectPrefix); err != nil {
			return Issue{}, Event{}, false, err
		}
	}
	if err := validateIssueTypeNotEmbeddedInKeyPrefix(p.IssueID); err != nil {
		return Issue{}, Event{}, false, err
	}

	parentID := strings.TrimSpace(p.ParentID)
	if parentID != "" {
		normalizedParentID, err := normalizeIssueKey(parentID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		parentIssue, err := getIssueTx(ctx, tx, normalizedParentID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if err := validateIssueLinkForNewIssueTx(ctx, tx, p.IssueID, issueType, parentIssue); err != nil {
			return Issue{}, Event{}, false, err
		}
		parentID = normalizedParentID
	}

	payload := issueCreatedPayload{
		IssueID:   p.IssueID,
		Type:      issueType,
		Title:     strings.TrimSpace(p.Title),
		ParentID:  parentID,
		Status:    "Todo",
		CreatedAt: nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            p.IssueID,
		EventType:           eventTypeIssueCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeIssueCreate {
		return Issue{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if !appendRes.AlreadyExists && appendRes.Event.EntitySeq != 1 {
		return Issue{}, Event{}, false, fmt.Errorf("issue key %q already exists", appendRes.Event.EntityID)
	}

	if err := applyIssueCreatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Issue{}, Event{}, false, err
	}

	issue, err := getIssueTx(ctx, tx, appendRes.Event.EntityID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return issue, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) GetIssue(ctx context.Context, id string) (Issue, error) {
	issue, err := getIssueQueryable(ctx, s.db, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Issue{}, fmt.Errorf("issue %q not found", id)
		}
		return Issue{}, err
	}
	return issue, nil
}

func (s *Store) UpdateIssueStatus(ctx context.Context, p UpdateIssueStatusParams) (Issue, Event, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return Issue{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return Issue{}, Event{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeIssueUpdate {
			return Issue{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		issue, err := getIssueTx(ctx, tx, existingEvent.EntityID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Issue{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return issue, existingEvent, true, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	targetStatus, err := normalizeIssueStatus(p.Status)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	currentIssue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if err := validateIssueStatusTransition(currentIssue.Status, targetStatus); err != nil {
		return Issue{}, Event{}, false, err
	}
	if targetStatus == "Done" {
		if err := validateIssueCloseEligibilityTx(ctx, tx, issueID); err != nil {
			return Issue{}, Event{}, false, err
		}
	}

	payload := issueUpdatedPayload{
		IssueID:    issueID,
		StatusFrom: currentIssue.Status,
		StatusTo:   targetStatus,
		UpdatedAt:  nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeIssueUpdate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeIssueUpdate {
		return Issue{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyIssueUpdatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return Issue{}, Event{}, false, err
		}
	}

	issue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return issue, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) LinkIssue(ctx context.Context, p LinkIssueParams) (Issue, Event, bool, error) {
	childID, err := normalizeIssueKey(p.ChildIssueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	parentID, err := normalizeIssueKey(p.ParentIssueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return Issue{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return Issue{}, Event{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeIssueLink {
			return Issue{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		issue, err := getIssueTx(ctx, tx, existingEvent.EntityID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Issue{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return issue, existingEvent, true, nil
	}

	childIssue, err := getIssueTx(ctx, tx, childID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	parentIssue, err := getIssueTx(ctx, tx, parentID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if err := validateIssueLinkTx(ctx, tx, childIssue, parentIssue); err != nil {
		return Issue{}, Event{}, false, err
	}

	payload := issueLinkedPayload{
		IssueID:      childIssue.ID,
		ParentIDFrom: childIssue.ParentID,
		ParentIDTo:   parentIssue.ID,
		LinkedAt:     nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            childIssue.ID,
		EventType:           eventTypeIssueLink,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Issue{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeIssueLink {
		return Issue{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyIssueLinkedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return Issue{}, Event{}, false, err
		}
	}

	issue, err := getIssueTx(ctx, tx, childIssue.ID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Issue{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return issue, appendRes.Event, appendRes.AlreadyExists, nil
}

func findEventByActorCommandTx(ctx context.Context, tx *sql.Tx, actor, commandID string) (Event, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE actor = ? AND command_id = ?
	`, actor, commandID)
	event, err := scanEvent(row)
	if err == nil {
		return event, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	return Event{}, false, fmt.Errorf("check command idempotency: %w", err)
}

func (s *Store) ListIssues(ctx context.Context, p ListIssuesParams) ([]Issue, error) {
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 3)

	if strings.TrimSpace(p.Type) != "" {
		issueType, err := normalizeIssueType(p.Type)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "type = ?")
		args = append(args, issueType)
	}

	if strings.TrimSpace(p.Status) != "" {
		status, err := normalizeIssueStatus(p.Status)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}

	if strings.TrimSpace(p.ParentID) != "" {
		clauses = append(clauses, "parent_id = ?")
		args = append(args, strings.TrimSpace(p.ParentID))
	}

	query := `
		SELECT id, type, title, COALESCE(parent_id, ''), status, created_at, updated_at, last_event_id
		FROM work_items
	`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at ASC, id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query work_items backlog: %w", err)
	}
	defer rows.Close()

	issues := make([]Issue, 0)
	for rows.Next() {
		var issue Issue
		if err := rows.Scan(
			&issue.ID,
			&issue.Type,
			&issue.Title,
			&issue.ParentID,
			&issue.Status,
			&issue.CreatedAt,
			&issue.UpdatedAt,
			&issue.LastEventID,
		); err != nil {
			return nil, fmt.Errorf("scan work_item row: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items rows: %w", err)
	}

	return issues, nil
}

func (s *Store) ListEventsForEntity(ctx context.Context, entityType, entityID string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ? AND entity_id = ?
		ORDER BY event_order ASC
	`, entityType, entityID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func (s *Store) ReplayProjections(ctx context.Context) (ReplayResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM work_items`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear work_items: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		ORDER BY event_order ASC
	`)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("query events for replay: %w", err)
	}
	defer rows.Close()

	applied := 0
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return ReplayResult{}, err
		}
		if err := applyEventProjectionTx(ctx, tx, event); err != nil {
			return ReplayResult{}, err
		}
		applied++
	}
	if err := rows.Err(); err != nil {
		return ReplayResult{}, fmt.Errorf("iterate replay events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return ReplayResult{}, fmt.Errorf("commit replay tx: %w", err)
	}

	return ReplayResult{EventsApplied: applied}, nil
}

func applyEventProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	switch event.EventType {
	case eventTypeIssueCreate:
		return applyIssueCreatedProjectionTx(ctx, tx, event)
	case eventTypeIssueUpdate:
		return applyIssueUpdatedProjectionTx(ctx, tx, event)
	case eventTypeIssueLink:
		return applyIssueLinkedProjectionTx(ctx, tx, event)
	default:
		return nil
	}
}

func applyIssueCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueCreatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.created payload for event %s: %w", event.EventID, err)
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status,
			labels_json, current_cycle_no, active_gate_set_id,
			created_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, '[]', 1, NULL, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			type=excluded.type,
			title=excluded.title,
			parent_id=excluded.parent_id,
			status=excluded.status,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.Type,
		payload.Title,
		nullIfEmpty(payload.ParentID),
		payload.Status,
		payload.CreatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert work_item from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyIssueUpdatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueUpdatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
	}

	issueStatus, err := normalizeIssueStatus(payload.StatusTo)
	if err != nil {
		return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET status = ?, updated_at = ?, last_event_id = ?
		WHERE id = ?
	`, issueStatus, event.CreatedAt, event.EventID, payload.IssueID)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}

	return nil
}

func applyIssueLinkedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueLinkedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	linkedParentID, err := normalizeIssueKey(payload.ParentIDTo)
	if err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET parent_id = ?, updated_at = ?, last_event_id = ?
		WHERE id = ?
	`, linkedParentID, event.CreatedAt, event.EventID, payload.IssueID)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}

	return nil
}

func (s *Store) appendEventTx(ctx context.Context, tx *sql.Tx, req appendEventRequest) (appendEventResult, error) {
	if strings.TrimSpace(req.CommandID) == "" {
		return appendEventResult{}, errors.New("command_id is required")
	}
	if strings.TrimSpace(req.Actor) == "" {
		return appendEventResult{}, errors.New("actor is required")
	}
	if req.EventPayloadVersion <= 0 {
		req.EventPayloadVersion = 1
	}
	if req.CreatedAt == "" {
		req.CreatedAt = nowUTC()
	}

	row := tx.QueryRowContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE actor = ? AND command_id = ?
	`, req.Actor, req.CommandID)
	existing, err := scanEvent(row)
	if err == nil {
		return appendEventResult{Event: existing, AlreadyExists: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return appendEventResult{}, fmt.Errorf("check command idempotency: %w", err)
	}

	var lastOrder sql.NullInt64
	var prevHash sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT event_order, hash
		FROM events
		ORDER BY event_order DESC
		LIMIT 1
	`).Scan(&lastOrder, &prevHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return appendEventResult{}, fmt.Errorf("query last event: %w", err)
	}

	nextOrder := int64(1)
	if lastOrder.Valid {
		nextOrder = lastOrder.Int64 + 1
	}

	var maxSeq int64
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(entity_seq), 0)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, req.EntityType, req.EntityID).Scan(&maxSeq)
	if err != nil {
		return appendEventResult{}, fmt.Errorf("query entity sequence: %w", err)
	}
	nextSeq := maxSeq + 1

	eventID := newID("evt")
	prevHashValue := ""
	if prevHash.Valid {
		prevHashValue = prevHash.String
	}
	hash := computeEventHash(nextOrder, nextSeq, req, prevHashValue)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		eventID,
		nextOrder,
		req.EntityType,
		req.EntityID,
		nextSeq,
		req.EventType,
		req.PayloadJSON,
		req.Actor,
		req.CommandID,
		nullIfEmpty(req.CausationID),
		nullIfEmpty(req.CorrelationID),
		req.CreatedAt,
		hash,
		nullIfEmpty(prevHashValue),
		req.EventPayloadVersion,
	)
	if err != nil {
		return appendEventResult{}, fmt.Errorf("insert event: %w", err)
	}

	return appendEventResult{Event: Event{
		EventID:             eventID,
		EventOrder:          nextOrder,
		EntityType:          req.EntityType,
		EntityID:            req.EntityID,
		EntitySeq:           nextSeq,
		EventType:           req.EventType,
		PayloadJSON:         req.PayloadJSON,
		Actor:               req.Actor,
		CommandID:           req.CommandID,
		CausationID:         req.CausationID,
		CorrelationID:       req.CorrelationID,
		CreatedAt:           req.CreatedAt,
		Hash:                hash,
		PrevHash:            prevHashValue,
		EventPayloadVersion: req.EventPayloadVersion,
	}}, nil
}

func getIssueTx(ctx context.Context, tx *sql.Tx, id string) (Issue, error) {
	issue, err := getIssueQueryable(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Issue{}, fmt.Errorf("issue %q not found", id)
		}
		return Issue{}, err
	}
	return issue, nil
}

func getIssueQueryable(ctx context.Context, q queryable, id string) (Issue, error) {
	row := q.QueryRowContext(ctx, `
		SELECT id, type, title, COALESCE(parent_id, ''), status, created_at, updated_at, last_event_id
		FROM work_items
		WHERE id = ?
	`, id)
	var issue Issue
	if err := row.Scan(
		&issue.ID,
		&issue.Type,
		&issue.Title,
		&issue.ParentID,
		&issue.Status,
		&issue.CreatedAt,
		&issue.UpdatedAt,
		&issue.LastEventID,
	); err != nil {
		return Issue{}, err
	}
	return issue, nil
}

type queryable interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func scanEvent(scanner interface{ Scan(dest ...any) error }) (Event, error) {
	var event Event
	var causationID sql.NullString
	var correlationID sql.NullString
	var prevHash sql.NullString

	if err := scanner.Scan(
		&event.EventID,
		&event.EventOrder,
		&event.EntityType,
		&event.EntityID,
		&event.EntitySeq,
		&event.EventType,
		&event.PayloadJSON,
		&event.Actor,
		&event.CommandID,
		&causationID,
		&correlationID,
		&event.CreatedAt,
		&event.Hash,
		&prevHash,
		&event.EventPayloadVersion,
	); err != nil {
		return Event{}, err
	}

	if causationID.Valid {
		event.CausationID = causationID.String
	}
	if correlationID.Valid {
		event.CorrelationID = correlationID.String
	}
	if prevHash.Valid {
		event.PrevHash = prevHash.String
	}

	return event, nil
}

func computeEventHash(order, seq int64, req appendEventRequest, prevHash string) string {
	h := sha256.New()
	parts := []string{
		strconv.FormatInt(order, 10),
		req.EntityType,
		req.EntityID,
		strconv.FormatInt(seq, 10),
		req.EventType,
		req.PayloadJSON,
		req.Actor,
		req.CommandID,
		req.CausationID,
		req.CorrelationID,
		req.CreatedAt,
		prevHash,
		strconv.Itoa(req.EventPayloadVersion),
	}
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalizeIssueType(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "epic":
		return "Epic", nil
	case "story":
		return "Story", nil
	case "task":
		return "Task", nil
	case "bug":
		return "Bug", nil
	default:
		return "", fmt.Errorf("invalid --type %q (expected epic|story|task|bug)", raw)
	}
}

func normalizeIssueStatus(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "todo":
		return "Todo", nil
	case "inprogress":
		return "InProgress", nil
	case "blocked":
		return "Blocked", nil
	case "done":
		return "Done", nil
	default:
		return "", fmt.Errorf("invalid --status %q (expected todo|inprogress|blocked|done)", raw)
	}
}

func validateIssueStatusTransition(from, to string) error {
	fromStatus, err := normalizeIssueStatus(from)
	if err != nil {
		return fmt.Errorf("invalid current status %q: %w", from, err)
	}
	toStatus, err := normalizeIssueStatus(to)
	if err != nil {
		return err
	}
	if fromStatus == toStatus {
		return fmt.Errorf("issue is already in status %q", toStatus)
	}

	allowed := map[string]map[string]bool{
		"Todo":       {"InProgress": true, "Blocked": true},
		"InProgress": {"Blocked": true, "Done": true},
		"Blocked":    {"InProgress": true},
		"Done":       {},
	}
	if !allowed[fromStatus][toStatus] {
		return fmt.Errorf("invalid status transition %q -> %q", fromStatus, toStatus)
	}
	return nil
}

func validateIssueCloseEligibilityTx(ctx context.Context, tx *sql.Tx, issueID string) error {
	var gateSetID string
	err := tx.QueryRowContext(ctx, `
		SELECT gs.gate_set_id
		FROM gate_sets gs
		INNER JOIN work_items wi
			ON wi.id = gs.issue_id
			AND wi.current_cycle_no = gs.cycle_no
		WHERE gs.issue_id = ?
			AND gs.locked_at IS NOT NULL
		ORDER BY gs.cycle_no DESC
		LIMIT 1
	`, issueID).Scan(&gateSetID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("close validation query gate set for issue %q: %w", issueID, err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			r.gate_id,
			COALESCE((
				SELECT json_extract(e.payload_json, '$.result')
				FROM events e
				WHERE e.entity_type = ?
					AND e.entity_id = ?
					AND e.event_type = ?
					AND json_extract(e.payload_json, '$.gate_set_id') = ?
					AND json_extract(e.payload_json, '$.gate_id') = r.gate_id
				ORDER BY e.event_order DESC
				LIMIT 1
			), '')
		FROM gate_set_items r
		WHERE r.gate_set_id = ?
			AND r.required = 1
		ORDER BY r.gate_id ASC
	`, entityTypeIssue, issueID, eventTypeGateEval, gateSetID, gateSetID)
	if err != nil {
		return fmt.Errorf("close validation list required gates for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	failures := make([]string, 0)
	for rows.Next() {
		var gateID, result string
		if err := rows.Scan(&gateID, &result); err != nil {
			return fmt.Errorf("close validation scan required gate for issue %q: %w", issueID, err)
		}
		if strings.ToUpper(strings.TrimSpace(result)) != "PASS" {
			status := "MISSING"
			if strings.TrimSpace(result) != "" {
				status = strings.ToUpper(strings.TrimSpace(result))
			}
			failures = append(failures, gateID+"="+status)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("close validation iterate required gates for issue %q: %w", issueID, err)
	}

	if len(failures) > 0 {
		return fmt.Errorf(
			"close validation failed for issue %q (gate_set %q): required gates not PASS: %s",
			issueID,
			gateSetID,
			strings.Join(failures, ", "),
		)
	}
	return nil
}

func validateIssueLinkForNewIssueTx(
	ctx context.Context,
	tx *sql.Tx,
	childID, childType string,
	parentIssue Issue,
) error {
	if childID == parentIssue.ID {
		return fmt.Errorf("invalid issue link %q -> %q: issue cannot be its own parent", childID, parentIssue.ID)
	}
	if err := validateParentChildTypeConstraint(parentIssue.Type, childType); err != nil {
		return err
	}
	if createsCycle, err := wouldCreateIssueLinkCycleTx(ctx, tx, childID, parentIssue.ID); err != nil {
		return err
	} else if createsCycle {
		return fmt.Errorf("invalid issue link %q -> %q: cycle detected", childID, parentIssue.ID)
	}
	return nil
}

func validateIssueLinkTx(ctx context.Context, tx *sql.Tx, childIssue, parentIssue Issue) error {
	if childIssue.ID == parentIssue.ID {
		return fmt.Errorf("invalid issue link %q -> %q: issue cannot be its own parent", childIssue.ID, parentIssue.ID)
	}
	if childIssue.ParentID == parentIssue.ID {
		return fmt.Errorf("issue %q is already linked to parent %q", childIssue.ID, parentIssue.ID)
	}
	if err := validateParentChildTypeConstraint(parentIssue.Type, childIssue.Type); err != nil {
		return err
	}
	if createsCycle, err := wouldCreateIssueLinkCycleTx(ctx, tx, childIssue.ID, parentIssue.ID); err != nil {
		return err
	} else if createsCycle {
		return fmt.Errorf("invalid issue link %q -> %q: cycle detected", childIssue.ID, parentIssue.ID)
	}
	return nil
}

func validateParentChildTypeConstraint(parentType, childType string) error {
	switch parentType {
	case "Epic":
		if childType != "Story" {
			return fmt.Errorf("invalid issue link type: parent Epic requires child Story (got %s)", childType)
		}
	case "Story":
		if childType != "Task" && childType != "Bug" {
			return fmt.Errorf("invalid issue link type: parent Story requires child Task|Bug (got %s)", childType)
		}
	default:
		return fmt.Errorf("invalid issue link type: parent %s cannot have children", parentType)
	}
	return nil
}

func wouldCreateIssueLinkCycleTx(ctx context.Context, tx *sql.Tx, childID, proposedParentID string) (bool, error) {
	current := strings.TrimSpace(proposedParentID)
	for current != "" {
		if current == childID {
			return true, nil
		}
		parentID, err := parentIDForIssueTx(ctx, tx, current)
		if err != nil {
			return false, err
		}
		current = strings.TrimSpace(parentID)
	}
	return false, nil
}

func parentIDForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (string, error) {
	var parentID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT parent_id FROM work_items WHERE id = ?`, issueID).Scan(&parentID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("issue %q not found", issueID)
	}
	if err != nil {
		return "", fmt.Errorf("query issue parent for %q: %w", issueID, err)
	}
	if parentID.Valid {
		return parentID.String, nil
	}
	return "", nil
}

func normalizeIssueKey(raw string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(raw))
	parts := strings.Split(key, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", raw)
	}

	prefix, err := normalizeIssueKeyPrefix(parts[0])
	if err != nil {
		return "", err
	}

	shortSHA := parts[1]
	if len(shortSHA) < 7 || len(shortSHA) > 12 {
		return "", fmt.Errorf("invalid issue key %q (shortSHA must be 7-12 hex chars)", raw)
	}
	for _, r := range shortSHA {
		if !isHexRune(r) {
			return "", fmt.Errorf("invalid issue key %q (shortSHA must be hex)", raw)
		}
	}

	return prefix + "-" + shortSHA, nil
}

func normalizeIssueKeyPrefix(raw string) (string, error) {
	prefix := strings.ToLower(strings.TrimSpace(raw))
	if prefix == "" {
		prefix = DefaultIssueKeyPrefix
	}
	if len(prefix) < 2 || len(prefix) > 16 {
		return "", fmt.Errorf("invalid issue key prefix %q (must be 2-16 lowercase letters/digits)", raw)
	}
	for i, r := range prefix {
		if i == 0 && (r < 'a' || r > 'z') {
			return "", fmt.Errorf("invalid issue key prefix %q (must start with a letter)", raw)
		}
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return "", fmt.Errorf("invalid issue key prefix %q (must use lowercase letters/digits)", raw)
		}
	}
	return prefix, nil
}

func validateIssueTypeNotEmbeddedInKeyPrefix(issueKey string) error {
	parts := strings.Split(issueKey, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", issueKey)
	}
	switch parts[0] {
	case "epic", "story", "task", "bug":
		return fmt.Errorf("invalid issue key %q (type must be in --type, not key prefix)", issueKey)
	default:
		return nil
	}
}

func validateIssueKeyPrefixMatchesProject(issueKey, projectPrefix string) error {
	parts := strings.Split(issueKey, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", issueKey)
	}
	if parts[0] != projectPrefix {
		return fmt.Errorf(
			"invalid issue key %q (prefix must match project prefix %q)",
			issueKey,
			projectPrefix,
		)
	}
	return nil
}

func isHexRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func defaultActor() string {
	if current, err := user.Current(); err == nil {
		if current.Username != "" {
			return current.Username
		}
	}
	if fromEnv := os.Getenv("USER"); fromEnv != "" {
		return fromEnv
	}
	return "local"
}

func newID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(buf))
}

func newIssueKey(prefix string) string {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		now := strconv.FormatInt(time.Now().UnixNano(), 10)
		sum := sha256.Sum256([]byte(prefix + ":" + now))
		return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(sum[:])[:7])
	}
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	input := append(random, []byte(now)...)
	sum := sha256.Sum256(input)
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(sum[:])[:7])
}

func (s *Store) projectIssueKeyPrefixTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var prefix string
	err := tx.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&prefix)
	if errors.Is(err, sql.ErrNoRows) {
		prefix = DefaultIssueKeyPrefix
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at) VALUES('issue_key_prefix', ?, ?)
		`, prefix, nowUTC()); err != nil {
			return "", fmt.Errorf("insert missing issue_key_prefix: %w", err)
		}
		return prefix, nil
	}
	if err != nil {
		return "", fmt.Errorf("read issue_key_prefix: %w", err)
	}
	normalized, err := normalizeIssueKeyPrefix(prefix)
	if err != nil {
		return "", fmt.Errorf("invalid stored issue_key_prefix %q: %w", prefix, err)
	}
	return normalized, nil
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
