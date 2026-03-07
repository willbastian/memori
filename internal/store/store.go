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
	"sort"
	"strconv"
	"strings"
	"time"

	"memori/internal/dbschema"

	_ "modernc.org/sqlite"
)

const DefaultIssueKeyPrefix = "mem"

const (
	entityTypeIssue   = "issue"
	entityTypeSession = "session"
	entityTypePacket  = "packet"

	eventTypeIssueCreate       = "issue.created"
	eventTypeIssueUpdate       = "issue.updated"
	eventTypeIssueLink         = "issue.linked"
	eventTypeGateEval          = "gate.evaluated"
	eventTypeSessionCheckpoint = "session.checkpointed"
	eventTypePacketBuilt       = "packet.built"
)

type Store struct {
	db *sql.DB
}

type Issue struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Title       string   `json:"title"`
	ParentID    string   `json:"parent_id,omitempty"`
	Status      string   `json:"status"`
	Priority    string   `json:"priority,omitempty"`
	Labels      []string `json:"labels,omitempty"`
	Description string   `json:"description,omitempty"`
	Acceptance  string   `json:"acceptance_criteria,omitempty"`
	References  []string `json:"references,omitempty"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
	LastEventID string   `json:"last_event_id"`
}

type IssueNextCandidate struct {
	Issue   Issue    `json:"issue"`
	Score   int      `json:"score"`
	Reasons []string `json:"reasons"`
}

type IssueNextResult struct {
	Agent      string               `json:"agent,omitempty"`
	Candidate  IssueNextCandidate   `json:"candidate"`
	Candidates []IssueNextCandidate `json:"candidates"`
	Considered int                  `json:"considered"`
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
	IssueID            string
	Type               string
	Title              string
	ParentID           string
	Description        string
	AcceptanceCriteria string
	References         []string
	Actor              string
	CommandID          string
}

type UpdateIssueStatusParams struct {
	IssueID   string
	Status    string
	Actor     string
	CommandID string
}

type UpdateIssueParams struct {
	IssueID            string
	Status             *string
	Priority           *string
	Labels             *[]string
	Description        *string
	AcceptanceCriteria *string
	References         *[]string
	Actor              string
	CommandID          string
}

type LinkIssueParams struct {
	ChildIssueID  string
	ParentIssueID string
	Actor         string
	CommandID     string
}

type EvaluateGateParams struct {
	IssueID      string
	GateID       string
	Result       string
	EvidenceRefs []string
	Proof        *GateEvaluationProof
	Actor        string
	CommandID    string
}

type GetGateStatusParams struct {
	IssueID string
	CycleNo *int
}

type CheckpointSessionParams struct {
	SessionID string
	Trigger   string
	Actor     string
	CommandID string
}

type RehydrateSessionParams struct {
	SessionID string
}

type BuildPacketParams struct {
	Scope     string
	ScopeID   string
	Actor     string
	CommandID string
}

type GetPacketParams struct {
	PacketID string
}

type UsePacketParams struct {
	AgentID  string
	PacketID string
}

type ListOpenLoopsParams struct {
	IssueID string
	CycleNo *int
}

type CreateGateTemplateParams struct {
	TemplateID     string
	Version        int
	AppliesTo      []string
	DefinitionJSON string
	Actor          string
}

type ListGateTemplatesParams struct {
	IssueType string
}

type InstantiateGateSetParams struct {
	IssueID      string
	TemplateRefs []string
	Actor        string
}

type LockGateSetParams struct {
	IssueID string
	CycleNo *int
	Actor   string
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
	IssueID            string   `json:"issue_id"`
	Type               string   `json:"type"`
	Title              string   `json:"title"`
	ParentID           string   `json:"parent_id,omitempty"`
	Status             string   `json:"status"`
	Description        string   `json:"description,omitempty"`
	AcceptanceCriteria string   `json:"acceptance_criteria,omitempty"`
	References         []string `json:"references,omitempty"`
	CreatedAt          string   `json:"created_at"`
}

type issueUpdatedPayload struct {
	IssueID                string                   `json:"issue_id"`
	StatusFrom             *string                  `json:"status_from,omitempty"`
	StatusTo               *string                  `json:"status_to,omitempty"`
	PriorityFrom           *string                  `json:"priority_from,omitempty"`
	PriorityTo             *string                  `json:"priority_to,omitempty"`
	LabelsFrom             *[]string                `json:"labels_from,omitempty"`
	LabelsTo               *[]string                `json:"labels_to,omitempty"`
	DescriptionFrom        *string                  `json:"description_from,omitempty"`
	DescriptionTo          *string                  `json:"description_to,omitempty"`
	AcceptanceCriteriaFrom *string                  `json:"acceptance_criteria_from,omitempty"`
	AcceptanceCriteriaTo   *string                  `json:"acceptance_criteria_to,omitempty"`
	ReferencesFrom         *[]string                `json:"references_from,omitempty"`
	ReferencesTo           *[]string                `json:"references_to,omitempty"`
	CloseProof             *IssueCloseAuthorization `json:"close_proof,omitempty"`
	UpdatedAt              string                   `json:"updated_at"`
}

type issueLinkedPayload struct {
	IssueID      string `json:"issue_id"`
	ParentIDFrom string `json:"parent_id_from,omitempty"`
	ParentIDTo   string `json:"parent_id_to"`
	LinkedAt     string `json:"linked_at"`
}

type sessionCheckpointedPayload struct {
	SessionID           string         `json:"session_id"`
	Trigger             string         `json:"trigger"`
	StartedAt           string         `json:"started_at"`
	Checkpoint          map[string]any `json:"checkpoint"`
	CheckpointedAt      string         `json:"checkpointed_at"`
	ContextChunkID      string         `json:"context_chunk_id"`
	ContextChunkKind    string         `json:"context_chunk_kind"`
	ContextChunkContent string         `json:"context_chunk_content"`
	ContextChunkMeta    map[string]any `json:"context_chunk_metadata"`
	CreatedBy           string         `json:"created_by"`
}

type packetBuiltPayload struct {
	PacketID            string         `json:"packet_id"`
	Scope               string         `json:"scope"`
	Packet              map[string]any `json:"packet"`
	PacketSchemaVersion int            `json:"packet_schema_version"`
	BuiltFromEventID    string         `json:"built_from_event_id,omitempty"`
	CreatedAt           string         `json:"created_at"`
	IssueID             string         `json:"issue_id,omitempty"`
	IssueCycleNo        int            `json:"issue_cycle_no,omitempty"`
}

type gateEvaluatedPayload struct {
	IssueID      string               `json:"issue_id"`
	GateSetID    string               `json:"gate_set_id"`
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
	EvaluatedAt  string               `json:"evaluated_at"`
}

type GateEvaluationProof struct {
	Verifier      string `json:"verifier"`
	Runner        string `json:"runner"`
	RunnerVersion string `json:"runner_version"`
	ExitCode      int    `json:"exit_code"`
	StartedAt     string `json:"started_at,omitempty"`
	FinishedAt    string `json:"finished_at,omitempty"`
	GateSetHash   string `json:"gate_set_hash,omitempty"`
}

type GateEvaluation struct {
	IssueID      string               `json:"issue_id"`
	GateSetID    string               `json:"gate_set_id"`
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
	EvaluatedAt  string               `json:"evaluated_at"`
}

type GateVerificationSpec struct {
	IssueID     string `json:"issue_id"`
	GateSetID   string `json:"gate_set_id"`
	GateSetHash string `json:"gate_set_hash"`
	GateID      string `json:"gate_id"`
	Command     string `json:"command"`
}

type IssueCloseGateProof struct {
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
}

type IssueCloseAuthorization struct {
	GateSetID   string                `json:"gate_set_id"`
	GateSetHash string                `json:"gate_set_hash"`
	Gates       []IssueCloseGateProof `json:"gates"`
}

type GateStatus struct {
	IssueID   string           `json:"issue_id"`
	GateSetID string           `json:"gate_set_id"`
	CycleNo   int              `json:"cycle_no"`
	LockedAt  string           `json:"locked_at,omitempty"`
	Gates     []GateStatusItem `json:"gates"`
}

type GateStatusItem struct {
	GateID       string   `json:"gate_id"`
	Kind         string   `json:"kind"`
	Required     bool     `json:"required"`
	Result       string   `json:"result"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
	EvaluatedAt  string   `json:"evaluated_at,omitempty"`
	LastEventID  string   `json:"last_event_id,omitempty"`
}

type Session struct {
	SessionID      string         `json:"session_id"`
	Trigger        string         `json:"trigger"`
	StartedAt      string         `json:"started_at"`
	EndedAt        string         `json:"ended_at,omitempty"`
	SummaryEventID string         `json:"summary_event_id,omitempty"`
	Checkpoint     map[string]any `json:"checkpoint,omitempty"`
	CreatedBy      string         `json:"created_by"`
}

type RehydratePacket struct {
	PacketID            string         `json:"packet_id"`
	Scope               string         `json:"scope"`
	Packet              map[string]any `json:"packet"`
	PacketSchemaVersion int            `json:"packet_schema_version"`
	BuiltFromEventID    string         `json:"built_from_event_id,omitempty"`
	CreatedAt           string         `json:"created_at"`
}

type AgentFocus struct {
	AgentID       string `json:"agent_id"`
	ActiveIssueID string `json:"active_issue_id,omitempty"`
	ActiveCycleNo int    `json:"active_cycle_no,omitempty"`
	LastPacketID  string `json:"last_packet_id"`
	UpdatedAt     string `json:"updated_at"`
}

type SessionRehydrateResult struct {
	SessionID string          `json:"session_id"`
	Source    string          `json:"source"`
	Packet    RehydratePacket `json:"packet"`
}

type OpenLoop struct {
	LoopID        string `json:"loop_id"`
	IssueID       string `json:"issue_id"`
	CycleNo       int    `json:"cycle_no"`
	LoopType      string `json:"loop_type"`
	Status        string `json:"status"`
	Owner         string `json:"owner,omitempty"`
	Priority      string `json:"priority,omitempty"`
	SourceEventID string `json:"source_event_id,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}

type GateTemplate struct {
	TemplateID     string   `json:"template_id"`
	Version        int      `json:"version"`
	AppliesTo      []string `json:"applies_to"`
	DefinitionJSON string   `json:"definition_json"`
	DefinitionHash string   `json:"definition_hash"`
	CreatedAt      string   `json:"created_at"`
	CreatedBy      string   `json:"created_by"`
}

type GateSet struct {
	GateSetID        string              `json:"gate_set_id"`
	IssueID          string              `json:"issue_id"`
	CycleNo          int                 `json:"cycle_no"`
	TemplateRefs     []string            `json:"template_refs"`
	FrozenDefinition map[string]any      `json:"frozen_definition,omitempty"`
	GateSetHash      string              `json:"gate_set_hash"`
	LockedAt         string              `json:"locked_at,omitempty"`
	CreatedAt        string              `json:"created_at"`
	CreatedBy        string              `json:"created_by"`
	Items            []GateSetDefinition `json:"items,omitempty"`
}

type GateSetDefinition struct {
	GateID   string `json:"gate_id"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	Criteria any    `json:"criteria,omitempty"`
}

type HumanAuthCredential struct {
	CredentialID string `json:"credential_id"`
	Algorithm    string `json:"algorithm"`
	Iterations   int    `json:"iterations"`
	SaltHex      string `json:"salt_hex"`
	HashHex      string `json:"hash_hex"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	RotatedBy    string `json:"rotated_by"`
}

type UpsertHumanAuthCredentialParams struct {
	Algorithm  string
	Iterations int
	SaltHex    string
	HashHex    string
	Actor      string
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

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Initialize(ctx context.Context, p InitializeParams) error {
	issueKeyPrefix, err := normalizeIssueKeyPrefix(p.IssueKeyPrefix)
	if err != nil {
		return fmt.Errorf("invalid issue key prefix: %w", err)
	}
	if err := validateIssueTypeNotEmbeddedInKeyPrefix(issueKeyPrefix + "-a1b2c3d"); err != nil {
		return fmt.Errorf("invalid issue key prefix: %w", err)
	}
	migrationStatus, err := dbschema.Migrate(ctx, s.db, nil)
	if err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	schema := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS events (
			event_id TEXT PRIMARY KEY,
			event_order INTEGER NOT NULL CHECK(event_order > 0),
			entity_type TEXT NOT NULL CHECK(entity_type IN ('issue','session','packet')),
			entity_id TEXT NOT NULL,
			entity_seq INTEGER NOT NULL CHECK(entity_seq > 0),
			event_type TEXT NOT NULL CHECK(event_type IN ('issue.created','issue.updated','issue.linked','gate.evaluated','session.checkpointed','packet.built')),
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
			description TEXT NOT NULL DEFAULT '',
			acceptance_criteria TEXT NOT NULL DEFAULT '',
			references_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(references_json)),
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
		`CREATE TABLE IF NOT EXISTS human_auth_credentials (
			credential_id TEXT PRIMARY KEY CHECK(credential_id = 'default'),
			algorithm TEXT NOT NULL CHECK(algorithm = 'pbkdf2-sha256'),
			iterations INTEGER NOT NULL CHECK(iterations >= 310000),
			salt_hex TEXT NOT NULL CHECK(length(salt_hex) >= 32),
			hash_hex TEXT NOT NULL CHECK(length(hash_hex) = 64),
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			rotated_by TEXT NOT NULL
		);`,
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
		`CREATE TABLE IF NOT EXISTS gate_status_projection (
			issue_id TEXT NOT NULL,
			gate_set_id TEXT NOT NULL,
			gate_id TEXT NOT NULL,
			result TEXT NOT NULL CHECK(result IN ('PASS','FAIL','BLOCKED')),
			evidence_refs_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(evidence_refs_json)),
			evaluated_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			last_event_id TEXT NOT NULL,
			PRIMARY KEY(issue_id, gate_set_id, gate_id),
			FOREIGN KEY(issue_id) REFERENCES work_items(id),
			FOREIGN KEY(gate_set_id, gate_id) REFERENCES gate_set_items(gate_set_id, gate_id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_gate_status_projection_issue ON gate_status_projection(issue_id, gate_set_id);`,
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
		{key: "db_schema_version", value: strconv.Itoa(migrationStatus.CurrentVersion)},
		{key: "min_supported_db_schema_version", value: strconv.Itoa(dbschema.MinSupportedVersion)},
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

func (s *Store) GetHumanAuthCredential(ctx context.Context) (HumanAuthCredential, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT credential_id, algorithm, iterations, salt_hex, hash_hex, created_at, updated_at, rotated_by
		FROM human_auth_credentials
		WHERE credential_id = 'default'
	`)

	var credential HumanAuthCredential
	if err := row.Scan(
		&credential.CredentialID,
		&credential.Algorithm,
		&credential.Iterations,
		&credential.SaltHex,
		&credential.HashHex,
		&credential.CreatedAt,
		&credential.UpdatedAt,
		&credential.RotatedBy,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanAuthCredential{}, false, nil
		}
		return HumanAuthCredential{}, false, fmt.Errorf("query human auth credential: %w", err)
	}
	return credential, true, nil
}

func (s *Store) UpsertHumanAuthCredential(ctx context.Context, p UpsertHumanAuthCredentialParams) (HumanAuthCredential, bool, error) {
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	algorithm := strings.TrimSpace(p.Algorithm)
	if algorithm == "" {
		return HumanAuthCredential{}, false, errors.New("algorithm is required")
	}
	if p.Iterations <= 0 {
		return HumanAuthCredential{}, false, errors.New("iterations must be > 0")
	}
	if strings.TrimSpace(p.SaltHex) == "" {
		return HumanAuthCredential{}, false, errors.New("salt_hex is required")
	}
	if strings.TrimSpace(p.HashHex) == "" {
		return HumanAuthCredential{}, false, errors.New("hash_hex is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HumanAuthCredential{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var existingCreatedAt string
	err = tx.QueryRowContext(ctx, `
		SELECT created_at
		FROM human_auth_credentials
		WHERE credential_id = 'default'
	`).Scan(&existingCreatedAt)
	rotated := true
	if errors.Is(err, sql.ErrNoRows) {
		rotated = false
		existingCreatedAt = nowUTC()
	} else if err != nil {
		return HumanAuthCredential{}, false, fmt.Errorf("query existing human auth credential: %w", err)
	}

	updatedAt := nowUTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO human_auth_credentials(
			credential_id, algorithm, iterations, salt_hex, hash_hex, created_at, updated_at, rotated_by
		) VALUES('default', ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(credential_id) DO UPDATE SET
			algorithm=excluded.algorithm,
			iterations=excluded.iterations,
			salt_hex=excluded.salt_hex,
			hash_hex=excluded.hash_hex,
			updated_at=excluded.updated_at,
			rotated_by=excluded.rotated_by
	`, algorithm, p.Iterations, strings.TrimSpace(p.SaltHex), strings.TrimSpace(p.HashHex), existingCreatedAt, updatedAt, actor); err != nil {
		return HumanAuthCredential{}, false, fmt.Errorf("upsert human auth credential: %w", err)
	}

	credential, found, err := getHumanAuthCredentialTx(ctx, tx)
	if err != nil {
		return HumanAuthCredential{}, false, err
	}
	if !found {
		return HumanAuthCredential{}, false, errors.New("human auth credential write did not persist")
	}

	if err := tx.Commit(); err != nil {
		return HumanAuthCredential{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return credential, rotated, nil
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
		IssueID:            p.IssueID,
		Type:               issueType,
		Title:              strings.TrimSpace(p.Title),
		ParentID:           parentID,
		Status:             "Todo",
		Description:        strings.TrimSpace(p.Description),
		AcceptanceCriteria: strings.TrimSpace(p.AcceptanceCriteria),
		References:         normalizeReferences(p.References),
		CreatedAt:          nowUTC(),
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
	status := p.Status
	return s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   p.IssueID,
		Status:    &status,
		Actor:     p.Actor,
		CommandID: p.CommandID,
	})
}

func (s *Store) UpdateIssue(ctx context.Context, p UpdateIssueParams) (Issue, Event, bool, error) {
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

	currentIssue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return Issue{}, Event{}, false, err
	}

	payload := issueUpdatedPayload{
		IssueID:   issueID,
		UpdatedAt: nowUTC(),
	}
	changed := false
	targetStatus := ""
	var closeProof *IssueCloseAuthorization

	if p.Status != nil {
		statusTo, err := normalizeIssueStatus(*p.Status)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if err := validateIssueStatusTransition(currentIssue.Status, statusTo); err != nil {
			return Issue{}, Event{}, false, err
		}
		statusFrom := currentIssue.Status
		payload.StatusFrom = &statusFrom
		payload.StatusTo = &statusTo
		targetStatus = statusTo
		changed = true
	}
	if p.Priority != nil {
		priorityTo, err := normalizePriority(*p.Priority)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if currentIssue.Priority != priorityTo {
			priorityFrom := currentIssue.Priority
			payload.PriorityFrom = &priorityFrom
			payload.PriorityTo = &priorityTo
			changed = true
		}
	}
	if p.Labels != nil {
		labelsTo := normalizeLabels(*p.Labels)
		if !equalStringSlices(currentIssue.Labels, labelsTo) {
			labelsFrom := copyStringSlice(currentIssue.Labels)
			labelsToCopy := copyStringSlice(labelsTo)
			payload.LabelsFrom = &labelsFrom
			payload.LabelsTo = &labelsToCopy
			changed = true
		}
	}
	if p.Description != nil {
		descriptionTo := strings.TrimSpace(*p.Description)
		if currentIssue.Description != descriptionTo {
			descriptionFrom := currentIssue.Description
			payload.DescriptionFrom = &descriptionFrom
			payload.DescriptionTo = &descriptionTo
			changed = true
		}
	}
	if p.AcceptanceCriteria != nil {
		acceptanceTo := strings.TrimSpace(*p.AcceptanceCriteria)
		if currentIssue.Acceptance != acceptanceTo {
			acceptanceFrom := currentIssue.Acceptance
			payload.AcceptanceCriteriaFrom = &acceptanceFrom
			payload.AcceptanceCriteriaTo = &acceptanceTo
			changed = true
		}
	}
	if p.References != nil {
		referencesTo := normalizeReferences(*p.References)
		if !equalStringSlices(currentIssue.References, referencesTo) {
			referencesFrom := copyStringSlice(currentIssue.References)
			referencesToCopy := copyStringSlice(referencesTo)
			payload.ReferencesFrom = &referencesFrom
			payload.ReferencesTo = &referencesToCopy
			changed = true
		}
	}

	if !changed {
		return Issue{}, Event{}, false, errors.New("--status, --priority, --label, --description, --acceptance-criteria, or --reference is required")
	}

	if targetStatus == "Done" {
		closeProofValue, err := validateIssueCloseEligibilityTx(ctx, tx, issueID)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		closeProof = closeProofValue
		payload.CloseProof = closeProof
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

func (s *Store) EvaluateGate(ctx context.Context, p EvaluateGateParams) (GateEvaluation, Event, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--command-id is required")
	}
	if len(normalizeReferences(p.EvidenceRefs)) == 0 {
		return GateEvaluation{}, Event{}, false, errors.New("--evidence is required")
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	gateID := strings.TrimSpace(p.GateID)
	if gateID == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--gate is required")
	}
	result, err := normalizeGateResult(p.Result)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	evidenceRefs := normalizeReferences(p.EvidenceRefs)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateEvaluation{}, Event{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateEval {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateEvaluatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateEvaluation{}, Event{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return GateEvaluation{
			IssueID:      payload.IssueID,
			GateSetID:    payload.GateSetID,
			GateID:       payload.GateID,
			Result:       payload.Result,
			EvidenceRefs: payload.EvidenceRefs,
			Proof:        payload.Proof,
			EvaluatedAt:  payload.EvaluatedAt,
		}, existingEvent, true, nil
	}

	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return GateEvaluation{}, Event{}, false, err
	}

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if !found {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("no locked gate set found for issue %q", issueID)
	}
	proof := normalizeGateEvaluationProof(p.Proof)
	if proof != nil && strings.TrimSpace(proof.GateSetHash) == "" {
		proof.GateSetHash = gateSet.GateSetHash
	}

	var gateCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, gateSet.GateSetID, gateID).Scan(&gateCount); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("lookup gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
	}
	if gateCount == 0 {
		return GateEvaluation{}, Event{}, false, fmt.Errorf(
			"gate %q is not defined in locked gate_set %q for issue %q",
			gateID,
			gateSet.GateSetID,
			issueID,
		)
	}

	payload := gateEvaluatedPayload{
		IssueID:      issueID,
		GateSetID:    gateSet.GateSetID,
		GateID:       gateID,
		Result:       result,
		EvidenceRefs: evidenceRefs,
		Proof:        proof,
		EvaluatedAt:  nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateEval {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateEvaluatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateEvaluation{}, Event{}, false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateEvaluation{
		IssueID:      payload.IssueID,
		GateSetID:    payload.GateSetID,
		GateID:       payload.GateID,
		Result:       payload.Result,
		EvidenceRefs: payload.EvidenceRefs,
		Proof:        payload.Proof,
		EvaluatedAt:  payload.EvaluatedAt,
	}, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) LookupGateEvaluationByCommand(ctx context.Context, actor, commandID string) (GateEvaluation, Event, bool, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--actor is required")
	}
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	existingEvent, found, err := findEventByActorCommandTx(ctx, tx, actor, commandID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return GateEvaluation{}, Event{}, false, nil
	}
	if existingEvent.EventType != eventTypeGateEval {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
	}

	payload, err := decodeGateEvaluatedPayload(existingEvent.PayloadJSON)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateEvaluation{
		IssueID:      payload.IssueID,
		GateSetID:    payload.GateSetID,
		GateID:       payload.GateID,
		Result:       payload.Result,
		EvidenceRefs: payload.EvidenceRefs,
		Proof:        payload.Proof,
		EvaluatedAt:  payload.EvaluatedAt,
	}, existingEvent, true, nil
}

func (s *Store) LookupGateVerificationSpec(ctx context.Context, issueID, gateID string) (GateVerificationSpec, error) {
	normalizedIssueID, err := normalizeIssueKey(issueID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	normalizedGateID := strings.TrimSpace(gateID)
	if normalizedGateID == "" {
		return GateVerificationSpec{}, errors.New("--gate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateVerificationSpec{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, normalizedIssueID); err != nil {
		return GateVerificationSpec{}, err
	}
	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, normalizedIssueID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	if !found {
		return GateVerificationSpec{}, fmt.Errorf("no locked gate set found for issue %q", normalizedIssueID)
	}

	var criteriaJSON string
	if err := tx.QueryRowContext(ctx, `
		SELECT criteria_json
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, gateSet.GateSetID, normalizedGateID).Scan(&criteriaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GateVerificationSpec{}, fmt.Errorf("gate %q is not defined in locked gate_set %q for issue %q", normalizedGateID, gateSet.GateSetID, normalizedIssueID)
		}
		return GateVerificationSpec{}, fmt.Errorf("lookup verification criteria for gate %q: %w", normalizedGateID, err)
	}

	var criteria map[string]any
	if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
		return GateVerificationSpec{}, fmt.Errorf("decode criteria_json for gate %q: %w", normalizedGateID, err)
	}
	command, _ := criteria["command"].(string)
	command = strings.TrimSpace(command)
	if command == "" {
		return GateVerificationSpec{}, fmt.Errorf("gate %q has no executable verifier command in criteria.command", normalizedGateID)
	}

	if err := tx.Commit(); err != nil {
		return GateVerificationSpec{}, fmt.Errorf("commit tx: %w", err)
	}
	return GateVerificationSpec{
		IssueID:     normalizedIssueID,
		GateSetID:   gateSet.GateSetID,
		GateSetHash: gateSet.GateSetHash,
		GateID:      normalizedGateID,
		Command:     command,
	}, nil
}

func (s *Store) GetGateStatus(ctx context.Context, issueID string) (GateStatus, error) {
	return s.GetGateStatusForCycle(ctx, GetGateStatusParams{IssueID: issueID})
}

func (s *Store) GetGateStatusForCycle(ctx context.Context, p GetGateStatusParams) (GateStatus, error) {
	normalizedIssueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateStatus{}, err
	}
	if p.CycleNo != nil && *p.CycleNo <= 0 {
		return GateStatus{}, errors.New("--cycle must be > 0")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateStatus{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, normalizedIssueID); err != nil {
		return GateStatus{}, err
	}

	var (
		gateSet lockedGateSet
		found   bool
	)
	if p.CycleNo != nil {
		gateSet, found, err = lockedGateSetForIssueCycleTx(ctx, tx, normalizedIssueID, *p.CycleNo)
	} else {
		gateSet, found, err = lockedGateSetForIssueTx(ctx, tx, normalizedIssueID)
	}
	if err != nil {
		return GateStatus{}, err
	}
	if !found {
		if p.CycleNo != nil {
			return GateStatus{}, fmt.Errorf(
				"no locked gate set found for issue %q cycle %d",
				normalizedIssueID,
				*p.CycleNo,
			)
		}
		return GateStatus{}, fmt.Errorf("no locked gate set found for issue %q", normalizedIssueID)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			i.gate_id,
			i.kind,
			i.required,
			COALESCE(gs.result, ''),
			COALESCE(gs.evidence_refs_json, '[]'),
			COALESCE(gs.evaluated_at, ''),
			COALESCE(gs.last_event_id, '')
		FROM gate_set_items i
		LEFT JOIN gate_status_projection gs
			ON gs.issue_id = ?
			AND gs.gate_set_id = i.gate_set_id
			AND gs.gate_id = i.gate_id
		WHERE i.gate_set_id = ?
		ORDER BY i.gate_id ASC
	`, normalizedIssueID, gateSet.GateSetID)
	if err != nil {
		return GateStatus{}, fmt.Errorf("query gate status for issue %q: %w", normalizedIssueID, err)
	}
	defer rows.Close()

	gates := make([]GateStatusItem, 0)
	for rows.Next() {
		var (
			item         GateStatusItem
			requiredInt  int
			rawResult    string
			evidenceJSON string
		)
		if err := rows.Scan(
			&item.GateID,
			&item.Kind,
			&requiredInt,
			&rawResult,
			&evidenceJSON,
			&item.EvaluatedAt,
			&item.LastEventID,
		); err != nil {
			return GateStatus{}, fmt.Errorf("scan gate status row for issue %q: %w", normalizedIssueID, err)
		}
		item.Required = requiredInt == 1
		if strings.TrimSpace(rawResult) == "" {
			item.Result = "MISSING"
		} else if normalizedResult, err := normalizeGateResult(rawResult); err == nil {
			item.Result = normalizedResult
		} else {
			item.Result = strings.ToUpper(strings.TrimSpace(rawResult))
		}
		evidenceRefs, err := parseReferencesJSON(evidenceJSON)
		if err != nil {
			return GateStatus{}, fmt.Errorf("decode gate status evidence for issue %q: %w", normalizedIssueID, err)
		}
		item.EvidenceRefs = evidenceRefs
		gates = append(gates, item)
	}
	if err := rows.Err(); err != nil {
		return GateStatus{}, fmt.Errorf("iterate gate status rows for issue %q: %w", normalizedIssueID, err)
	}

	if err := tx.Commit(); err != nil {
		return GateStatus{}, fmt.Errorf("commit tx: %w", err)
	}

	return GateStatus{
		IssueID:   normalizedIssueID,
		GateSetID: gateSet.GateSetID,
		CycleNo:   gateSet.CycleNo,
		LockedAt:  gateSet.LockedAt,
		Gates:     gates,
	}, nil
}

func (s *Store) CheckpointSession(ctx context.Context, p CheckpointSessionParams) (Session, bool, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return Session{}, false, errors.New("--session is required")
	}
	trigger := strings.TrimSpace(p.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Session{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	existingSession, err := sessionByIDTx(ctx, tx, sessionID)
	sessionExists := err == nil
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return Session{}, false, err
	}

	now := nowUTC()
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return Session{}, false, err
	}
	checkpoint := map[string]any{
		"session_id":  sessionID,
		"trigger":     trigger,
		"captured_at": now,
	}
	if latestEventID != "" {
		checkpoint["latest_event_id"] = latestEventID
	}

	startedAt := now
	createdBy := actor
	if sessionExists {
		startedAt = existingSession.StartedAt
		createdBy = existingSession.CreatedBy
	}
	chunkID := newID("chk")
	chunkMetadata := map[string]any{
		"trigger":         trigger,
		"latest_event_id": latestEventID,
	}
	payloadBytes, err := json.Marshal(sessionCheckpointedPayload{
		SessionID:           sessionID,
		Trigger:             trigger,
		StartedAt:           startedAt,
		Checkpoint:          checkpoint,
		CheckpointedAt:      now,
		ContextChunkID:      chunkID,
		ContextChunkKind:    "checkpoint",
		ContextChunkContent: fmt.Sprintf("checkpoint for session %s", sessionID),
		ContextChunkMeta:    chunkMetadata,
		CreatedBy:           createdBy,
	})
	if err != nil {
		return Session{}, false, fmt.Errorf("marshal session checkpoint payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            sessionID,
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Session{}, false, err
	}
	if appendRes.Event.EventType != eventTypeSessionCheckpoint {
		return Session{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applySessionCheckpointedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Session{}, false, err
	}

	session, err := sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Session{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return session, appendRes.Event.EntitySeq == 1, nil
}

func (s *Store) RehydrateSession(ctx context.Context, p RehydrateSessionParams) (SessionRehydrateResult, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return SessionRehydrateResult{}, errors.New("--session is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionRehydrateResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := sessionByIDTx(ctx, tx, sessionID); err != nil {
		return SessionRehydrateResult{}, err
	}

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "session", sessionID)
	if err != nil {
		return SessionRehydrateResult{}, err
	}

	result := SessionRehydrateResult{
		SessionID: sessionID,
	}
	if found {
		result.Source = "packet"
		result.Packet = packet
	} else {
		latestEventID, err := latestEventIDTx(ctx, tx)
		if err != nil {
			return SessionRehydrateResult{}, err
		}
		packetJSON := map[string]any{
			"scope":      "session",
			"scope_id":   sessionID,
			"goal":       "Resume session context",
			"state":      map[string]any{"session_id": sessionID},
			"gates":      []any{},
			"open_loops": []any{},
			"next_actions": []any{
				"Build or select a packet for this session",
			},
			"risks":  []any{},
			"source": "raw-events-fallback",
		}
		if latestEventID != "" {
			packetJSON["latest_event_id"] = latestEventID
		}
		result.Source = "raw-events-fallback"
		result.Packet = RehydratePacket{
			Scope:               "session",
			Packet:              packetJSON,
			PacketSchemaVersion: 1,
			BuiltFromEventID:    latestEventID,
			CreatedAt:           nowUTC(),
		}
	}

	if err := tx.Commit(); err != nil {
		return SessionRehydrateResult{}, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}

func (s *Store) BuildRehydratePacket(ctx context.Context, p BuildPacketParams) (RehydratePacket, error) {
	scope := strings.ToLower(strings.TrimSpace(p.Scope))
	if scope != "issue" && scope != "session" {
		return RehydratePacket{}, errors.New("--scope must be issue|session")
	}
	scopeID := strings.TrimSpace(p.ScopeID)
	if scopeID == "" {
		return RehydratePacket{}, errors.New("--id is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return RehydratePacket{}, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	issueIDForSummary := ""
	issueCycleNo := 0

	packetJSON := map[string]any{
		"scope":      scope,
		"scope_id":   scopeID,
		"gates":      []any{},
		"open_loops": []any{},
		"next_actions": []any{
			"Review current state and continue execution",
		},
		"risks": []any{},
	}

	switch scope {
	case "issue":
		issueID, err := normalizeIssueKey(scopeID)
		if err != nil {
			return RehydratePacket{}, err
		}
		issue, err := getIssueTx(ctx, tx, issueID)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["scope_id"] = issueID
		packetJSON["goal"] = issue.Title
		packetJSON["state"] = map[string]any{
			"issue_id": issue.ID,
			"type":     issue.Type,
			"status":   issue.Status,
		}
		if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&issueCycleNo); err != nil {
			return RehydratePacket{}, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
		}
		issueIDForSummary = issueID
		gates, risks, nextActions, err := gateSnapshotForIssueTx(ctx, tx, issueID)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["gates"] = gates
		packetJSON["risks"] = risks
		if len(nextActions) > 0 {
			packetJSON["next_actions"] = nextActions
		}
	case "session":
		if _, err := sessionByIDTx(ctx, tx, scopeID); err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["goal"] = "Resume session context"
		packetJSON["state"] = map[string]any{
			"session_id": scopeID,
		}
	}

	packetID := newID("pkt")
	createdAt := nowUTC()
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return RehydratePacket{}, err
	}
	if issueIDForSummary != "" {
		openLoops, err := listOpenLoopsForIssueCycleTx(ctx, tx, issueIDForSummary, issueCycleNo)
		if err != nil {
			return RehydratePacket{}, err
		}
		packetJSON["open_loops"] = openLoopsToAny(openLoops)
	}
	payloadBytes, err := json.Marshal(packetBuiltPayload{
		PacketID:            packetID,
		Scope:               scope,
		Packet:              packetJSON,
		PacketSchemaVersion: 1,
		BuiltFromEventID:    latestEventID,
		CreatedAt:           createdAt,
		IssueID:             issueIDForSummary,
		IssueCycleNo:        issueCycleNo,
	})
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("marshal packet.built payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypePacket,
		EntityID:            packetID,
		EventType:           eventTypePacketBuilt,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return RehydratePacket{}, err
	}
	if appendRes.Event.EventType != eventTypePacketBuilt {
		return RehydratePacket{}, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyPacketBuiltProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return RehydratePacket{}, err
	}

	packet, err := packetByIDTx(ctx, tx, appendRes.Event.EntityID)
	if err != nil {
		return RehydratePacket{}, err
	}

	if err := tx.Commit(); err != nil {
		return RehydratePacket{}, fmt.Errorf("commit tx: %w", err)
	}

	return packet, nil
}

func (s *Store) GetRehydratePacket(ctx context.Context, p GetPacketParams) (RehydratePacket, error) {
	packetID := strings.TrimSpace(p.PacketID)
	if packetID == "" {
		return RehydratePacket{}, errors.New("--packet is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	packet, err := packetByIDTx(ctx, tx, packetID)
	if err != nil {
		return RehydratePacket{}, err
	}

	if err := tx.Commit(); err != nil {
		return RehydratePacket{}, fmt.Errorf("commit tx: %w", err)
	}
	return packet, nil
}

func (s *Store) UseRehydratePacket(ctx context.Context, p UsePacketParams) (AgentFocus, RehydratePacket, error) {
	agentID := strings.TrimSpace(p.AgentID)
	if agentID == "" {
		return AgentFocus{}, RehydratePacket{}, errors.New("--agent is required")
	}
	packetID := strings.TrimSpace(p.PacketID)
	if packetID == "" {
		return AgentFocus{}, RehydratePacket{}, errors.New("--packet is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	packet, err := packetByIDTx(ctx, tx, packetID)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, err
	}

	activeIssueID := ""
	activeCycleNo := 0
	if packet.Scope == "issue" {
		if rawScopeID, ok := packet.Packet["scope_id"].(string); ok && strings.TrimSpace(rawScopeID) != "" {
			normalizedIssueID, err := normalizeIssueKey(rawScopeID)
			if err == nil {
				activeIssueID = normalizedIssueID
				if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, normalizedIssueID).Scan(&activeCycleNo); err != nil && !errors.Is(err, sql.ErrNoRows) {
					return AgentFocus{}, RehydratePacket{}, fmt.Errorf("read issue cycle for %q: %w", normalizedIssueID, err)
				}
			}
		}
	}

	now := nowUTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_focus(agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			active_issue_id=excluded.active_issue_id,
			active_cycle_no=excluded.active_cycle_no,
			last_packet_id=excluded.last_packet_id,
			updated_at=excluded.updated_at
	`, agentID, nullIfEmpty(activeIssueID), nullIfZero(activeCycleNo), packet.PacketID, now)
	if err != nil {
		return AgentFocus{}, RehydratePacket{}, fmt.Errorf("upsert agent_focus for %q: %w", agentID, err)
	}

	var (
		focus          AgentFocus
		activeIssueRaw sql.NullString
		activeCycleRaw sql.NullInt64
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at
		FROM agent_focus
		WHERE agent_id = ?
	`, agentID).Scan(
		&focus.AgentID,
		&activeIssueRaw,
		&activeCycleRaw,
		&focus.LastPacketID,
		&focus.UpdatedAt,
	); err != nil {
		return AgentFocus{}, RehydratePacket{}, fmt.Errorf("query agent_focus for %q: %w", agentID, err)
	}
	if activeIssueRaw.Valid {
		focus.ActiveIssueID = activeIssueRaw.String
	}
	if activeCycleRaw.Valid {
		focus.ActiveCycleNo = int(activeCycleRaw.Int64)
	}

	if err := tx.Commit(); err != nil {
		return AgentFocus{}, RehydratePacket{}, fmt.Errorf("commit tx: %w", err)
	}
	return focus, packet, nil
}

func (s *Store) ListOpenLoops(ctx context.Context, p ListOpenLoopsParams) ([]OpenLoop, error) {
	var (
		args         []any
		conditions   []string
		normalizedID string
	)
	if strings.TrimSpace(p.IssueID) != "" {
		issueID, err := normalizeIssueKey(p.IssueID)
		if err != nil {
			return nil, err
		}
		normalizedID = issueID
		conditions = append(conditions, "issue_id = ?")
		args = append(args, issueID)
	}
	if p.CycleNo != nil {
		if *p.CycleNo <= 0 {
			return nil, errors.New("--cycle must be > 0")
		}
		conditions = append(conditions, "cycle_no = ?")
		args = append(args, *p.CycleNo)
	}

	query := `
		SELECT loop_id, issue_id, cycle_no, loop_type, status,
			COALESCE(owner, ''), COALESCE(priority, ''), COALESCE(source_event_id, ''), updated_at
		FROM open_loops
	`
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY issue_id ASC, cycle_no ASC, updated_at DESC, loop_id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		if normalizedID != "" {
			return nil, fmt.Errorf("list open loops for issue %q: %w", normalizedID, err)
		}
		return nil, fmt.Errorf("list open loops: %w", err)
	}
	defer rows.Close()

	loops := make([]OpenLoop, 0)
	for rows.Next() {
		var item OpenLoop
		if err := rows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan open loop row: %w", err)
		}
		loops = append(loops, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open loops: %w", err)
	}
	return loops, nil
}

func (s *Store) CreateGateTemplate(ctx context.Context, p CreateGateTemplateParams) (GateTemplate, bool, error) {
	templateID, err := normalizeGateTemplateID(p.TemplateID)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if p.Version <= 0 {
		return GateTemplate{}, false, errors.New("--version must be > 0")
	}
	appliesTo, err := normalizeGateAppliesTo(p.AppliesTo)
	if err != nil {
		return GateTemplate{}, false, err
	}
	definitionJSON, definitionHash, err := canonicalizeGateDefinition(p.DefinitionJSON)
	if err != nil {
		return GateTemplate{}, false, err
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var (
		existingAppliesToJSON string
		existingDefinition    string
		existingHash          string
		existingCreatedAt     string
		existingCreatedBy     string
	)
	err = tx.QueryRowContext(ctx, `
		SELECT applies_to_json, definition_json, definition_hash, created_at, created_by
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, templateID, p.Version).Scan(
		&existingAppliesToJSON,
		&existingDefinition,
		&existingHash,
		&existingCreatedAt,
		&existingCreatedBy,
	)
	if err == nil {
		existingAppliesTo, parseErr := parseAppliesToJSON(existingAppliesToJSON)
		if parseErr != nil {
			return GateTemplate{}, false, parseErr
		}
		if existingHash == definitionHash &&
			existingDefinition == definitionJSON &&
			equalStringSlices(existingAppliesTo, appliesTo) {
			if err := tx.Commit(); err != nil {
				return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return GateTemplate{
				TemplateID:     templateID,
				Version:        p.Version,
				AppliesTo:      existingAppliesTo,
				DefinitionJSON: existingDefinition,
				DefinitionHash: existingHash,
				CreatedAt:      existingCreatedAt,
				CreatedBy:      existingCreatedBy,
			}, true, nil
		}
		return GateTemplate{}, false, fmt.Errorf("template %s@%d already exists (create a new version to change it)", templateID, p.Version)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return GateTemplate{}, false, fmt.Errorf("query gate template %s@%d: %w", templateID, p.Version, err)
	}

	appliesToJSON, err := json.Marshal(appliesTo)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("encode applies_to for %s@%d: %w", templateID, p.Version, err)
	}
	createdAt := nowUTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, templateID, p.Version, string(appliesToJSON), definitionJSON, definitionHash, createdAt, actor)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("insert gate template %s@%d: %w", templateID, p.Version, err)
	}

	if err := tx.Commit(); err != nil {
		return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateTemplate{
		TemplateID:     templateID,
		Version:        p.Version,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		DefinitionHash: definitionHash,
		CreatedAt:      createdAt,
		CreatedBy:      actor,
	}, false, nil
}

func (s *Store) ListGateTemplates(ctx context.Context, p ListGateTemplatesParams) ([]GateTemplate, error) {
	var issueTypeFilter string
	if strings.TrimSpace(p.IssueType) != "" {
		normalizedType, err := normalizeIssueType(p.IssueType)
		if err != nil {
			return nil, err
		}
		issueTypeFilter = normalizedType
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT template_id, version, applies_to_json, definition_json, definition_hash, created_at, created_by
		FROM gate_templates
		ORDER BY template_id ASC, version ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query gate_templates: %w", err)
	}
	defer rows.Close()

	templates := make([]GateTemplate, 0)
	for rows.Next() {
		var (
			template      GateTemplate
			appliesToJSON string
		)
		if err := rows.Scan(
			&template.TemplateID,
			&template.Version,
			&appliesToJSON,
			&template.DefinitionJSON,
			&template.DefinitionHash,
			&template.CreatedAt,
			&template.CreatedBy,
		); err != nil {
			return nil, fmt.Errorf("scan gate template: %w", err)
		}
		appliesTo, err := parseAppliesToJSON(appliesToJSON)
		if err != nil {
			return nil, err
		}
		template.AppliesTo = appliesTo
		if issueTypeFilter != "" && !stringSliceContains(appliesTo, issueTypeFilter) {
			continue
		}
		templates = append(templates, template)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate_templates rows: %w", err)
	}
	return templates, nil
}

func (s *Store) InstantiateGateSet(ctx context.Context, p InstantiateGateSetParams) (GateSet, bool, error) {
	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}
	templateRefs, parsedRefs, err := normalizeGateTemplateRefs(p.TemplateRefs)
	if err != nil {
		return GateSet{}, false, err
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	issue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return GateSet{}, false, err
	}

	var cycleNo int
	if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
		return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
	}

	existing, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if found {
		existingRefs := normalizeReferences(existing.TemplateRefs)
		if equalStringSlices(existingRefs, templateRefs) {
			if err := tx.Commit(); err != nil {
				return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return existing, true, nil
		}
		return GateSet{}, false, fmt.Errorf(
			"gate set already exists for issue %q cycle %d (existing gate_set_id %q)",
			issueID,
			cycleNo,
			existing.GateSetID,
		)
	}

	gates, err := buildGateSetDefinitionsTx(ctx, tx, issue.Type, parsedRefs)
	if err != nil {
		return GateSet{}, false, err
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition(templateRefs, gates)
	if err != nil {
		return GateSet{}, false, err
	}
	hash := sha256.Sum256([]byte(frozenJSON))
	gateSetHash := hex.EncodeToString(hash[:])
	gateSetID := newID("gset")
	createdAt := nowUTC()

	templateRefsJSON, err := json.Marshal(templateRefs)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("encode template refs for %s cycle %d: %w", issueID, cycleNo, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, gateSetID, issueID, cycleNo, string(templateRefsJSON), frozenJSON, gateSetHash, createdAt, actor)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("insert gate set for issue %q cycle %d: %w", issueID, cycleNo, err)
	}

	for _, gate := range gates {
		criteriaJSON, err := json.Marshal(gate.Criteria)
		if err != nil {
			return GateSet{}, false, fmt.Errorf("encode gate criteria for %s/%s: %w", gateSetID, gate.GateID, err)
		}
		requiredInt := 0
		if gate.Required {
			requiredInt = 1
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
			VALUES(?, ?, ?, ?, ?)
		`, gateSetID, gate.GateID, gate.Kind, requiredInt, string(criteriaJSON))
		if err != nil {
			return GateSet{}, false, fmt.Errorf("insert gate set item %s/%s: %w", gateSetID, gate.GateID, err)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, gateSetID, createdAt, issueID)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("set active_gate_set_id for issue %q: %w", issueID, err)
	}

	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateSet{
		GateSetID:        gateSetID,
		IssueID:          issueID,
		CycleNo:          cycleNo,
		TemplateRefs:     templateRefs,
		FrozenDefinition: frozenObj,
		GateSetHash:      gateSetHash,
		CreatedAt:        createdAt,
		CreatedBy:        actor,
		Items:            gates,
	}, false, nil
}

func (s *Store) LockGateSet(ctx context.Context, p LockGateSetParams) (GateSet, bool, error) {
	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return GateSet{}, false, err
	}

	cycleNo := 0
	if p.CycleNo != nil {
		if *p.CycleNo <= 0 {
			return GateSet{}, false, errors.New("--cycle must be > 0")
		}
		cycleNo = *p.CycleNo
	} else {
		if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
			return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
		}
	}

	gateSet, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("no gate set found for issue %q cycle %d", issueID, cycleNo)
	}
	if len(gateSet.Items) == 0 {
		return GateSet{}, false, fmt.Errorf("cannot lock gate set %q: no gate items defined", gateSet.GateSetID)
	}

	lockedNow := false
	lockTime := strings.TrimSpace(gateSet.LockedAt)
	if lockTime == "" {
		lockTime = nowUTC()
		if _, err := tx.ExecContext(ctx, `
			UPDATE gate_sets
			SET locked_at = ?
			WHERE gate_set_id = ?
		`, lockTime, gateSet.GateSetID); err != nil {
			return GateSet{}, false, fmt.Errorf("lock gate set %q: %w", gateSet.GateSetID, err)
		}
		lockedNow = true
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, gateSet.GateSetID, nowUTC(), issueID); err != nil {
		return GateSet{}, false, fmt.Errorf("set active gate set for issue %q: %w", issueID, err)
	}

	gateSet.LockedAt = lockTime
	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return gateSet, lockedNow, nil
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
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id
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
		var labelsJSON string
		var referencesJSON string
		if err := rows.Scan(
			&issue.ID,
			&issue.Type,
			&issue.Title,
			&issue.ParentID,
			&issue.Status,
			&issue.Priority,
			&labelsJSON,
			&issue.Description,
			&issue.Acceptance,
			&referencesJSON,
			&issue.CreatedAt,
			&issue.UpdatedAt,
			&issue.LastEventID,
		); err != nil {
			return nil, fmt.Errorf("scan work_item row: %w", err)
		}
		labels, err := parseLabelsJSON(labelsJSON)
		if err != nil {
			return nil, fmt.Errorf("scan work_item row labels: %w", err)
		}
		issue.Labels = labels
		references, err := parseReferencesJSON(referencesJSON)
		if err != nil {
			return nil, fmt.Errorf("scan work_item row references: %w", err)
		}
		issue.References = references
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items rows: %w", err)
	}

	return issues, nil
}

func (s *Store) NextIssue(ctx context.Context, agent string) (IssueNextResult, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id
		FROM work_items
		WHERE status IN ('Todo', 'InProgress')
		ORDER BY id ASC
	`)
	if err != nil {
		return IssueNextResult{}, fmt.Errorf("query next-issue candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]IssueNextCandidate, 0)
	for rows.Next() {
		var (
			issue          Issue
			labelsJSON     string
			referencesJSON string
		)
		if err := rows.Scan(
			&issue.ID,
			&issue.Type,
			&issue.Title,
			&issue.ParentID,
			&issue.Status,
			&issue.Priority,
			&labelsJSON,
			&issue.Description,
			&issue.Acceptance,
			&referencesJSON,
			&issue.CreatedAt,
			&issue.UpdatedAt,
			&issue.LastEventID,
		); err != nil {
			return IssueNextResult{}, fmt.Errorf("scan next-issue candidate row: %w", err)
		}
		labels, err := parseLabelsJSON(labelsJSON)
		if err != nil {
			return IssueNextResult{}, fmt.Errorf("decode candidate labels for issue %q: %w", issue.ID, err)
		}
		issue.Labels = labels
		references, err := parseReferencesJSON(referencesJSON)
		if err != nil {
			return IssueNextResult{}, fmt.Errorf("decode candidate references for issue %q: %w", issue.ID, err)
		}
		issue.References = references

		score, reasons := scoreIssueCandidate(issue, issue.Priority)
		candidates = append(candidates, IssueNextCandidate{
			Issue:   issue,
			Score:   score,
			Reasons: reasons,
		})
	}
	if err := rows.Err(); err != nil {
		return IssueNextResult{}, fmt.Errorf("iterate next-issue candidate rows: %w", err)
	}
	if len(candidates) == 0 {
		return IssueNextResult{}, errors.New("no actionable issues found (Todo/InProgress)")
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].Issue.UpdatedAt != candidates[j].Issue.UpdatedAt {
			return candidates[i].Issue.UpdatedAt < candidates[j].Issue.UpdatedAt
		}
		return candidates[i].Issue.ID < candidates[j].Issue.ID
	})

	result := IssueNextResult{
		Agent:      strings.TrimSpace(agent),
		Candidate:  candidates[0],
		Candidates: candidates,
		Considered: len(candidates),
	}
	return result, nil
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

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return ReplayResult{}, fmt.Errorf("defer foreign keys for replay: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_status_projection`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_status_projection: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM rehydrate_packets
		WHERE packet_id IN (
			SELECT entity_id
			FROM events
			WHERE entity_type = ? AND event_type = ?
		)
	`, entityTypePacket, eventTypePacketBuilt); err != nil {
		return ReplayResult{}, fmt.Errorf("clear event-sourced rehydrate_packets: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM issue_summaries WHERE summary_level = 'packet'`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear packet issue_summaries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM open_loops`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear open_loops: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_chunks`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear context_chunks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sessions`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear sessions: %w", err)
	}
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
	if err := syncAllOpenLoopsTx(ctx, tx); err != nil {
		return ReplayResult{}, err
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
	case eventTypeGateEval:
		return applyGateEvaluatedProjectionTx(ctx, tx, event)
	case eventTypeSessionCheckpoint:
		return applySessionCheckpointedProjectionTx(ctx, tx, event)
	case eventTypePacketBuilt:
		return applyPacketBuiltProjectionTx(ctx, tx, event)
	default:
		return nil
	}
}

func applyIssueCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueCreatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.created payload for event %s: %w", event.EventID, err)
	}
	referencesJSON, err := json.Marshal(normalizeReferences(payload.References))
	if err != nil {
		return fmt.Errorf("encode issue.created references payload for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status,
			description, acceptance_criteria, references_json,
			labels_json, current_cycle_no, active_gate_set_id,
			created_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, '[]', 1, NULL, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			type=excluded.type,
			title=excluded.title,
			parent_id=excluded.parent_id,
			status=excluded.status,
			description=excluded.description,
			acceptance_criteria=excluded.acceptance_criteria,
			references_json=excluded.references_json,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.Type,
		payload.Title,
		nullIfEmpty(payload.ParentID),
		payload.Status,
		strings.TrimSpace(payload.Description),
		strings.TrimSpace(payload.AcceptanceCriteria),
		string(referencesJSON),
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

	setClauses := make([]string, 0, 8)
	args := make([]any, 0, 10)

	if payload.StatusTo != nil {
		issueStatus, err := normalizeIssueStatus(*payload.StatusTo)
		if err != nil {
			return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "status = ?")
		args = append(args, issueStatus)
	}
	if payload.PriorityTo != nil {
		setClauses = append(setClauses, "priority = ?")
		args = append(args, nullIfEmpty(strings.TrimSpace(*payload.PriorityTo)))
	}
	if payload.LabelsTo != nil {
		labelsJSON, err := json.Marshal(normalizeLabels(*payload.LabelsTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated labels payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "labels_json = ?")
		args = append(args, string(labelsJSON))
	}
	if payload.DescriptionTo != nil {
		setClauses = append(setClauses, "description = ?")
		args = append(args, strings.TrimSpace(*payload.DescriptionTo))
	}
	if payload.AcceptanceCriteriaTo != nil {
		setClauses = append(setClauses, "acceptance_criteria = ?")
		args = append(args, strings.TrimSpace(*payload.AcceptanceCriteriaTo))
	}
	if payload.ReferencesTo != nil {
		referencesJSON, err := json.Marshal(normalizeReferences(*payload.ReferencesTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated references payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "references_json = ?")
		args = append(args, string(referencesJSON))
	}
	if len(setClauses) == 0 {
		return fmt.Errorf("decode issue.updated payload for event %s: no mutable fields provided", event.EventID)
	}
	setClauses = append(setClauses, "updated_at = ?", "last_event_id = ?")
	args = append(args, event.CreatedAt, event.EventID, payload.IssueID)

	query := `
		UPDATE work_items
		SET ` + strings.Join(setClauses, ", ") + `
		WHERE id = ?
	`
	result, err := tx.ExecContext(ctx, query, args...)
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

func applyGateEvaluatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateEvaluatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if payload.EvaluatedAt == "" {
		payload.EvaluatedAt = event.CreatedAt
	}

	var gateCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, payload.GateSetID, payload.GateID).Scan(&gateCount); err != nil {
		return fmt.Errorf("validate gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if gateCount == 0 {
		return fmt.Errorf(
			"validate gate.evaluated payload for event %s: gate %q not found in gate_set %q",
			event.EventID,
			payload.GateID,
			payload.GateSetID,
		)
	}

	evidenceJSON, err := json.Marshal(normalizeReferences(payload.EvidenceRefs))
	if err != nil {
		return fmt.Errorf("encode gate.evaluated evidence refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_status_projection(
			issue_id, gate_set_id, gate_id, result,
			evidence_refs_json, evaluated_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(issue_id, gate_set_id, gate_id) DO UPDATE SET
			result=excluded.result,
			evidence_refs_json=excluded.evidence_refs_json,
			evaluated_at=excluded.evaluated_at,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.GateSetID,
		payload.GateID,
		payload.Result,
		string(evidenceJSON),
		payload.EvaluatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert gate status projection from event %s: %w", event.EventID, err)
	}
	if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
		return fmt.Errorf("sync open loops from gate event %s: %w", event.EventID, err)
	}
	return nil
}

func applySessionCheckpointedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionCheckpointedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.checkpointed payload for event %s: %w", event.EventID, err)
	}
	checkpointJSON, err := json.Marshal(payload.Checkpoint)
	if err != nil {
		return fmt.Errorf("encode session.checkpointed checkpoint payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.checkpointed context metadata for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO sessions(
			session_id, trigger, started_at, ended_at, summary_event_id, checkpoint_json, created_by
		) VALUES(?, ?, ?, NULL, NULL, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			trigger=excluded.trigger,
			started_at=excluded.started_at,
			ended_at=NULL,
			checkpoint_json=excluded.checkpoint_json,
			created_by=excluded.created_by
	`, payload.SessionID, payload.Trigger, payload.StartedAt, string(checkpointJSON), payload.CreatedBy)
	if err != nil {
		return fmt.Errorf("upsert session from event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO context_chunks(
			chunk_id, session_id, entity_type, entity_id, kind, content, metadata_json, embedding_ref, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(chunk_id) DO UPDATE SET
			session_id=excluded.session_id,
			entity_type=excluded.entity_type,
			entity_id=excluded.entity_id,
			kind=excluded.kind,
			content=excluded.content,
			metadata_json=excluded.metadata_json,
			created_at=excluded.created_at
	`,
		payload.ContextChunkID,
		payload.SessionID,
		entityTypeSession,
		payload.SessionID,
		payload.ContextChunkKind,
		payload.ContextChunkContent,
		string(contextChunkMetaJSON),
		payload.CheckpointedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert context chunk from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyPacketBuiltProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload packetBuiltPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode packet.built payload for event %s: %w", event.EventID, err)
	}
	packetJSON, err := json.Marshal(payload.Packet)
	if err != nil {
		return fmt.Errorf("encode packet.built packet json for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(
			packet_id, scope, packet_json, packet_schema_version, built_from_event_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(packet_id) DO UPDATE SET
			scope=excluded.scope,
			packet_json=excluded.packet_json,
			packet_schema_version=excluded.packet_schema_version,
			built_from_event_id=excluded.built_from_event_id,
			created_at=excluded.created_at
	`, payload.PacketID, payload.Scope, string(packetJSON), payload.PacketSchemaVersion, nullIfEmpty(payload.BuiltFromEventID), payload.CreatedAt)
	if err != nil {
		return fmt.Errorf("upsert rehydrate packet from event %s: %w", event.EventID, err)
	}

	if strings.TrimSpace(payload.IssueID) != "" && payload.IssueCycleNo > 0 {
		if err := upsertIssueSummaryForPacketTx(ctx, tx, payload.IssueID, payload.IssueCycleNo, string(packetJSON), payload.PacketID, payload.CreatedAt); err != nil {
			return fmt.Errorf("upsert issue summary from event %s: %w", event.EventID, err)
		}
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

func getHumanAuthCredentialTx(ctx context.Context, tx *sql.Tx) (HumanAuthCredential, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT credential_id, algorithm, iterations, salt_hex, hash_hex, created_at, updated_at, rotated_by
		FROM human_auth_credentials
		WHERE credential_id = 'default'
	`)
	var credential HumanAuthCredential
	if err := row.Scan(
		&credential.CredentialID,
		&credential.Algorithm,
		&credential.Iterations,
		&credential.SaltHex,
		&credential.HashHex,
		&credential.CreatedAt,
		&credential.UpdatedAt,
		&credential.RotatedBy,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanAuthCredential{}, false, nil
		}
		return HumanAuthCredential{}, false, fmt.Errorf("query human auth credential: %w", err)
	}
	return credential, true, nil
}

func getIssueQueryable(ctx context.Context, q queryable, id string) (Issue, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id
		FROM work_items
		WHERE id = ?
	`, id)
	var issue Issue
	var labelsJSON string
	var referencesJSON string
	if err := row.Scan(
		&issue.ID,
		&issue.Type,
		&issue.Title,
		&issue.ParentID,
		&issue.Status,
		&issue.Priority,
		&labelsJSON,
		&issue.Description,
		&issue.Acceptance,
		&referencesJSON,
		&issue.CreatedAt,
		&issue.UpdatedAt,
		&issue.LastEventID,
	); err != nil {
		return Issue{}, err
	}
	labels, err := parseLabelsJSON(labelsJSON)
	if err != nil {
		return Issue{}, err
	}
	issue.Labels = labels
	references, err := parseReferencesJSON(referencesJSON)
	if err != nil {
		return Issue{}, err
	}
	issue.References = references
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

func normalizeGateResult(raw string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "PASS":
		return "PASS", nil
	case "FAIL":
		return "FAIL", nil
	case "BLOCKED":
		return "BLOCKED", nil
	default:
		return "", fmt.Errorf("invalid --result %q (expected PASS|FAIL|BLOCKED)", raw)
	}
}

func decodeGateEvaluatedPayload(payloadJSON string) (gateEvaluatedPayload, error) {
	var payload gateEvaluatedPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gateEvaluatedPayload{}, err
	}

	issueID, err := normalizeIssueKey(payload.IssueID)
	if err != nil {
		return gateEvaluatedPayload{}, fmt.Errorf("invalid issue_id: %w", err)
	}
	payload.IssueID = issueID

	payload.GateSetID = strings.TrimSpace(payload.GateSetID)
	if payload.GateSetID == "" {
		return gateEvaluatedPayload{}, errors.New("gate_set_id is required")
	}
	payload.GateID = strings.TrimSpace(payload.GateID)
	if payload.GateID == "" {
		return gateEvaluatedPayload{}, errors.New("gate_id is required")
	}
	result, err := normalizeGateResult(payload.Result)
	if err != nil {
		return gateEvaluatedPayload{}, err
	}
	payload.Result = result
	payload.EvidenceRefs = normalizeReferences(payload.EvidenceRefs)
	payload.EvaluatedAt = strings.TrimSpace(payload.EvaluatedAt)

	return payload, nil
}

func normalizeGateTemplateID(raw string) (string, error) {
	templateID := strings.ToLower(strings.TrimSpace(raw))
	if templateID == "" {
		return "", errors.New("--id is required")
	}
	if len(templateID) < 2 || len(templateID) > 64 {
		return "", fmt.Errorf("invalid template id %q (must be 2-64 chars)", raw)
	}
	for i, r := range templateID {
		if i == 0 && (r < 'a' || r > 'z') {
			return "", fmt.Errorf("invalid template id %q (must start with a lowercase letter)", raw)
		}
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return "", fmt.Errorf("invalid template id %q (allowed: lowercase letters, digits, -, _)", raw)
		}
	}
	return templateID, nil
}

func normalizeGateAppliesTo(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, errors.New("--applies-to is required")
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		issueType, err := normalizeIssueType(value)
		if err != nil {
			return nil, fmt.Errorf("invalid --applies-to %q: %w", value, err)
		}
		if seen[issueType] {
			continue
		}
		seen[issueType] = true
		normalized = append(normalized, issueType)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func parseAppliesToJSON(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("applies_to_json is empty")
	}
	var appliesTo []string
	if err := json.Unmarshal([]byte(raw), &appliesTo); err != nil {
		return nil, fmt.Errorf("decode applies_to_json: %w", err)
	}
	return normalizeGateAppliesTo(appliesTo)
}

func canonicalizeGateDefinition(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("--file must contain JSON")
	}

	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", "", fmt.Errorf("invalid gate definition JSON: %w", err)
	}

	canonicalBytes, err := json.Marshal(decoded)
	if err != nil {
		return "", "", fmt.Errorf("canonicalize gate definition JSON: %w", err)
	}
	hash := sha256.Sum256(canonicalBytes)
	return string(canonicalBytes), hex.EncodeToString(hash[:]), nil
}

func parseGateTemplateRef(raw string) (gateTemplateRef, error) {
	ref := strings.TrimSpace(raw)
	templateIDRaw, versionRaw, ok := strings.Cut(ref, "@")
	if !ok {
		return gateTemplateRef{}, fmt.Errorf("invalid --template %q (expected <template_id>@<version>)", raw)
	}
	templateID, err := normalizeGateTemplateID(templateIDRaw)
	if err != nil {
		return gateTemplateRef{}, err
	}
	version, err := strconv.Atoi(strings.TrimSpace(versionRaw))
	if err != nil || version <= 0 {
		return gateTemplateRef{}, fmt.Errorf("invalid --template %q (version must be > 0)", raw)
	}
	return gateTemplateRef{
		TemplateID: templateID,
		Version:    version,
		Ref:        fmt.Sprintf("%s@%d", templateID, version),
	}, nil
}

func normalizeGateTemplateRefs(rawRefs []string) ([]string, []gateTemplateRef, error) {
	if len(rawRefs) == 0 {
		return nil, nil, errors.New("--template is required")
	}
	parsed := make([]gateTemplateRef, 0, len(rawRefs))
	seen := make(map[string]bool, len(rawRefs))
	for _, raw := range rawRefs {
		ref, err := parseGateTemplateRef(raw)
		if err != nil {
			return nil, nil, err
		}
		if seen[ref.Ref] {
			continue
		}
		seen[ref.Ref] = true
		parsed = append(parsed, ref)
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].Ref < parsed[j].Ref
	})
	refs := make([]string, 0, len(parsed))
	for _, ref := range parsed {
		refs = append(refs, ref.Ref)
	}
	return refs, parsed, nil
}

func buildGateSetDefinitionsTx(ctx context.Context, tx *sql.Tx, issueType string, refs []gateTemplateRef) ([]GateSetDefinition, error) {
	gatesByID := make(map[string]GateSetDefinition)
	for _, ref := range refs {
		var (
			appliesToJSON  string
			definitionJSON string
		)
		err := tx.QueryRowContext(ctx, `
			SELECT applies_to_json, definition_json
			FROM gate_templates
			WHERE template_id = ? AND version = ?
		`, ref.TemplateID, ref.Version).Scan(&appliesToJSON, &definitionJSON)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("gate template %s@%d not found", ref.TemplateID, ref.Version)
		}
		if err != nil {
			return nil, fmt.Errorf("query gate template %s@%d: %w", ref.TemplateID, ref.Version, err)
		}

		appliesTo, err := parseAppliesToJSON(appliesToJSON)
		if err != nil {
			return nil, err
		}
		if !stringSliceContains(appliesTo, issueType) {
			return nil, fmt.Errorf("gate template %s@%d does not apply to issue type %s", ref.TemplateID, ref.Version, issueType)
		}

		defs, err := extractGateDefinitions(definitionJSON)
		if err != nil {
			return nil, fmt.Errorf("invalid gate definition in template %s@%d: %w", ref.TemplateID, ref.Version, err)
		}
		for _, gate := range defs {
			if existing, exists := gatesByID[gate.GateID]; exists {
				return nil, fmt.Errorf("duplicate gate id %q across templates (%s conflicts with existing %s)", gate.GateID, ref.Ref, existing.GateID)
			}
			gatesByID[gate.GateID] = gate
		}
	}

	gates := make([]GateSetDefinition, 0, len(gatesByID))
	for _, gate := range gatesByID {
		gates = append(gates, gate)
	}
	sort.Slice(gates, func(i, j int) bool {
		return gates[i].GateID < gates[j].GateID
	})
	if len(gates) == 0 {
		return nil, errors.New("instantiated gate set has no gates")
	}
	return gates, nil
}

func extractGateDefinitions(definitionJSON string) ([]GateSetDefinition, error) {
	var parsed struct {
		Gates []struct {
			ID       string          `json:"id"`
			Kind     string          `json:"kind"`
			Required *bool           `json:"required"`
			Criteria json.RawMessage `json:"criteria"`
		} `json:"gates"`
	}
	if err := json.Unmarshal([]byte(definitionJSON), &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Gates) == 0 {
		return nil, errors.New("definition must contain at least one gate")
	}

	defs := make([]GateSetDefinition, 0, len(parsed.Gates))
	seen := make(map[string]bool, len(parsed.Gates))
	for _, gate := range parsed.Gates {
		gateID := strings.TrimSpace(gate.ID)
		if gateID == "" {
			return nil, errors.New("each gate requires a non-empty id")
		}
		if seen[gateID] {
			return nil, fmt.Errorf("duplicate gate id %q in template definition", gateID)
		}
		seen[gateID] = true

		kind := strings.TrimSpace(gate.Kind)
		if kind == "" {
			kind = "check"
		}
		required := true
		if gate.Required != nil {
			required = *gate.Required
		}

		var criteria any = map[string]any{}
		if len(gate.Criteria) > 0 {
			if err := json.Unmarshal(gate.Criteria, &criteria); err != nil {
				return nil, fmt.Errorf("decode criteria for gate %q: %w", gateID, err)
			}
		}

		defs = append(defs, GateSetDefinition{
			GateID:   gateID,
			Kind:     kind,
			Required: required,
			Criteria: criteria,
		})
	}

	sort.Slice(defs, func(i, j int) bool {
		return defs[i].GateID < defs[j].GateID
	})
	return defs, nil
}

func buildFrozenGateDefinition(templateRefs []string, gates []GateSetDefinition) (string, map[string]any, error) {
	frozen := struct {
		Templates []string            `json:"templates"`
		Gates     []GateSetDefinition `json:"gates"`
	}{
		Templates: templateRefs,
		Gates:     gates,
	}
	frozenBytes, err := json.Marshal(frozen)
	if err != nil {
		return "", nil, fmt.Errorf("encode frozen gate definition: %w", err)
	}
	var frozenObj map[string]any
	if err := json.Unmarshal(frozenBytes, &frozenObj); err != nil {
		return "", nil, fmt.Errorf("decode frozen gate definition: %w", err)
	}
	return string(frozenBytes), frozenObj, nil
}

func gateSetForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) (GateSet, bool, error) {
	var (
		gateSet          GateSet
		templateRefsJSON string
		frozenJSON       string
		lockedAt         sql.NullString
	)
	err := tx.QueryRowContext(ctx, `
		SELECT gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		FROM gate_sets
		WHERE issue_id = ? AND cycle_no = ?
	`, issueID, cycleNo).Scan(
		&gateSet.GateSetID,
		&gateSet.IssueID,
		&gateSet.CycleNo,
		&templateRefsJSON,
		&frozenJSON,
		&gateSet.GateSetHash,
		&lockedAt,
		&gateSet.CreatedAt,
		&gateSet.CreatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GateSet{}, false, nil
	}
	if err != nil {
		return GateSet{}, false, fmt.Errorf("query gate set for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	if lockedAt.Valid {
		gateSet.LockedAt = lockedAt.String
	}
	templateRefs, err := parseReferencesJSON(templateRefsJSON)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("decode template refs for gate set %q: %w", gateSet.GateSetID, err)
	}
	gateSet.TemplateRefs = templateRefs
	if strings.TrimSpace(frozenJSON) != "" {
		if err := json.Unmarshal([]byte(frozenJSON), &gateSet.FrozenDefinition); err != nil {
			return GateSet{}, false, fmt.Errorf("decode frozen definition for gate set %q: %w", gateSet.GateSetID, err)
		}
	}
	items, err := gateSetItemsTx(ctx, tx, gateSet.GateSetID)
	if err != nil {
		return GateSet{}, false, err
	}
	gateSet.Items = items
	return gateSet, true, nil
}

func gateSetItemsTx(ctx context.Context, tx *sql.Tx, gateSetID string) ([]GateSetDefinition, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT gate_id, kind, required, criteria_json
		FROM gate_set_items
		WHERE gate_set_id = ?
		ORDER BY gate_id ASC
	`, gateSetID)
	if err != nil {
		return nil, fmt.Errorf("query gate set items for %q: %w", gateSetID, err)
	}
	defer rows.Close()

	items := make([]GateSetDefinition, 0)
	for rows.Next() {
		var (
			item         GateSetDefinition
			requiredInt  int
			criteriaJSON string
		)
		if err := rows.Scan(&item.GateID, &item.Kind, &requiredInt, &criteriaJSON); err != nil {
			return nil, fmt.Errorf("scan gate set item for %q: %w", gateSetID, err)
		}
		item.Required = requiredInt == 1
		var criteria any
		if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
			return nil, fmt.Errorf("decode criteria_json for %q/%s: %w", gateSetID, item.GateID, err)
		}
		item.Criteria = criteria
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate set items for %q: %w", gateSetID, err)
	}
	return items, nil
}

func latestEventIDTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var latest sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT event_id
		FROM events
		ORDER BY event_order DESC
		LIMIT 1
	`).Scan(&latest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query latest event id: %w", err)
	}
	if latest.Valid {
		return latest.String, nil
	}
	return "", nil
}

func sessionByIDTx(ctx context.Context, tx *sql.Tx, sessionID string) (Session, error) {
	var (
		session        Session
		endedAt        sql.NullString
		summaryEventID sql.NullString
		checkpointJSON string
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			session_id,
			trigger,
			started_at,
			ended_at,
			summary_event_id,
			COALESCE(checkpoint_json, '{}'),
			created_by
		FROM sessions
		WHERE session_id = ?
	`, sessionID).Scan(
		&session.SessionID,
		&session.Trigger,
		&session.StartedAt,
		&endedAt,
		&summaryEventID,
		&checkpointJSON,
		&session.CreatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, fmt.Errorf("session %q not found", sessionID)
	}
	if err != nil {
		return Session{}, fmt.Errorf("query session %q: %w", sessionID, err)
	}
	if endedAt.Valid {
		session.EndedAt = endedAt.String
	}
	if summaryEventID.Valid {
		session.SummaryEventID = summaryEventID.String
	}
	if strings.TrimSpace(checkpointJSON) != "" {
		if err := json.Unmarshal([]byte(checkpointJSON), &session.Checkpoint); err != nil {
			return Session{}, fmt.Errorf("decode session checkpoint_json for %q: %w", sessionID, err)
		}
	}
	return session, nil
}

func packetByIDTx(ctx context.Context, tx *sql.Tx, packetID string) (RehydratePacket, error) {
	var (
		packet     RehydratePacket
		packetJSON string
		builtFrom  sql.NullString
	)
	err := tx.QueryRowContext(ctx, `
		SELECT packet_id, scope, packet_json, packet_schema_version, built_from_event_id, created_at
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, packetID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&packetJSON,
		&packet.PacketSchemaVersion,
		&builtFrom,
		&packet.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RehydratePacket{}, fmt.Errorf("packet %q not found", packetID)
	}
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("query packet %q: %w", packetID, err)
	}
	if builtFrom.Valid {
		packet.BuiltFromEventID = builtFrom.String
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, fmt.Errorf("decode packet_json for %q: %w", packetID, err)
	}
	return packet, nil
}

func latestPacketForScopeIDTx(ctx context.Context, tx *sql.Tx, scope, scopeID string) (RehydratePacket, bool, error) {
	var (
		packet     RehydratePacket
		packetJSON string
		builtFrom  sql.NullString
	)
	err := tx.QueryRowContext(ctx, `
		SELECT packet_id, scope, packet_json, packet_schema_version, built_from_event_id, created_at
		FROM rehydrate_packets
		WHERE scope = ?
			AND json_extract(packet_json, '$.scope_id') = ?
		ORDER BY created_at DESC, packet_id DESC
		LIMIT 1
	`, scope, scopeID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&packetJSON,
		&packet.PacketSchemaVersion,
		&builtFrom,
		&packet.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RehydratePacket{}, false, nil
	}
	if err != nil {
		return RehydratePacket{}, false, fmt.Errorf("query latest packet for %s:%s: %w", scope, scopeID, err)
	}
	if builtFrom.Valid {
		packet.BuiltFromEventID = builtFrom.String
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, false, fmt.Errorf("decode packet_json for %q: %w", packet.PacketID, err)
	}
	return packet, true, nil
}

func gateSnapshotForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) ([]any, []any, []any, error) {
	gates := make([]any, 0)
	risks := make([]any, 0)
	nextActions := make([]any, 0)

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		nextActions = append(nextActions, "Instantiate and lock a gate set for the current cycle")
		return gates, risks, nextActions, nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			i.gate_id,
			i.required,
			COALESCE(gs.result, ''),
			COALESCE(gs.evidence_refs_json, '[]')
		FROM gate_set_items i
		LEFT JOIN gate_status_projection gs
			ON gs.issue_id = ?
			AND gs.gate_set_id = i.gate_set_id
			AND gs.gate_id = i.gate_id
		WHERE i.gate_set_id = ?
		ORDER BY i.gate_id ASC
	`, issueID, gateSet.GateSetID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("query gate snapshot for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			gateID       string
			requiredInt  int
			result       string
			evidenceJSON string
			evidenceRefs []string
		)
		if err := rows.Scan(&gateID, &requiredInt, &result, &evidenceJSON); err != nil {
			return nil, nil, nil, fmt.Errorf("scan gate snapshot row for issue %q: %w", issueID, err)
		}
		if err := json.Unmarshal([]byte(evidenceJSON), &evidenceRefs); err != nil {
			return nil, nil, nil, fmt.Errorf("decode gate snapshot evidence for issue %q: %w", issueID, err)
		}
		normalized := "MISSING"
		if strings.TrimSpace(result) != "" {
			normalized = strings.ToUpper(strings.TrimSpace(result))
		}
		required := requiredInt == 1
		gates = append(gates, map[string]any{
			"gate_id":       gateID,
			"required":      required,
			"result":        normalized,
			"evidence_refs": evidenceRefs,
		})
		if required && normalized != "PASS" {
			risks = append(risks, fmt.Sprintf("Required gate %s is %s", gateID, normalized))
			nextActions = append(nextActions, fmt.Sprintf("Resolve required gate %s (%s)", gateID, normalized))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("iterate gate snapshot rows for issue %q: %w", issueID, err)
	}

	if len(nextActions) == 0 {
		nextActions = append(nextActions, "All required gates are passing")
	}
	return gates, risks, nextActions, nil
}

func syncOpenLoopsForIssueFromGatesTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	cycleNo int,
	gates []any,
	sourceEventID string,
) ([]OpenLoop, error) {
	now := nowUTC()
	expectedOpen := make(map[string]OpenLoop)
	for _, rawGate := range gates {
		gateMap, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		gateID, _ := gateMap["gate_id"].(string)
		if strings.TrimSpace(gateID) == "" {
			continue
		}
		required, _ := gateMap["required"].(bool)
		if !required {
			continue
		}
		result, _ := gateMap["result"].(string)
		if strings.EqualFold(strings.TrimSpace(result), "PASS") {
			continue
		}
		loopID := deterministicLoopID(issueID, cycleNo, "gate", gateID)
		expectedOpen[loopID] = OpenLoop{
			LoopID:        loopID,
			IssueID:       issueID,
			CycleNo:       cycleNo,
			LoopType:      "gate",
			Status:        "Open",
			Priority:      "P1",
			SourceEventID: sourceEventID,
			UpdatedAt:     now,
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT loop_id, status
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND loop_type = 'gate'
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query existing gate loops for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	existing := make(map[string]string)
	for rows.Next() {
		var (
			loopID string
			status string
		)
		if err := rows.Scan(&loopID, &status); err != nil {
			return nil, fmt.Errorf("scan existing gate loop row for issue %q: %w", issueID, err)
		}
		existing[loopID] = status
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing gate loops for issue %q: %w", issueID, err)
	}

	for loopID, loop := range expectedOpen {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO open_loops(
				loop_id, issue_id, cycle_no, loop_type, status, owner, priority, source_event_id, updated_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(loop_id) DO UPDATE SET
				issue_id=excluded.issue_id,
				cycle_no=excluded.cycle_no,
				loop_type=excluded.loop_type,
				status=excluded.status,
				owner=excluded.owner,
				priority=excluded.priority,
				source_event_id=excluded.source_event_id,
				updated_at=excluded.updated_at
		`, loopID, loop.IssueID, loop.CycleNo, loop.LoopType, loop.Status, nullIfEmpty(loop.Owner), nullIfEmpty(loop.Priority), nullIfEmpty(loop.SourceEventID), loop.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("upsert open loop %q: %w", loopID, err)
		}
	}

	for loopID, status := range existing {
		if _, stillOpen := expectedOpen[loopID]; stillOpen {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(status), "Resolved") {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_loops
			SET status = 'Resolved', updated_at = ?
			WHERE loop_id = ?
		`, now, loopID); err != nil {
			return nil, fmt.Errorf("resolve stale open loop %q: %w", loopID, err)
		}
	}

	loopRows, err := tx.QueryContext(ctx, `
		SELECT loop_id, issue_id, cycle_no, loop_type, status,
			COALESCE(owner, ''), COALESCE(priority, ''), COALESCE(source_event_id, ''), updated_at
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
		ORDER BY status ASC, loop_id ASC
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query synchronized loops for issue %q: %w", issueID, err)
	}
	defer loopRows.Close()

	loops := make([]OpenLoop, 0)
	for loopRows.Next() {
		var item OpenLoop
		if err := loopRows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan synchronized loop row for issue %q: %w", issueID, err)
		}
		loops = append(loops, item)
	}
	if err := loopRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate synchronized loop rows for issue %q: %w", issueID, err)
	}
	return loops, nil
}

func syncOpenLoopsForCurrentCycleTx(ctx context.Context, tx *sql.Tx, issueID, sourceEventID string) ([]OpenLoop, error) {
	var (
		cycleNo     int
		lastEventID string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT current_cycle_no, COALESCE(last_event_id, '')
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &lastEventID); err != nil {
		return nil, fmt.Errorf("query current cycle for issue %q: %w", issueID, err)
	}
	if strings.TrimSpace(sourceEventID) == "" {
		sourceEventID = lastEventID
	}
	gates, _, _, err := gateSnapshotForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, err
	}
	return syncOpenLoopsForIssueFromGatesTx(ctx, tx, issueID, cycleNo, gates, sourceEventID)
}

func syncAllOpenLoopsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, COALESCE(last_event_id, '')
		FROM work_items
	`)
	if err != nil {
		return fmt.Errorf("query work_items for open-loop sync: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var issueID string
		var lastEventID string
		if err := rows.Scan(&issueID, &lastEventID); err != nil {
			return fmt.Errorf("scan work_item for open-loop sync: %w", err)
		}
		if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, issueID, lastEventID); err != nil {
			return fmt.Errorf("sync open loops for issue %q: %w", issueID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate work_items for open-loop sync: %w", err)
	}
	return nil
}

func listOpenLoopsForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) ([]OpenLoop, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			loop_id,
			issue_id,
			cycle_no,
			loop_type,
			status,
			COALESCE(owner, ''),
			COALESCE(priority, ''),
			COALESCE(source_event_id, ''),
			updated_at
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
		ORDER BY
			CASE status WHEN 'Open' THEN 0 ELSE 1 END,
			updated_at DESC,
			loop_id ASC
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query open loops for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	defer rows.Close()

	loops := make([]OpenLoop, 0)
	for rows.Next() {
		var item OpenLoop
		if err := rows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan open loop row: %w", err)
		}
		loops = append(loops, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open loops: %w", err)
	}
	return loops, nil
}

func upsertIssueSummaryForPacketTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int, summaryJSON, packetID, createdAt string) error {
	var maxSeq int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(entity_seq), 0)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, entityTypeIssue, issueID).Scan(&maxSeq); err != nil {
		return fmt.Errorf("query max entity_seq for issue %q summary: %w", issueID, err)
	}

	summaryID := "sum_" + strings.TrimSpace(packetID)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO issue_summaries(
			summary_id, issue_id, cycle_no, summary_level, summary_json,
			from_entity_seq, to_entity_seq, parent_summary_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(summary_id) DO UPDATE SET
			issue_id=excluded.issue_id,
			cycle_no=excluded.cycle_no,
			summary_level=excluded.summary_level,
			summary_json=excluded.summary_json,
			from_entity_seq=excluded.from_entity_seq,
			to_entity_seq=excluded.to_entity_seq,
			parent_summary_id=excluded.parent_summary_id,
			created_at=excluded.created_at
	`,
		summaryID,
		issueID,
		cycleNo,
		"packet",
		summaryJSON,
		1,
		maxSeq,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert issue summary for issue %q: %w", issueID, err)
	}
	return nil
}

func openLoopsToAny(loops []OpenLoop) []any {
	items := make([]any, 0, len(loops))
	for _, loop := range loops {
		items = append(items, map[string]any{
			"loop_id":         loop.LoopID,
			"issue_id":        loop.IssueID,
			"cycle_no":        loop.CycleNo,
			"loop_type":       loop.LoopType,
			"status":          loop.Status,
			"owner":           loop.Owner,
			"priority":        loop.Priority,
			"source_event_id": loop.SourceEventID,
			"updated_at":      loop.UpdatedAt,
		})
	}
	return items
}

func deterministicLoopID(issueID string, cycleNo int, loopType, key string) string {
	sum := sha256.Sum256([]byte(issueID + ":" + strconv.Itoa(cycleNo) + ":" + loopType + ":" + key))
	return "loop_" + hex.EncodeToString(sum[:])[:12]
}

func scoreIssueCandidate(issue Issue, priority string) (int, []string) {
	score := 0
	reasons := make([]string, 0, 4)

	switch issue.Status {
	case "InProgress":
		score += 100
		reasons = append(reasons, "in-progress work is prioritized for continuity")
	case "Todo":
		score += 50
		reasons = append(reasons, "todo work is actionable")
	}

	switch issue.Type {
	case "Task":
		score += 40
		reasons = append(reasons, "task is implementation-ready")
	case "Bug":
		score += 35
		reasons = append(reasons, "bug fix has high operational value")
	case "Story":
		score += 20
		reasons = append(reasons, "story provides cross-task scope")
	case "Epic":
		score += 10
		reasons = append(reasons, "epic is planning-level work")
	}

	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "P0":
		score += 30
		reasons = append(reasons, "priority P0")
	case "P1":
		score += 20
		reasons = append(reasons, "priority P1")
	case "P2":
		score += 10
		reasons = append(reasons, "priority P2")
	}

	if issue.ParentID == "" && (issue.Type == "Task" || issue.Type == "Bug") {
		score += 5
		reasons = append(reasons, "standalone item can start immediately")
	}
	return score, reasons
}

func nullIfZero(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
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

type lockedGateSet struct {
	GateSetID   string
	GateSetHash string
	CycleNo     int
	LockedAt    string
}

type gateTemplateRef struct {
	TemplateID string
	Version    int
	Ref        string
}

func lockedGateSetForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (lockedGateSet, bool, error) {
	var gateSet lockedGateSet
	err := tx.QueryRowContext(ctx, `
		SELECT gs.gate_set_id, gs.gate_set_hash, gs.cycle_no, gs.locked_at
		FROM gate_sets gs
		INNER JOIN work_items wi
			ON wi.id = gs.issue_id
			AND wi.current_cycle_no = gs.cycle_no
		WHERE gs.issue_id = ?
			AND gs.locked_at IS NOT NULL
		ORDER BY gs.cycle_no DESC
		LIMIT 1
	`, issueID).Scan(&gateSet.GateSetID, &gateSet.GateSetHash, &gateSet.CycleNo, &gateSet.LockedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedGateSet{}, false, nil
	}
	if err != nil {
		return lockedGateSet{}, false, fmt.Errorf("query locked gate set for issue %q: %w", issueID, err)
	}
	return gateSet, true, nil
}

func lockedGateSetForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) (lockedGateSet, bool, error) {
	var gateSet lockedGateSet
	err := tx.QueryRowContext(ctx, `
		SELECT gate_set_id, gate_set_hash, cycle_no, locked_at
		FROM gate_sets
		WHERE issue_id = ?
			AND cycle_no = ?
			AND locked_at IS NOT NULL
	`, issueID, cycleNo).Scan(&gateSet.GateSetID, &gateSet.GateSetHash, &gateSet.CycleNo, &gateSet.LockedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedGateSet{}, false, nil
	}
	if err != nil {
		return lockedGateSet{}, false, fmt.Errorf("query locked gate set for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	return gateSet, true, nil
}

func validateIssueCloseEligibilityTx(ctx context.Context, tx *sql.Tx, issueID string) (*IssueCloseAuthorization, error) {
	openChildren, err := listIncompleteChildIssuesTx(ctx, tx, issueID)
	if err != nil {
		return nil, fmt.Errorf("close validation %w", err)
	}
	if len(openChildren) > 0 {
		return nil, fmt.Errorf(
			"close validation failed for issue %q: child issues must be Done first: %s",
			issueID,
			strings.Join(openChildren, ", "),
		)
	}

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, fmt.Errorf("close validation %w", err)
	}
	if !found {
		return nil, fmt.Errorf("close validation failed for issue %q: no locked gate set for current cycle", issueID)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			r.gate_id,
			COALESCE((
				SELECT e.payload_json
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
	`,
		entityTypeIssue, issueID, eventTypeGateEval, gateSet.GateSetID,
		gateSet.GateSetID,
	)
	if err != nil {
		return nil, fmt.Errorf("close validation list required gates for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	failures := make([]string, 0)
	closeProof := &IssueCloseAuthorization{
		GateSetID:   gateSet.GateSetID,
		GateSetHash: gateSet.GateSetHash,
		Gates:       make([]IssueCloseGateProof, 0),
	}
	for rows.Next() {
		var (
			gateID      string
			payloadJSON string
		)
		if err := rows.Scan(&gateID, &payloadJSON); err != nil {
			return nil, fmt.Errorf("close validation scan required gate for issue %q: %w", issueID, err)
		}
		if strings.TrimSpace(payloadJSON) == "" {
			failures = append(failures, gateID+"=MISSING")
			continue
		}
		payload, err := decodeGateEvaluatedPayload(payloadJSON)
		if err != nil {
			return nil, fmt.Errorf("close validation decode required gate %q for issue %q: %w", gateID, issueID, err)
		}
		normalizedResult := strings.ToUpper(strings.TrimSpace(payload.Result))
		if normalizedResult != "PASS" {
			failures = append(failures, gateID+"="+normalizedResult)
			continue
		}
		if len(payload.EvidenceRefs) == 0 {
			failures = append(failures, gateID+"=PASS_NO_PROOF")
			continue
		}
		if payload.Proof == nil ||
			strings.TrimSpace(payload.Proof.Runner) == "" ||
			strings.TrimSpace(payload.Proof.GateSetHash) != gateSet.GateSetHash ||
			payload.Proof.ExitCode != 0 {
			failures = append(failures, gateID+"=PASS_UNVERIFIED")
			continue
		}
		closeProof.Gates = append(closeProof.Gates, IssueCloseGateProof{
			GateID:       payload.GateID,
			Result:       payload.Result,
			EvidenceRefs: copyStringSlice(payload.EvidenceRefs),
			Proof:        payload.Proof,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("close validation iterate required gates for issue %q: %w", issueID, err)
	}

	if len(failures) > 0 {
		return nil, fmt.Errorf(
			"close validation failed for issue %q (gate_set %q): required gates not PASS: %s",
			issueID,
			gateSet.GateSetID,
			strings.Join(failures, ", "),
		)
	}
	return closeProof, nil
}

func listIncompleteChildIssuesTx(ctx context.Context, tx *sql.Tx, parentID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, status
		FROM work_items
		WHERE parent_id = ?
			AND status != 'Done'
		ORDER BY id ASC
	`, parentID)
	if err != nil {
		return nil, fmt.Errorf("list incomplete child issues for %q: %w", parentID, err)
	}
	defer rows.Close()

	children := make([]string, 0)
	for rows.Next() {
		var childID, status string
		if err := rows.Scan(&childID, &status); err != nil {
			return nil, fmt.Errorf("scan incomplete child issue for %q: %w", parentID, err)
		}
		children = append(children, childID+"="+status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incomplete child issues for %q: %w", parentID, err)
	}
	return children, nil
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

func normalizeReferences(references []string) []string {
	if len(references) == 0 {
		return []string{}
	}
	normalized := make([]string, 0, len(references))
	seen := make(map[string]bool, len(references))
	for _, reference := range references {
		ref := strings.TrimSpace(reference)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		normalized = append(normalized, ref)
	}
	return normalized
}

func parseReferencesJSON(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	var references []string
	if err := json.Unmarshal([]byte(raw), &references); err != nil {
		return nil, fmt.Errorf("decode references_json: %w", err)
	}
	return normalizeReferences(references), nil
}

func normalizeLabels(labels []string) []string {
	return normalizeReferences(labels)
}

func normalizeGateEvaluationProof(proof *GateEvaluationProof) *GateEvaluationProof {
	if proof == nil {
		return nil
	}
	normalized := *proof
	normalized.Verifier = strings.TrimSpace(normalized.Verifier)
	normalized.Runner = strings.TrimSpace(normalized.Runner)
	normalized.RunnerVersion = strings.TrimSpace(normalized.RunnerVersion)
	normalized.StartedAt = strings.TrimSpace(normalized.StartedAt)
	normalized.FinishedAt = strings.TrimSpace(normalized.FinishedAt)
	normalized.GateSetHash = strings.TrimSpace(normalized.GateSetHash)
	return &normalized
}

func parseLabelsJSON(raw string) ([]string, error) {
	labels, err := parseReferencesJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode labels_json: %w", err)
	}
	return labels, nil
}

func normalizePriority(raw string) (string, error) {
	priority := strings.ToUpper(strings.TrimSpace(raw))
	if priority == "" {
		return "", nil
	}
	if len(priority) > 32 {
		return "", fmt.Errorf("invalid --priority %q (max length 32)", raw)
	}
	for _, ch := range priority {
		if (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' {
			return "", fmt.Errorf("invalid --priority %q", raw)
		}
	}
	return priority, nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func copyStringSlice(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
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
