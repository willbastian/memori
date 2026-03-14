package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/willbastian/memori/internal/dbschema"

	_ "modernc.org/sqlite"
)

const DefaultIssueKeyPrefix = "mem"

const (
	packetSchemaVersion                 = 2
	packetRelevantChunkLimit            = 3
	compactionEventThreshold            = 25
	compactionOpenLoopThreshold         = 1
	compactionContextChunkThreshold     = 3
	compactionPolicyVersion             = 1
	compactionPolicyMode                = "deterministic-ledger-derivation"
	compactionPolicyBuildReasonOnDemand = "on-demand-packet-build"
)

const (
	entityTypeIssue        = "issue"
	entityTypeSession      = "session"
	entityTypePacket       = "packet"
	entityTypeFocus        = "focus"
	entityTypeGateTemplate = "gate_template"
	entityTypeGateSet      = "gate_set"

	eventTypeIssueCreate         = "issue.created"
	eventTypeIssueUpdate         = "issue.updated"
	eventTypeIssueLink           = "issue.linked"
	eventTypeGateEval            = "gate.evaluated"
	eventTypeSessionCheckpoint   = "session.checkpointed"
	eventTypeSessionSummarized   = "session.summarized"
	eventTypeSessionClosed       = "session.closed"
	eventTypePacketBuilt         = "packet.built"
	eventTypeFocusUsed           = "focus.used"
	eventTypeGateTemplateCreate  = "gate_template.created"
	eventTypeGateTemplateApprove = "gate_template.approved"
	eventTypeGateSetCreate       = "gate_set.instantiated"
	eventTypeGateSetLock         = "gate_set.locked"
)

func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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

func sqliteDSN(path string) string {
	dsn := path
	dsn = appendSQLiteQueryParam(dsn, "_txlock=immediate")
	dsn = appendSQLiteQueryParam(dsn, "_pragma=journal_mode(WAL)")
	dsn = appendSQLiteQueryParam(dsn, "_pragma=foreign_keys(ON)")
	dsn = appendSQLiteQueryParam(dsn, "_pragma=busy_timeout(5000)")
	return dsn
}

func appendSQLiteQueryParam(dsn, param string) string {
	key := param
	if idx := strings.Index(param, "="); idx >= 0 {
		key = param[:idx+1]
	}
	if strings.Contains(dsn, key) {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&" + param
	}
	return dsn + "?" + param
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
	if _, err := dbschema.Migrate(ctx, s.db, nil); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := nowUTC()
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

	if p.Title != nil {
		titleTo := strings.TrimSpace(*p.Title)
		if titleTo == "" {
			return Issue{}, Event{}, false, errors.New("--title is required")
		}
		if currentIssue.Title != titleTo {
			titleFrom := currentIssue.Title
			payload.TitleFrom = &titleFrom
			payload.TitleTo = &titleTo
			changed = true
		}
	}
	if p.Status != nil {
		statusTo, err := normalizeIssueStatus(*p.Status)
		if err != nil {
			return Issue{}, Event{}, false, err
		}
		if statusTo == "WontDo" && !actorIsHuman(p.Actor) {
			return Issue{}, Event{}, false, errors.New("WontDo status requires a human actor")
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
		return Issue{}, Event{}, false, errors.New("--title, --status, --priority, --label, --description, --acceptance-criteria, or --reference is required")
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

func latestEventForEntityTx(ctx context.Context, tx *sql.Tx, entityType, entityID string) (Event, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ? AND entity_id = ?
		ORDER BY entity_seq DESC
		LIMIT 1
	`, entityType, entityID)
	event, err := scanEvent(row)
	if err == nil {
		return event, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	return Event{}, false, fmt.Errorf("query latest event for %s:%s: %w", entityType, entityID, err)
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

	if strings.TrimSpace(req.CorrelationID) == "" {
		req.CorrelationID = defaultCorrelationID(req.EntityType, req.EntityID)
	}
	if strings.TrimSpace(req.CausationID) == "" {
		previousEvent, found, err := latestEventForEntityTx(ctx, tx, req.EntityType, req.EntityID)
		if err != nil {
			return appendEventResult{}, err
		}
		if found {
			req.CausationID = previousEvent.EventID
		}
	}

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

func gateSetByIDTx(ctx context.Context, tx *sql.Tx, gateSetID string) (GateSet, bool, error) {
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
		WHERE gate_set_id = ?
	`, gateSetID).Scan(
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
		return GateSet{}, false, fmt.Errorf("query gate set %q: %w", gateSetID, err)
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

func gateTemplateByIDVersionTx(ctx context.Context, tx *sql.Tx, templateID string, version int) (GateTemplate, bool, error) {
	var (
		template      GateTemplate
		appliesToJSON string
	)
	err := tx.QueryRowContext(ctx, `
		SELECT t.template_id, t.version, t.applies_to_json, t.definition_json, t.definition_hash,
			COALESCE(a.approved_at, ''), COALESCE(a.approved_by, ''), t.created_at, t.created_by
		FROM gate_templates AS t
		LEFT JOIN gate_template_approvals AS a
			ON a.template_id = t.template_id
			AND a.version = t.version
		WHERE t.template_id = ? AND t.version = ?
	`, templateID, version).Scan(
		&template.TemplateID,
		&template.Version,
		&appliesToJSON,
		&template.DefinitionJSON,
		&template.DefinitionHash,
		&template.ApprovedAt,
		&template.ApprovedBy,
		&template.CreatedAt,
		&template.CreatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GateTemplate{}, false, nil
	}
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("query gate template %s@%d: %w", templateID, version, err)
	}
	appliesTo, err := parseAppliesToJSON(appliesToJSON)
	if err != nil {
		return GateTemplate{}, false, err
	}
	template.AppliesTo = appliesTo
	template.Executable = gateDefinitionContainsExecutableCommand(template.DefinitionJSON)
	return template, true, nil
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

func latestOpenSessionTx(ctx context.Context, tx *sql.Tx) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		WHERE COALESCE(TRIM(s.ended_at), '') = ''
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession)
	if err != nil {
		return Session{}, fmt.Errorf("query latest open session: %w", err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func latestSessionTx(ctx context.Context, tx *sql.Tx) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession)
	if err != nil {
		return Session{}, fmt.Errorf("query latest session: %w", err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func sessionForCommandIDTx(ctx context.Context, tx *sql.Tx, commandID string) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT entity_id
		FROM events
		WHERE entity_type = ?
		  AND command_id = ?
		ORDER BY event_order DESC, entity_seq DESC
		LIMIT 1
	`, entityTypeSession, commandID)
	if err != nil {
		return Session{}, fmt.Errorf("query session for command %q: %w", commandID, err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func sessionIDForQueryTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (string, error) {
	var sessionID string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sql.ErrNoRows
		}
		return "", err
	}
	return sessionID, nil
}

func packetByIDTx(ctx context.Context, tx *sql.Tx, packetID string) (RehydratePacket, error) {
	var (
		packet     RehydratePacket
		packetJSON string
		builtFrom  sql.NullString
		scopeID    sql.NullString
		issueID    sql.NullString
		sessionID  sql.NullString
		issueCycle sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			packet_id,
			scope,
			COALESCE(scope_id, json_extract(packet_json, '$.scope_id')),
			COALESCE(issue_id, CASE WHEN scope = 'issue' THEN json_extract(packet_json, '$.scope_id') END),
			COALESCE(session_id, CASE WHEN scope = 'session' THEN json_extract(packet_json, '$.scope_id') END),
			issue_cycle_no,
			packet_json,
			packet_schema_version,
			built_from_event_id,
			created_at
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, packetID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&scopeID,
		&issueID,
		&sessionID,
		&issueCycle,
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
	if scopeID.Valid {
		packet.ScopeID = scopeID.String
	}
	if issueID.Valid {
		packet.IssueID = issueID.String
	}
	if sessionID.Valid {
		packet.SessionID = sessionID.String
	}
	if issueCycle.Valid {
		packet.IssueCycleNo = int(issueCycle.Int64)
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, fmt.Errorf("decode packet_json for %q: %w", packetID, err)
	}
	return packet, nil
}

func latestPacketForScopeIDTx(ctx context.Context, tx *sql.Tx, scope, scopeID string) (RehydratePacket, bool, error) {
	var (
		packet        RehydratePacket
		packetJSON    string
		builtFrom     sql.NullString
		packetScopeID sql.NullString
		issueID       sql.NullString
		sessionID     sql.NullString
		issueCycle    sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			packet_id,
			scope,
			COALESCE(scope_id, json_extract(packet_json, '$.scope_id')),
			COALESCE(issue_id, CASE WHEN scope = 'issue' THEN json_extract(packet_json, '$.scope_id') END),
			COALESCE(session_id, CASE WHEN scope = 'session' THEN json_extract(packet_json, '$.scope_id') END),
			issue_cycle_no,
			packet_json,
			packet_schema_version,
			built_from_event_id,
			created_at
		FROM rehydrate_packets
		WHERE scope = ?
			AND COALESCE(scope_id, json_extract(packet_json, '$.scope_id')) = ?
		ORDER BY created_at DESC, packet_id DESC
		LIMIT 1
	`, scope, scopeID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&packetScopeID,
		&issueID,
		&sessionID,
		&issueCycle,
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
	if packetScopeID.Valid {
		packet.ScopeID = packetScopeID.String
	}
	if issueID.Valid {
		packet.IssueID = issueID.String
	}
	if sessionID.Valid {
		packet.SessionID = sessionID.String
	}
	if issueCycle.Valid {
		packet.IssueCycleNo = int(issueCycle.Int64)
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, false, fmt.Errorf("decode packet_json for %q: %w", packet.PacketID, err)
	}
	return packet, true, nil
}

func agentFocusByAgentTx(ctx context.Context, tx *sql.Tx, agent string) (AgentFocus, bool, error) {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return AgentFocus{}, false, nil
	}

	var (
		focus         AgentFocus
		activeIssueID sql.NullString
		activeCycleNo sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at
		FROM agent_focus
		WHERE agent_id = ?
	`, agent).Scan(&focus.AgentID, &activeIssueID, &activeCycleNo, &focus.LastPacketID, &focus.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentFocus{}, false, nil
	}
	if err != nil {
		return AgentFocus{}, false, fmt.Errorf("query agent focus for %q: %w", agent, err)
	}
	if activeIssueID.Valid {
		focus.ActiveIssueID = activeIssueID.String
	}
	if activeCycleNo.Valid {
		focus.ActiveCycleNo = int(activeCycleNo.Int64)
	}
	return focus, true, nil
}

func eventOrderByIDTx(ctx context.Context, tx *sql.Tx, eventID string) (int64, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return 0, false, nil
	}

	var order int64
	err := tx.QueryRowContext(ctx, `
		SELECT event_order
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&order)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query event order for %q: %w", eventID, err)
	}
	return order, true, nil
}

func latestContinuityEventOrderForIssueCycleTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issueLastEventID string,
	currentCycleNo int,
) (int64, error) {
	eventIDs := make([]string, 0, 8)
	if strings.TrimSpace(issueLastEventID) != "" {
		eventIDs = append(eventIDs, issueLastEventID)
	}

	lockedGateSet, found, err := lockedGateSetForIssueCycleTx(ctx, tx, issueID, currentCycleNo)
	if err != nil {
		return 0, err
	}
	if found {
		rows, err := tx.QueryContext(ctx, `
			SELECT COALESCE(gs.last_event_id, '')
			FROM gate_set_items i
			LEFT JOIN gate_status_projection gs
				ON gs.issue_id = ?
				AND gs.gate_set_id = i.gate_set_id
				AND gs.gate_id = i.gate_id
			WHERE i.gate_set_id = ?
		`, issueID, lockedGateSet.GateSetID)
		if err != nil {
			return 0, fmt.Errorf("query gate continuity events for issue %q: %w", issueID, err)
		}
		defer rows.Close()

		for rows.Next() {
			var eventID string
			if err := rows.Scan(&eventID); err != nil {
				return 0, fmt.Errorf("scan gate continuity event for issue %q: %w", issueID, err)
			}
			if strings.TrimSpace(eventID) != "" {
				eventIDs = append(eventIDs, eventID)
			}
		}
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate gate continuity events for issue %q: %w", issueID, err)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT COALESCE(source_event_id, '')
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
	`, issueID, currentCycleNo)
	if err != nil {
		return 0, fmt.Errorf("query open-loop continuity events for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return 0, fmt.Errorf("scan open-loop continuity event for issue %q cycle %d: %w", issueID, currentCycleNo, err)
		}
		if strings.TrimSpace(eventID) != "" {
			eventIDs = append(eventIDs, eventID)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate open-loop continuity events for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}

	maxOrder := int64(0)
	seen := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		eventID = strings.TrimSpace(eventID)
		if eventID == "" {
			continue
		}
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		order, found, err := eventOrderByIDTx(ctx, tx, eventID)
		if err != nil {
			return 0, err
		}
		if found && order > maxOrder {
			maxOrder = order
		}
	}
	return maxOrder, nil
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
