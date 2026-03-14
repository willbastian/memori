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
	"sort"
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
	gateSetEvent, _, err := latestEventForEntityTx(ctx, tx, entityTypeGateSet, gateSet.GateSetID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	proof := normalizeGateEvaluationProof(p.Proof)
	if proof != nil && strings.TrimSpace(proof.GateSetHash) == "" {
		proof.GateSetHash = gateSet.GateSetHash
	}

	var (
		requiredInt  int
		criteriaJSON string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT required, criteria_json
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, gateSet.GateSetID, gateID).Scan(&requiredInt, &criteriaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GateEvaluation{}, Event{}, false, fmt.Errorf(
				"gate %q is not defined in locked gate_set %q for issue %q",
				gateID,
				gateSet.GateSetID,
				issueID,
			)
		}
		return GateEvaluation{}, Event{}, false, fmt.Errorf("lookup gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
	}

	var criteria any
	if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("decode criteria_json for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
	}
	if result == "PASS" && gateCriteriaCommand(criteria) != "" && proof == nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf(
			"gate %q uses executable criteria.command; use memori gate verify --issue %s --gate %s to record PASS",
			gateID,
			issueID,
			gateID,
		)
	}
	if requiredInt == 0 {
		// Optional gates may still use manual PASS/FAIL/BLOCKED recording for informational workflows.
	} else if result == "PASS" && gateCriteriaCommand(criteria) == "" && proof != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("gate %q has no executable criteria.command and cannot accept verifier proof", gateID)
	}

	if strings.TrimSpace(criteriaJSON) == "" {
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
		CausationID:         gateSetEvent.EventID,
		CorrelationID:       gateCycleCorrelationID(issueID, gateSet.CycleNo),
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
	fullGateSet, found, err := gateSetByIDTx(ctx, tx, gateSet.GateSetID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	if !found {
		return GateVerificationSpec{}, fmt.Errorf("gate set %q not found", gateSet.GateSetID)
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
	if err := validateExecutableGateVerificationGovernanceTx(ctx, tx, fullGateSet, normalizedGateID, command); err != nil {
		return GateVerificationSpec{}, err
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
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateTemplate{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateTemplate{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateTemplateCreate {
			return GateTemplate{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateTemplateCreatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateTemplate{}, false, err
		}
		template, found, err := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
		}
		if !found {
			if err := applyGateTemplateCreatedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateTemplate{}, false, err
			}
			template, found, err = gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after replaying event %s", payload.TemplateID, payload.Version, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

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

	var (
		existingAppliesToJSON string
		existingDefinition    string
		existingHash          string
	)
	err = tx.QueryRowContext(ctx, `
		SELECT applies_to_json, definition_json, definition_hash
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, templateID, p.Version).Scan(
		&existingAppliesToJSON,
		&existingDefinition,
		&existingHash,
	)
	if err == nil {
		existingAppliesTo, parseErr := parseAppliesToJSON(existingAppliesToJSON)
		if parseErr != nil {
			return GateTemplate{}, false, parseErr
		}
		if existingHash == definitionHash &&
			existingDefinition == definitionJSON &&
			equalStringSlices(existingAppliesTo, appliesTo) {
			template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after idempotent lookup", templateID, p.Version)
			}
			if err := tx.Commit(); err != nil {
				return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return template, true, nil
		}
		return GateTemplate{}, false, fmt.Errorf("template %s@%d already exists (create a new version to change it)", templateID, p.Version)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return GateTemplate{}, false, fmt.Errorf("query gate template %s@%d: %w", templateID, p.Version, err)
	}

	payload := gateTemplateCreatedPayload{
		TemplateID:     templateID,
		Version:        p.Version,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		DefinitionHash: definitionHash,
		CreatedAt:      nowUTC(),
		CreatedBy:      p.Actor,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateTemplate,
		EntityID:            gateTemplateEntityID(templateID, p.Version),
		EventType:           eventTypeGateTemplateCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateTemplateCorrelationID(templateID, p.Version),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateTemplate{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateTemplateCreate {
		return GateTemplate{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateTemplateCreatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateTemplate{}, false, err
		}
	}

	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after projection", templateID, p.Version)
	}

	if err := tx.Commit(); err != nil {
		return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return template, appendRes.AlreadyExists, nil
}

func (s *Store) ApproveGateTemplate(ctx context.Context, p ApproveGateTemplateParams) (GateTemplate, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateTemplate{}, false, errors.New("--command-id is required")
	}
	if !actorIsHumanGoverned(p.Actor) {
		return GateTemplate{}, false, errors.New("executable gate template approval requires a human-governed actor")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateTemplate{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateTemplateApprove {
			return GateTemplate{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateTemplateApprovedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateTemplate{}, false, err
		}
		template, found, err := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
		}
		if !found || !actorIsHumanGoverned(template.ApprovedBy) {
			if err := applyGateTemplateApprovedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateTemplate{}, false, err
			}
			template, found, err = gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after replaying event %s", payload.TemplateID, payload.Version, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

	templateID, err := normalizeGateTemplateID(p.TemplateID)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if p.Version <= 0 {
		return GateTemplate{}, false, errors.New("--version must be > 0")
	}

	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found", templateID, p.Version)
	}
	if !template.Executable {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d has no executable criteria.command and does not require approval", templateID, p.Version)
	}
	if actorIsHumanGoverned(template.ApprovedBy) {
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

	payload := gateTemplateApprovedPayload{
		TemplateID:     templateID,
		Version:        p.Version,
		DefinitionHash: template.DefinitionHash,
		ApprovedAt:     nowUTC(),
		ApprovedBy:     p.Actor,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateTemplate,
		EntityID:            gateTemplateEntityID(templateID, p.Version),
		EventType:           eventTypeGateTemplateApprove,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateTemplateCorrelationID(templateID, p.Version),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateTemplate{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateTemplateApprove {
		return GateTemplate{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateTemplateApprovedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateTemplate{}, false, err
		}
	}

	template, found, err = gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after approval", templateID, p.Version)
	}

	if err := tx.Commit(); err != nil {
		return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return template, appendRes.AlreadyExists, nil
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
		SELECT t.template_id, t.version, t.applies_to_json, t.definition_json, t.definition_hash,
			COALESCE(a.approved_at, ''), COALESCE(a.approved_by, ''), t.created_at, t.created_by
		FROM gate_templates
		AS t
		LEFT JOIN gate_template_approvals AS a
			ON a.template_id = t.template_id
			AND a.version = t.version
		ORDER BY t.template_id ASC, t.version ASC
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
			&template.ApprovedAt,
			&template.ApprovedBy,
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
		template.Executable = gateDefinitionContainsExecutableCommand(template.DefinitionJSON)
		templates = append(templates, template)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate_templates rows: %w", err)
	}
	return templates, nil
}

func (s *Store) ListPendingExecutableGateTemplates(ctx context.Context) ([]GateTemplate, error) {
	templates, err := s.ListGateTemplates(ctx, ListGateTemplatesParams{})
	if err != nil {
		return nil, err
	}

	pending := make([]GateTemplate, 0, len(templates))
	for _, template := range templates {
		if !template.Executable {
			continue
		}
		if actorIsHumanGoverned(template.ApprovedBy) {
			continue
		}
		pending = append(pending, template)
	}
	return pending, nil
}

func (s *Store) InstantiateGateSet(ctx context.Context, p InstantiateGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetCreate {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetInstantiatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if err != nil {
			return GateSet{}, false, err
		}
		if !found {
			if err := applyGateSetInstantiatedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateSet{}, false, err
			}
			gateSet, found, err = gateSetByIDTx(ctx, tx, payload.GateSetID)
			if err != nil {
				return GateSet{}, false, err
			}
			if !found {
				return GateSet{}, false, fmt.Errorf("gate set %q not found after replaying event %s", payload.GateSetID, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, true, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}
	templateRefs, parsedRefs, err := normalizeGateTemplateRefs(p.TemplateRefs)
	if err != nil {
		return GateSet{}, false, err
	}

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
	frozenJSON, _, err := buildFrozenGateDefinition(templateRefs, gates)
	if err != nil {
		return GateSet{}, false, err
	}
	hash := sha256.Sum256([]byte(frozenJSON))
	gateSetHash := hex.EncodeToString(hash[:])
	gateSetID := newID("gset")
	createdAt := nowUTC()

	var frozenObjCopy map[string]any
	if err := json.Unmarshal([]byte(frozenJSON), &frozenObjCopy); err != nil {
		return GateSet{}, false, fmt.Errorf("decode frozen gate definition: %w", err)
	}
	payload := gateSetInstantiatedPayload{
		GateSetID:        gateSetID,
		IssueID:          issueID,
		CycleNo:          cycleNo,
		TemplateRefs:     templateRefs,
		FrozenDefinition: frozenObjCopy,
		GateSetHash:      gateSetHash,
		CreatedAt:        createdAt,
		CreatedBy:        p.Actor,
		Items:            gates,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateSet,
		EntityID:            gateSetID,
		EventType:           eventTypeGateSetCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateSet{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateSetCreate {
		return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if !appendRes.AlreadyExists {
		if err := applyGateSetInstantiatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateSet{}, false, err
		}
	}

	gateSet, found, err := gateSetByIDTx(ctx, tx, gateSetID)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("gate set %q not found after projection", gateSetID)
	}

	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return gateSet, appendRes.AlreadyExists, nil
}

func (s *Store) LockGateSet(ctx context.Context, p LockGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetLockedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if err != nil {
			return GateSet{}, false, err
		}
		if !found {
			if err := applyGateSetLockedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateSet{}, false, err
			}
			gateSet, found, err = gateSetByIDTx(ctx, tx, payload.GateSetID)
			if err != nil {
				return GateSet{}, false, err
			}
			if !found {
				return GateSet{}, false, fmt.Errorf("gate set %q not found after replaying event %s", payload.GateSetID, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, false, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}

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
		payloadBytes, err := json.Marshal(gateSetLockedPayload{
			GateSetID: gateSet.GateSetID,
			IssueID:   issueID,
			CycleNo:   cycleNo,
			LockedAt:  lockTime,
		})
		if err != nil {
			return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
		}
		appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
			EntityType:          entityTypeGateSet,
			EntityID:            gateSet.GateSetID,
			EventType:           eventTypeGateSetLock,
			PayloadJSON:         string(payloadBytes),
			Actor:               p.Actor,
			CommandID:           p.CommandID,
			CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
			EventPayloadVersion: 1,
		})
		if err != nil {
			return GateSet{}, false, err
		}
		if appendRes.Event.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
		}
		if !appendRes.AlreadyExists {
			if err := applyGateSetLockedProjectionTx(ctx, tx, appendRes.Event); err != nil {
				return GateSet{}, false, err
			}
		}
		lockedNow = true
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE work_items
			SET active_gate_set_id = ?, updated_at = ?
			WHERE id = ?
		`, gateSet.GateSetID, nowUTC(), issueID); err != nil {
			return GateSet{}, false, fmt.Errorf("set active gate set for issue %q: %w", issueID, err)
		}
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssueNextResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	agent = strings.TrimSpace(agent)
	focus, focusFound, err := agentFocusByAgentTx(ctx, tx, agent)
	if err != nil {
		return IssueNextResult{}, err
	}
	if !focusFound {
		focus = AgentFocus{}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id, current_cycle_no
		FROM work_items
		WHERE status IN ('Todo', 'InProgress')
		ORDER BY id ASC
	`)
	if err != nil {
		return IssueNextResult{}, fmt.Errorf("query next-issue candidates: %w", err)
	}

	type nextIssueSeed struct {
		Issue          Issue
		CurrentCycleNo int
	}
	seeds := make([]nextIssueSeed, 0)
	candidates := make([]IssueNextCandidate, 0)
	for rows.Next() {
		var (
			issue          Issue
			labelsJSON     string
			referencesJSON string
			currentCycleNo int
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
			&currentCycleNo,
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
		seeds = append(seeds, nextIssueSeed{
			Issue:          issue,
			CurrentCycleNo: currentCycleNo,
		})
	}
	if err := rows.Close(); err != nil {
		return IssueNextResult{}, fmt.Errorf("close next-issue candidate rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return IssueNextResult{}, fmt.Errorf("iterate next-issue candidate rows: %w", err)
	}
	for _, seed := range seeds {
		signals, err := loadIssueNextContinuitySignalsTx(ctx, tx, seed.Issue.ID, seed.Issue.LastEventID, seed.CurrentCycleNo, focus)
		if err != nil {
			return IssueNextResult{}, err
		}
		score, reasons := scoreIssueCandidate(seed.Issue, seed.Issue.Priority, signals)
		candidates = append(candidates, IssueNextCandidate{
			Issue:   seed.Issue,
			Score:   score,
			Reasons: reasons,
		})
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
		Agent:      agent,
		Candidate:  candidates[0],
		Candidates: candidates,
		Considered: len(candidates),
	}
	if err := tx.Commit(); err != nil {
		return IssueNextResult{}, fmt.Errorf("commit tx: %w", err)
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
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		return ReplayResult{}, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_status_projection`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_status_projection: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_set_items`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_set_items: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_sets`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_sets: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_template_approvals`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_template_approvals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_templates`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear gate_templates: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_focus`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear agent_focus: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rehydrate_packets`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear rehydrate_packets: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM issue_summaries`); err != nil {
		return ReplayResult{}, fmt.Errorf("clear issue_summaries: %w", err)
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
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
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
	case eventTypeSessionSummarized:
		return applySessionSummarizedProjectionTx(ctx, tx, event)
	case eventTypeSessionClosed:
		return applySessionClosedProjectionTx(ctx, tx, event)
	case eventTypePacketBuilt:
		return applyPacketBuiltProjectionTx(ctx, tx, event)
	case eventTypeFocusUsed:
		return applyFocusUsedProjectionTx(ctx, tx, event)
	case eventTypeGateTemplateCreate:
		return applyGateTemplateCreatedProjectionTx(ctx, tx, event)
	case eventTypeGateTemplateApprove:
		return applyGateTemplateApprovedProjectionTx(ctx, tx, event)
	case eventTypeGateSetCreate:
		return applyGateSetInstantiatedProjectionTx(ctx, tx, event)
	case eventTypeGateSetLock:
		return applyGateSetLockedProjectionTx(ctx, tx, event)
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
	reopenTransition := false

	if payload.TitleTo != nil {
		titleTo := strings.TrimSpace(*payload.TitleTo)
		if titleTo == "" {
			return fmt.Errorf("decode issue.updated payload for event %s: --title is required", event.EventID)
		}
		setClauses = append(setClauses, "title = ?")
		args = append(args, titleTo)
	}
	if payload.StatusTo != nil {
		issueStatus, err := normalizeIssueStatus(*payload.StatusTo)
		if err != nil {
			return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "status = ?")
		args = append(args, issueStatus)
		if payload.StatusFrom != nil {
			statusFrom, err := normalizeIssueStatus(*payload.StatusFrom)
			if err != nil {
				return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
			}
			reopenTransition = statusFrom == "Done" && issueStatus != "Done"
		}
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
	if reopenTransition {
		setClauses = append(setClauses, "current_cycle_no = current_cycle_no + 1", "active_gate_set_id = NULL")
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
	if reopenTransition {
		if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
			return fmt.Errorf("sync reopened cycle open loops from event %s: %w", event.EventID, err)
		}
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

func applySessionSummarizedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionSummarizedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.summarized payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.summarized context metadata for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE sessions
		SET summary_event_id = ?
		WHERE session_id = ?
	`, event.EventID, payload.SessionID)
	if err != nil {
		return fmt.Errorf("update session summary marker from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check session summary rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update session summary marker from event %s: session %q not found", event.EventID, payload.SessionID)
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
		payload.SummarizedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert summary context chunk from event %s: %w", event.EventID, err)
	}

	return nil
}

func applySessionClosedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload sessionClosedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode session.closed payload for event %s: %w", event.EventID, err)
	}
	contextChunkMetaJSON, err := json.Marshal(payload.ContextChunkMeta)
	if err != nil {
		return fmt.Errorf("encode session.closed context metadata for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE sessions
		SET ended_at = ?, summary_event_id = COALESCE(NULLIF(?, ''), summary_event_id)
		WHERE session_id = ?
	`, payload.EndedAt, payload.SummaryEventID, payload.SessionID)
	if err != nil {
		return fmt.Errorf("update session closure markers from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check session closure rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update session closure markers from event %s: session %q not found", event.EventID, payload.SessionID)
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
		payload.ClosedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert closure context chunk from event %s: %w", event.EventID, err)
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
	scopeID := strings.TrimSpace(anyToString(payload.Packet["scope_id"]))
	issueID := strings.TrimSpace(payload.IssueID)
	sessionID := ""
	if payload.Scope == "issue" && issueID == "" {
		issueID = scopeID
	}
	if payload.Scope == "session" {
		sessionID = scopeID
	}
	issueCycleNo := payload.IssueCycleNo
	if issueCycleNo == 0 {
		if provenanceRaw, ok := payload.Packet["provenance"].(map[string]any); ok {
			issueCycleNo = anyToInt(provenanceRaw["issue_cycle_no"])
		}
		if issueCycleNo == 0 {
			if stateRaw, ok := payload.Packet["state"].(map[string]any); ok {
				issueCycleNo = anyToInt(stateRaw["cycle_no"])
			}
		}
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(
			packet_id, scope, scope_id, issue_id, session_id, issue_cycle_no,
			packet_json, packet_schema_version, built_from_event_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(packet_id) DO UPDATE SET
			scope=excluded.scope,
			scope_id=excluded.scope_id,
			issue_id=excluded.issue_id,
			session_id=excluded.session_id,
			issue_cycle_no=excluded.issue_cycle_no,
			packet_json=excluded.packet_json,
			packet_schema_version=excluded.packet_schema_version,
			built_from_event_id=excluded.built_from_event_id,
			created_at=excluded.created_at
	`, payload.PacketID, payload.Scope, nullIfEmpty(scopeID), nullIfEmpty(issueID), nullIfEmpty(sessionID), nullIfZero(issueCycleNo), string(packetJSON), payload.PacketSchemaVersion, nullIfEmpty(payload.BuiltFromEventID), payload.CreatedAt)
	if err != nil {
		return fmt.Errorf("upsert rehydrate packet from event %s: %w", event.EventID, err)
	}

	if strings.TrimSpace(payload.IssueID) != "" && payload.IssueCycleNo > 0 {
		if err := upsertIssueSummaryForPacketTx(ctx, tx, payload.IssueID, payload.IssueCycleNo, payload.Packet, payload.PacketID, payload.PacketSchemaVersion, payload.CreatedAt); err != nil {
			return fmt.Errorf("upsert issue summary from event %s: %w", event.EventID, err)
		}
	}

	return nil
}

func applyFocusUsedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload focusUsedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode focus.used payload for event %s: %w", event.EventID, err)
	}
	payload.AgentID = strings.TrimSpace(payload.AgentID)
	if payload.AgentID == "" {
		return fmt.Errorf("decode focus.used payload for event %s: agent_id is required", event.EventID)
	}
	payload.LastPacketID = strings.TrimSpace(payload.LastPacketID)
	if payload.LastPacketID == "" {
		return fmt.Errorf("decode focus.used payload for event %s: last_packet_id is required", event.EventID)
	}
	if _, err := packetByIDTx(ctx, tx, payload.LastPacketID); err != nil {
		return fmt.Errorf("validate focus.used payload for event %s: %w", event.EventID, err)
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_focus(agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			active_issue_id=excluded.active_issue_id,
			active_cycle_no=excluded.active_cycle_no,
			last_packet_id=excluded.last_packet_id,
			updated_at=excluded.updated_at
	`, payload.AgentID, nullIfEmpty(payload.ActiveIssueID), nullIfZero(payload.ActiveCycleNo), payload.LastPacketID, payload.FocusedAt)
	if err != nil {
		return fmt.Errorf("upsert agent_focus from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyGateTemplateCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateCreatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.created payload for event %s: %w", event.EventID, err)
	}
	appliesToJSON, err := json.Marshal(payload.AppliesTo)
	if err != nil {
		return fmt.Errorf("encode gate_template.created applies_to for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, payload.TemplateID, payload.Version, string(appliesToJSON), payload.DefinitionJSON, payload.DefinitionHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.DefinitionHash != payload.DefinitionHash ||
			existing.DefinitionJSON != payload.DefinitionJSON ||
			!equalStringSlices(existing.AppliesTo, payload.AppliesTo) {
			return fmt.Errorf("insert gate template from event %s: %w", event.EventID, err)
		}
	}
	if gateDefinitionContainsExecutableCommand(payload.DefinitionJSON) && actorIsHumanGoverned(payload.CreatedBy) {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
			VALUES(?, ?, ?, ?)
			ON CONFLICT(template_id, version) DO NOTHING
		`, payload.TemplateID, payload.Version, payload.CreatedAt, payload.CreatedBy); err != nil {
			return fmt.Errorf("auto-approve gate template from event %s: %w", event.EventID, err)
		}
	}
	return nil
}

func applyGateTemplateApprovedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateApprovedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.approved payload for event %s: %w", event.EventID, err)
	}

	var currentDefinitionHash string
	err = tx.QueryRowContext(ctx, `
		SELECT definition_hash
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, payload.TemplateID, payload.Version).Scan(&currentDefinitionHash)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("approve gate template from event %s: template %s@%d not found", event.EventID, payload.TemplateID, payload.Version)
	}
	if err != nil {
		return fmt.Errorf("lookup gate template for approval from event %s: %w", event.EventID, err)
	}
	if currentDefinitionHash != payload.DefinitionHash {
		return fmt.Errorf("approve gate template from event %s: definition hash mismatch for %s@%d", event.EventID, payload.TemplateID, payload.Version)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(template_id, version) DO NOTHING
	`, payload.TemplateID, payload.Version, payload.ApprovedAt, payload.ApprovedBy); err != nil {
		return fmt.Errorf("approve gate template from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetInstantiatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetInstantiatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition(payload.TemplateRefs, payload.Items)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	if len(payload.FrozenDefinition) > 0 {
		payload.FrozenDefinition = frozenObj
	}
	templateRefsJSON, err := json.Marshal(payload.TemplateRefs)
	if err != nil {
		return fmt.Errorf("encode gate_set.instantiated template refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, payload.GateSetID, payload.IssueID, payload.CycleNo, string(templateRefsJSON), frozenJSON, payload.GateSetHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.IssueID != payload.IssueID ||
			existing.CycleNo != payload.CycleNo ||
			existing.GateSetHash != payload.GateSetHash ||
			!equalStringSlices(existing.TemplateRefs, payload.TemplateRefs) {
			return fmt.Errorf("insert gate set from event %s: %w", event.EventID, err)
		}
	}

	for _, item := range payload.Items {
		criteriaJSON, err := json.Marshal(item.Criteria)
		if err != nil {
			return fmt.Errorf("encode gate_set.instantiated criteria for event %s gate %s: %w", event.EventID, item.GateID, err)
		}
		requiredInt := 0
		if item.Required {
			requiredInt = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(gate_set_id, gate_id) DO NOTHING
		`, payload.GateSetID, item.GateID, item.Kind, requiredInt, string(criteriaJSON)); err != nil {
			return fmt.Errorf("insert gate set item from event %s gate %s: %w", event.EventID, item.GateID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.CreatedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetLockedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetLockedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.locked payload for event %s: %w", event.EventID, err)
	}
	current, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
	if err != nil {
		return fmt.Errorf("lookup gate set from event %s: %w", event.EventID, err)
	}
	if !found {
		return fmt.Errorf("lookup gate set from event %s: gate set %q not found", event.EventID, payload.GateSetID)
	}
	if strings.TrimSpace(current.LockedAt) == "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE gate_sets
			SET locked_at = ?
			WHERE gate_set_id = ?
		`, payload.LockedAt, payload.GateSetID); err != nil {
			return fmt.Errorf("lock gate set from event %s: %w", event.EventID, err)
		}
	} else if current.LockedAt != payload.LockedAt {
		return fmt.Errorf("lock gate set from event %s: gate set %q already locked at %s", event.EventID, payload.GateSetID, current.LockedAt)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.LockedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from lock event %s: %w", event.EventID, err)
	}
	return nil
}

func dropReplayProjectionDeleteTriggersTx(ctx context.Context, tx *sql.Tx) error {
	for _, triggerName := range []string{
		"gate_templates_no_delete",
		"gate_sets_no_delete",
		"gate_set_items_no_delete",
	} {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+triggerName); err != nil {
			return fmt.Errorf("drop replay delete trigger %s: %w", triggerName, err)
		}
	}
	return nil
}

func restoreReplayProjectionDeleteTriggersTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TRIGGER IF NOT EXISTS gate_templates_no_delete
			BEFORE DELETE ON gate_templates
		BEGIN
			SELECT RAISE(ABORT, 'gate_templates are immutable');
		END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_sets_no_delete
			BEFORE DELETE ON gate_sets
		BEGIN
			SELECT RAISE(ABORT, 'gate_sets are immutable');
		END;`,
		`CREATE TRIGGER IF NOT EXISTS gate_set_items_no_delete
			BEFORE DELETE ON gate_set_items
		BEGIN
			SELECT RAISE(ABORT, 'gate_set_items are immutable');
		END;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore replay delete trigger: %w", err)
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

func validateExecutableGateVerificationGovernanceTx(ctx context.Context, tx *sql.Tx, gateSet GateSet, gateID, command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}

	if len(gateSet.TemplateRefs) == 0 {
		if actorIsHumanGoverned(gateSet.CreatedBy) {
			return nil
		}
		return fmt.Errorf("gate %q in gate_set %q has executable criteria.command without approved template provenance", gateID, gateSet.GateSetID)
	}

	sourceTemplate := ""
	sourceCommand := ""
	sourceApprovedBy := ""
	for _, rawRef := range gateSet.TemplateRefs {
		ref, err := parseGateTemplateRef(rawRef)
		if err != nil {
			return fmt.Errorf("validate executable gate governance for %q: %w", gateID, err)
		}

		var (
			definitionJSON string
			approvedBy     string
		)
		err = tx.QueryRowContext(ctx, `
			SELECT t.definition_json, COALESCE(a.approved_by, '')
			FROM gate_templates AS t
			LEFT JOIN gate_template_approvals AS a
				ON a.template_id = t.template_id
				AND a.version = t.version
			WHERE t.template_id = ? AND t.version = ?
		`, ref.TemplateID, ref.Version).Scan(&definitionJSON, &approvedBy)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("gate %q in gate_set %q references missing template %s", gateID, gateSet.GateSetID, ref.Ref)
		}
		if err != nil {
			return fmt.Errorf("lookup template provenance for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
		}

		defs, err := extractGateDefinitions(definitionJSON)
		if err != nil {
			return fmt.Errorf("decode template provenance for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
		}
		for _, def := range defs {
			if def.GateID != gateID {
				continue
			}
			if templateCommand := gateCriteriaCommand(def.Criteria); templateCommand != "" {
				sourceTemplate = ref.Ref
				sourceCommand = templateCommand
				sourceApprovedBy = approvedBy
			}
		}
	}

	if sourceCommand == command && actorIsHumanGoverned(sourceApprovedBy) {
		return nil
	}
	if sourceCommand == "" {
		if actorIsHumanGoverned(gateSet.CreatedBy) {
			return nil
		}
		return fmt.Errorf("gate %q in gate_set %q has executable criteria.command without approved template provenance", gateID, gateSet.GateSetID)
	}
	if sourceCommand != command {
		return fmt.Errorf("gate %q in gate_set %q command does not match approved template provenance", gateID, gateSet.GateSetID)
	}
	return fmt.Errorf("gate %q in gate_set %q uses executable criteria.command from unapproved template %s", gateID, gateSet.GateSetID, sourceTemplate)
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
			COALESCE(gs.evidence_refs_json, '[]'),
			COALESCE(gs.last_event_id, '')
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
			lastEventID  string
			evidenceRefs []string
		)
		if err := rows.Scan(&gateID, &requiredInt, &result, &evidenceJSON, &lastEventID); err != nil {
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
			"last_event_id": lastEventID,
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
	rows, err := tx.QueryContext(ctx, `
		SELECT loop_id, status, COALESCE(source_event_id, '')
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND loop_type = 'gate'
	`, issueID, cycleNo)
	if err != nil {
		return nil, fmt.Errorf("query existing gate loops for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	type existingLoopState struct {
		Status        string
		SourceEventID string
	}
	existing := make(map[string]existingLoopState)
	for rows.Next() {
		var (
			loopID      string
			status      string
			loopEventID string
		)
		if err := rows.Scan(&loopID, &status, &loopEventID); err != nil {
			return nil, fmt.Errorf("scan existing gate loop row for issue %q: %w", issueID, err)
		}
		existing[loopID] = existingLoopState{
			Status:        status,
			SourceEventID: loopEventID,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing gate loops for issue %q: %w", issueID, err)
	}

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
		normalizedResult := strings.ToUpper(strings.TrimSpace(result))
		if normalizedResult == "PASS" {
			continue
		}
		loopID := deterministicLoopID(issueID, cycleNo, "gate", gateID)
		loopEventID, _ := gateMap["last_event_id"].(string)
		loopEventID = strings.TrimSpace(loopEventID)
		if loopEventID == "" {
			if existingState, ok := existing[loopID]; ok {
				loopEventID = strings.TrimSpace(existingState.SourceEventID)
			}
		}
		if loopEventID == "" && normalizedResult != "MISSING" {
			loopEventID = strings.TrimSpace(sourceEventID)
		}
		expectedOpen[loopID] = OpenLoop{
			LoopID:        loopID,
			IssueID:       issueID,
			CycleNo:       cycleNo,
			LoopType:      "gate",
			Status:        "Open",
			Priority:      "P1",
			SourceEventID: loopEventID,
			UpdatedAt:     now,
		}
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

	for loopID, state := range existing {
		if _, stillOpen := expectedOpen[loopID]; stillOpen {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(state.Status), "Resolved") {
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

func upsertIssueSummaryForPacketTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int, packet map[string]any, packetID string, packetSchemaVersion int, createdAt string) error {
	var maxSeq int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(entity_seq), 0)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, entityTypeIssue, issueID).Scan(&maxSeq); err != nil {
		return fmt.Errorf("query max entity_seq for issue %q summary: %w", issueID, err)
	}

	summaryJSON, err := buildPacketSummaryJSON(packet, packetID, packetSchemaVersion)
	if err != nil {
		return fmt.Errorf("build packet summary for issue %q: %w", issueID, err)
	}
	parentSummaryID, err := latestPacketSummaryIDForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return err
	}
	summaryID := "sum_" + strings.TrimSpace(packetID)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO issue_summaries(
			summary_id, issue_id, cycle_no, summary_level, summary_json,
			from_entity_seq, to_entity_seq, parent_summary_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		nullIfEmpty(parentSummaryID),
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert issue summary for issue %q: %w", issueID, err)
	}
	return nil
}

func buildPacketSummaryJSON(packet map[string]any, packetID string, packetSchemaVersion int) (string, error) {
	summary := map[string]any{
		"packet_id":             packetID,
		"packet_schema_version": packetSchemaVersion,
	}
	for _, key := range []string{
		"scope",
		"scope_id",
		"goal",
		"state",
		"decision_summary",
		"open_questions",
		"linked_work_items",
		"gates",
		"open_loops",
		"next_actions",
		"risks",
		"continuity",
		"provenance",
	} {
		if value, ok := packet[key]; ok {
			summary[key] = value
		}
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func latestPacketSummaryIDForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) (string, error) {
	var summaryID sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT summary_id
		FROM issue_summaries
		WHERE issue_id = ?
			AND cycle_no = ?
			AND summary_level = 'packet'
		ORDER BY to_entity_seq DESC, created_at DESC, summary_id DESC
		LIMIT 1
	`, issueID, cycleNo).Scan(&summaryID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query latest packet summary for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	if !summaryID.Valid {
		return "", nil
	}
	return summaryID.String, nil
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

func buildIssueDecisionSummary(issue Issue, cycleNo int, gates []any, openLoops []OpenLoop, linkedWorkItems []any) map[string]any {
	gateCounts := map[string]any{
		"pass":                 0,
		"fail":                 0,
		"blocked":              0,
		"missing":              0,
		"required_outstanding": 0,
	}
	closeReady := true
	for _, rawGate := range gates {
		gate, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		result := strings.ToUpper(strings.TrimSpace(anyToString(gate["result"])))
		required, _ := gate["required"].(bool)
		switch result {
		case "PASS":
			gateCounts["pass"] = anyToInt(gateCounts["pass"]) + 1
		case "FAIL":
			gateCounts["fail"] = anyToInt(gateCounts["fail"]) + 1
		case "BLOCKED":
			gateCounts["blocked"] = anyToInt(gateCounts["blocked"]) + 1
		default:
			gateCounts["missing"] = anyToInt(gateCounts["missing"]) + 1
		}
		if required && result != "PASS" {
			gateCounts["required_outstanding"] = anyToInt(gateCounts["required_outstanding"]) + 1
			closeReady = false
		}
	}
	openLoopCount := countOpenLoops(openLoops)
	if openLoopCount > 0 {
		closeReady = false
	}

	summary := map[string]any{
		"issue_id":               issue.ID,
		"issue_type":             issue.Type,
		"status":                 issue.Status,
		"cycle_no":               cycleNo,
		"latest_event_id":        issue.LastEventID,
		"gate_counts":            gateCounts,
		"open_loop_count":        openLoopCount,
		"linked_work_item_count": len(linkedWorkItems),
		"close_ready":            closeReady,
	}
	if strings.TrimSpace(issue.ParentID) != "" {
		summary["parent_id"] = issue.ParentID
	}
	return summary
}

func buildIssueOpenQuestions(gates []any, openLoops []OpenLoop) []any {
	questions := make([]any, 0)
	for _, rawGate := range gates {
		gate, ok := rawGate.(map[string]any)
		if !ok {
			continue
		}
		result := strings.ToUpper(strings.TrimSpace(anyToString(gate["result"])))
		required, _ := gate["required"].(bool)
		if !required || result == "PASS" {
			continue
		}
		gateID := strings.TrimSpace(anyToString(gate["gate_id"]))
		if gateID == "" {
			continue
		}
		questions = append(questions, map[string]any{
			"kind":     "gate",
			"gate_id":  gateID,
			"status":   result,
			"question": fmt.Sprintf("What is still needed to resolve required gate %s?", gateID),
		})
	}
	for _, loop := range openLoops {
		if !strings.EqualFold(strings.TrimSpace(loop.Status), "Open") {
			continue
		}
		questions = append(questions, map[string]any{
			"kind":       "open_loop",
			"loop_id":    loop.LoopID,
			"loop_type":  loop.LoopType,
			"owner":      loop.Owner,
			"priority":   loop.Priority,
			"updated_at": loop.UpdatedAt,
			"question":   fmt.Sprintf("What closes the %s loop for this issue cycle?", loop.LoopType),
		})
	}
	return questions
}

func buildCompactionPolicy(scope string, eventCount, openLoopCount, contextChunkCount int) map[string]any {
	reasons := make([]any, 0, 3)
	if eventCount >= compactionEventThreshold {
		reasons = append(reasons, "event-threshold")
	}
	if openLoopCount >= compactionOpenLoopThreshold {
		reasons = append(reasons, "open-loop-threshold")
	}
	if contextChunkCount >= compactionContextChunkThreshold {
		reasons = append(reasons, "context-chunk-threshold")
	}
	return map[string]any{
		"policy_version": compactionPolicyVersion,
		"mode":           compactionPolicyMode,
		"build_reason":   compactionPolicyBuildReasonOnDemand,
		"scope":          scope,
		"triggered":      len(reasons) > 0,
		"reasons":        reasons,
		"thresholds": map[string]any{
			"event_count":    compactionEventThreshold,
			"open_loops":     compactionOpenLoopThreshold,
			"context_chunks": compactionContextChunkThreshold,
		},
		"observed": map[string]any{
			"event_count":    eventCount,
			"open_loops":     openLoopCount,
			"context_chunks": contextChunkCount,
		},
	}
}

func listLinkedWorkItemsForIssueTx(ctx context.Context, tx *sql.Tx, issue Issue) ([]any, error) {
	items := make([]any, 0)
	if strings.TrimSpace(issue.ParentID) != "" {
		parent, err := getIssueTx(ctx, tx, issue.ParentID)
		if err != nil {
			return nil, err
		}
		items = append(items, issueToLinkedWorkItem(parent, "parent"))
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, type, title, status
		FROM work_items
		WHERE parent_id = ?
			AND status NOT IN ('Done', 'WontDo')
		ORDER BY id ASC
	`, issue.ID)
	if err != nil {
		return nil, fmt.Errorf("query child work items for %q: %w", issue.ID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var childID, childType, title, status string
		if err := rows.Scan(&childID, &childType, &title, &status); err != nil {
			return nil, fmt.Errorf("scan child work item for %q: %w", issue.ID, err)
		}
		items = append(items, map[string]any{
			"relation": "child",
			"issue_id": childID,
			"type":     childType,
			"title":    title,
			"status":   status,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate child work items for %q: %w", issue.ID, err)
	}
	return items, nil
}

func issueToLinkedWorkItem(issue Issue, relation string) map[string]any {
	item := map[string]any{
		"relation": relation,
		"issue_id": issue.ID,
		"type":     issue.Type,
		"title":    issue.Title,
		"status":   issue.Status,
	}
	if strings.TrimSpace(issue.ParentID) != "" {
		item["parent_id"] = issue.ParentID
	}
	return item
}

func listRelevantContextChunksTx(ctx context.Context, tx *sql.Tx, scope, scopeID string, limit int) ([]any, int, error) {
	if limit <= 0 {
		limit = packetRelevantChunkLimit
	}

	var (
		countQuery string
		listQuery  string
		countArgs  []any
		listArgs   []any
	)
	switch scope {
	case "session":
		countQuery = `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`
		listQuery = `
			SELECT chunk_id, kind, content, metadata_json, created_at
			FROM context_chunks
			WHERE session_id = ?
			ORDER BY created_at DESC, chunk_id DESC
			LIMIT ?
		`
		countArgs = []any{scopeID}
		listArgs = []any{scopeID, limit}
	case "issue":
		countQuery = `SELECT COUNT(1) FROM context_chunks WHERE entity_type = ? AND entity_id = ?`
		listQuery = `
			SELECT chunk_id, kind, content, metadata_json, created_at
			FROM context_chunks
			WHERE entity_type = ? AND entity_id = ?
			ORDER BY created_at DESC, chunk_id DESC
			LIMIT ?
		`
		countArgs = []any{entityTypeIssue, scopeID}
		listArgs = []any{entityTypeIssue, scopeID, limit}
	default:
		return []any{}, 0, nil
	}

	var total int
	if err := tx.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count context chunks for %s %q: %w", scope, scopeID, err)
	}

	rows, err := tx.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query context chunks for %s %q: %w", scope, scopeID, err)
	}
	defer rows.Close()

	chunks := make([]map[string]any, 0)
	for rows.Next() {
		var chunkID, kind, content, metadataJSON, createdAt string
		if err := rows.Scan(&chunkID, &kind, &content, &metadataJSON, &createdAt); err != nil {
			return nil, 0, fmt.Errorf("scan context chunk for %s %q: %w", scope, scopeID, err)
		}
		metadata := make(map[string]any)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return nil, 0, fmt.Errorf("decode context chunk metadata for %s %q: %w", scope, scopeID, err)
		}
		chunks = append(chunks, map[string]any{
			"chunk_id":   chunkID,
			"kind":       kind,
			"content":    content,
			"metadata":   metadata,
			"created_at": createdAt,
			"relevance":  "recent",
			"scope":      scope,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate context chunks for %s %q: %w", scope, scopeID, err)
	}

	items := make([]any, 0, len(chunks))
	for i := len(chunks) - 1; i >= 0; i-- {
		items = append(items, chunks[i])
	}
	return items, total, nil
}

func countEventsForEntityTx(ctx context.Context, tx *sql.Tx, entityType, entityID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM events
		WHERE entity_type = ? AND entity_id = ?
	`, entityType, entityID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count events for %s %q: %w", entityType, entityID, err)
	}
	return count, nil
}

func countOpenLoops(loops []OpenLoop) int {
	count := 0
	for _, loop := range loops {
		if strings.EqualFold(strings.TrimSpace(loop.Status), "Open") {
			count++
		}
	}
	return count
}

func deterministicLoopID(issueID string, cycleNo int, loopType, key string) string {
	sum := sha256.Sum256([]byte(issueID + ":" + strconv.Itoa(cycleNo) + ":" + loopType + ":" + key))
	return "loop_" + hex.EncodeToString(sum[:])[:12]
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

func loadIssueNextContinuitySignalsTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issueLastEventID string,
	currentCycleNo int,
	focus AgentFocus,
) (issueNextContinuitySignals, error) {
	signals := issueNextContinuitySignals{
		CurrentCycleNo: currentCycleNo,
	}
	if focus.ActiveIssueID == issueID && (focus.ActiveCycleNo == 0 || focus.ActiveCycleNo == currentCycleNo) {
		signals.FocusMatch = true
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND status = 'Open'
	`, issueID, currentCycleNo).Scan(&signals.OpenLoopCount); err != nil {
		return issueNextContinuitySignals{}, fmt.Errorf("query open loop count for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}

	lockedGateSet, found, err := lockedGateSetForIssueCycleTx(ctx, tx, issueID, currentCycleNo)
	if err != nil {
		return issueNextContinuitySignals{}, err
	}
	if found {
		rows, err := tx.QueryContext(ctx, `
			SELECT COALESCE(gs.result, '')
			FROM gate_set_items i
			LEFT JOIN gate_status_projection gs
				ON gs.issue_id = ?
				AND gs.gate_set_id = i.gate_set_id
				AND gs.gate_id = i.gate_id
			WHERE i.gate_set_id = ?
				AND i.required = 1
		`, issueID, lockedGateSet.GateSetID)
		if err != nil {
			return issueNextContinuitySignals{}, fmt.Errorf("query gate health for issue %q: %w", issueID, err)
		}
		defer rows.Close()

		for rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				return issueNextContinuitySignals{}, fmt.Errorf("scan gate health for issue %q: %w", issueID, err)
			}
			switch strings.TrimSpace(result) {
			case "FAIL":
				signals.FailingRequiredGates++
			case "BLOCKED":
				signals.BlockedRequiredGates++
			case "", "MISSING":
				signals.MissingRequiredGates++
			}
		}
		if err := rows.Err(); err != nil {
			return issueNextContinuitySignals{}, fmt.Errorf("iterate gate health for issue %q: %w", issueID, err)
		}
	}

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "issue", issueID)
	if err != nil {
		return issueNextContinuitySignals{}, err
	}
	if found {
		packetCycleNo := issuePacketCycleNo(packet)
		if focus.LastPacketID != "" && focus.LastPacketID == packet.PacketID {
			signals.FocusPacketMatch = true
		}
		packetEventOrder, packetEventFound, err := eventOrderByIDTx(ctx, tx, packet.BuiltFromEventID)
		if err != nil {
			return issueNextContinuitySignals{}, err
		}
		latestContinuityOrder, err := latestContinuityEventOrderForIssueCycleTx(ctx, tx, issueID, issueLastEventID, currentCycleNo)
		if err != nil {
			return issueNextContinuitySignals{}, err
		}
		if packetCycleNo == currentCycleNo && ((packetEventFound && packetEventOrder >= latestContinuityOrder) || (!packetEventFound && latestContinuityOrder == 0)) {
			signals.HasFreshPacket = true
		} else {
			signals.HasStalePacket = true
		}
	}

	return signals, nil
}

func issuePacketCycleNo(packet RehydratePacket) int {
	if packet.IssueCycleNo > 0 {
		return packet.IssueCycleNo
	}
	if packet.Packet == nil {
		return 0
	}
	if provenanceRaw, ok := packet.Packet["provenance"].(map[string]any); ok {
		if cycleNo := anyToInt(provenanceRaw["issue_cycle_no"]); cycleNo > 0 {
			return cycleNo
		}
	}
	if stateRaw, ok := packet.Packet["state"].(map[string]any); ok {
		return anyToInt(stateRaw["cycle_no"])
	}
	return 0
}

func anyToInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if v, err := typed.Int64(); err == nil {
			return int(v)
		}
	case string:
		if v, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			return v
		}
	}
	return 0
}

func anyToString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", value)
	}
}

func scoreIssueCandidate(issue Issue, priority string, signals issueNextContinuitySignals) (int, []string) {
	score := 0
	reasons := make([]string, 0, 8)

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

	if signals.FocusMatch {
		score += 120
		reasons = append(reasons, "matches the agent's active focus for resume")
	}
	if signals.FocusPacketMatch {
		score += 35
		reasons = append(reasons, "agent already holds the latest recovery packet")
	}
	if signals.OpenLoopCount > 0 {
		boost := signals.OpenLoopCount * 15
		if boost > 45 {
			boost = 45
		}
		score += boost
		reasons = append(reasons, fmt.Sprintf("has %d open loop(s) that need continuity", signals.OpenLoopCount))
	}
	if signals.FailingRequiredGates > 0 {
		score += 40
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) are failing", signals.FailingRequiredGates))
	}
	if signals.BlockedRequiredGates > 0 {
		score += 30
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) are blocked", signals.BlockedRequiredGates))
	}
	if signals.MissingRequiredGates > 0 {
		score += 15
		reasons = append(reasons, fmt.Sprintf("%d required gate(s) still need evaluation", signals.MissingRequiredGates))
	}
	if signals.HasFreshPacket {
		score += 20
		reasons = append(reasons, "fresh issue packet is ready for recovery")
	} else if signals.HasStalePacket {
		score -= 5
		reasons = append(reasons, "available issue packet is stale")
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
