package store

import (
	"context"
	"database/sql"
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
	packetSchemaVersion                 = 3
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
	entityTypeWorktree     = "worktree"
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
	eventTypeWorktreeRegistered  = "worktree.registered"
	eventTypeWorktreeAttached    = "worktree.attached"
	eventTypeWorktreeDetached    = "worktree.detached"
	eventTypeWorktreeArchived    = "worktree.archived"
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
