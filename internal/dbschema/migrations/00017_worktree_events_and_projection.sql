-- +goose Up

ALTER TABLE events RENAME TO events_old;

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    event_order INTEGER NOT NULL CHECK(event_order > 0),
    entity_type TEXT NOT NULL CHECK(entity_type IN ('issue','session','packet','focus','worktree','gate_template','gate_set')),
    entity_id TEXT NOT NULL,
    entity_seq INTEGER NOT NULL CHECK(entity_seq > 0),
    event_type TEXT NOT NULL CHECK(event_type IN (
        'issue.created',
        'issue.updated',
        'issue.linked',
        'gate.evaluated',
        'session.checkpointed',
        'session.summarized',
        'session.closed',
        'packet.built',
        'focus.used',
        'worktree.registered',
        'worktree.attached',
        'worktree.detached',
        'worktree.archived',
        'gate_template.created',
        'gate_template.approved',
        'gate_set.instantiated',
        'gate_set.locked'
    )),
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
);

INSERT INTO events(
    event_id, event_order, entity_type, entity_id, entity_seq,
    event_type, payload_json, actor, command_id, causation_id,
    correlation_id, created_at, hash, prev_hash, event_payload_version
)
SELECT
    event_id, event_order, entity_type, entity_id, entity_seq,
    event_type, payload_json, actor, command_id, causation_id,
    correlation_id, created_at, hash, prev_hash, event_payload_version
FROM events_old;

DROP TABLE events_old;

CREATE INDEX idx_events_entity_time ON events(entity_type, entity_id, created_at);
CREATE INDEX idx_events_type_time ON events(event_type, created_at);
CREATE INDEX idx_events_correlation_time ON events(correlation_id, created_at);

-- +goose StatementBegin
CREATE TRIGGER events_no_update
    BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER events_no_delete
    BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

CREATE TABLE worktrees (
    worktree_id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    repo_root TEXT NOT NULL,
    branch TEXT,
    head_oid TEXT,
    issue_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('Active','Archived')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    FOREIGN KEY(issue_id) REFERENCES work_items(id)
);

CREATE INDEX idx_worktrees_issue_status_time ON worktrees(issue_id, status, updated_at);
CREATE INDEX idx_worktrees_repo_root ON worktrees(repo_root);

-- +goose Down
SELECT 1;
