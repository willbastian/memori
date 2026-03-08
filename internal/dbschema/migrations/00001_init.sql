-- +goose Up

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
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
);

CREATE INDEX IF NOT EXISTS idx_events_entity_time ON events(entity_type, entity_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_events_correlation_time ON events(correlation_id, created_at);

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS events_no_update
    BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS events_no_delete
    BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS work_items (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('Epic','Story','Task','Bug')),
    title TEXT NOT NULL,
    parent_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('Todo','InProgress','Blocked','Done','WontDo')),
    priority TEXT,
    labels_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(labels_json)),
    current_cycle_no INTEGER NOT NULL DEFAULT 1,
    active_gate_set_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_work_items_type_status ON work_items(type, status);
CREATE INDEX IF NOT EXISTS idx_work_items_parent ON work_items(parent_id);

CREATE TABLE IF NOT EXISTS gate_templates (
    template_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    applies_to_json TEXT NOT NULL CHECK(json_valid(applies_to_json)),
    definition_json TEXT NOT NULL CHECK(json_valid(definition_json)),
    definition_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    PRIMARY KEY(template_id, version),
    UNIQUE(definition_hash)
);

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_templates_no_update
    BEFORE UPDATE ON gate_templates
BEGIN
    SELECT RAISE(ABORT, 'gate_templates are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_templates_no_delete
    BEFORE DELETE ON gate_templates
BEGIN
    SELECT RAISE(ABORT, 'gate_templates are immutable');
END;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS gate_sets (
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
);

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_sets_no_delete
    BEFORE DELETE ON gate_sets
BEGIN
    SELECT RAISE(ABORT, 'gate_sets are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_sets_frozen_definition_no_update
    BEFORE UPDATE ON gate_sets
    WHEN NEW.template_refs_json IS NOT OLD.template_refs_json
        OR NEW.frozen_definition_json IS NOT OLD.frozen_definition_json
        OR NEW.gate_set_hash IS NOT OLD.gate_set_hash
BEGIN
    SELECT RAISE(ABORT, 'gate_set definitions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_sets_lock_noop_rejected
    BEFORE UPDATE OF locked_at ON gate_sets
    WHEN OLD.locked_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'gate_set is already locked');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_sets_locked_row_no_update
    BEFORE UPDATE ON gate_sets
    WHEN OLD.locked_at IS NOT NULL
        AND NEW.locked_at IS OLD.locked_at
BEGIN
    SELECT RAISE(ABORT, 'locked gate_sets are immutable');
END;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS gate_set_items (
    gate_set_id TEXT NOT NULL,
    gate_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    required INTEGER NOT NULL CHECK(required IN (0,1)),
    criteria_json TEXT NOT NULL CHECK(json_valid(criteria_json)),
    PRIMARY KEY(gate_set_id, gate_id),
    FOREIGN KEY(gate_set_id) REFERENCES gate_sets(gate_set_id)
);

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_set_items_no_update
    BEFORE UPDATE ON gate_set_items
BEGIN
    SELECT RAISE(ABORT, 'gate_set_items are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER IF NOT EXISTS gate_set_items_no_delete
    BEFORE DELETE ON gate_set_items
BEGIN
    SELECT RAISE(ABORT, 'gate_set_items are immutable');
END;
-- +goose StatementEnd

-- +goose Down
SELECT 1;
