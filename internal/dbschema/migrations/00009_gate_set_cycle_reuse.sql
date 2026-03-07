-- +goose Up

PRAGMA foreign_keys=OFF;

DROP TRIGGER IF EXISTS gate_sets_no_delete;
DROP TRIGGER IF EXISTS gate_sets_frozen_definition_no_update;
DROP TRIGGER IF EXISTS gate_sets_lock_noop_rejected;
DROP TRIGGER IF EXISTS gate_sets_locked_row_no_update;
DROP TRIGGER IF EXISTS gate_set_items_no_update;
DROP TRIGGER IF EXISTS gate_set_items_no_delete;

ALTER TABLE gate_status_projection RENAME TO gate_status_projection_old;
ALTER TABLE gate_set_items RENAME TO gate_set_items_old;
ALTER TABLE gate_sets RENAME TO gate_sets_old;

CREATE TABLE gate_sets (
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
    FOREIGN KEY(issue_id) REFERENCES work_items(id)
);

-- +goose StatementBegin
CREATE TRIGGER gate_sets_no_delete
    BEFORE DELETE ON gate_sets
BEGIN
    SELECT RAISE(ABORT, 'gate_sets are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER gate_sets_frozen_definition_no_update
    BEFORE UPDATE ON gate_sets
    WHEN NEW.template_refs_json IS NOT OLD.template_refs_json
        OR NEW.frozen_definition_json IS NOT OLD.frozen_definition_json
        OR NEW.gate_set_hash IS NOT OLD.gate_set_hash
BEGIN
    SELECT RAISE(ABORT, 'gate_set definitions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER gate_sets_lock_noop_rejected
    BEFORE UPDATE OF locked_at ON gate_sets
    WHEN OLD.locked_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'gate_set is already locked');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER gate_sets_locked_row_no_update
    BEFORE UPDATE ON gate_sets
    WHEN OLD.locked_at IS NOT NULL
        AND NEW.locked_at IS OLD.locked_at
BEGIN
    SELECT RAISE(ABORT, 'locked gate_sets are immutable');
END;
-- +goose StatementEnd

INSERT INTO gate_sets(
    gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
    gate_set_hash, locked_at, created_at, created_by
)
SELECT
    gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
    gate_set_hash, locked_at, created_at, created_by
FROM gate_sets_old;

CREATE TABLE gate_set_items (
    gate_set_id TEXT NOT NULL,
    gate_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    required INTEGER NOT NULL CHECK(required IN (0,1)),
    criteria_json TEXT NOT NULL CHECK(json_valid(criteria_json)),
    PRIMARY KEY(gate_set_id, gate_id),
    FOREIGN KEY(gate_set_id) REFERENCES gate_sets(gate_set_id)
);

-- +goose StatementBegin
CREATE TRIGGER gate_set_items_no_update
    BEFORE UPDATE ON gate_set_items
BEGIN
    SELECT RAISE(ABORT, 'gate_set_items are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER gate_set_items_no_delete
    BEFORE DELETE ON gate_set_items
BEGIN
    SELECT RAISE(ABORT, 'gate_set_items are immutable');
END;
-- +goose StatementEnd

INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
SELECT gate_set_id, gate_id, kind, required, criteria_json
FROM gate_set_items_old;

CREATE TABLE gate_status_projection (
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
);

INSERT INTO gate_status_projection(
    issue_id, gate_set_id, gate_id, result, evidence_refs_json,
    evaluated_at, updated_at, last_event_id
)
SELECT
    issue_id, gate_set_id, gate_id, result, evidence_refs_json,
    evaluated_at, updated_at, last_event_id
FROM gate_status_projection_old;

DROP TABLE gate_status_projection_old;
DROP TABLE gate_set_items_old;
DROP TABLE gate_sets_old;

PRAGMA foreign_keys=ON;

-- +goose Down
SELECT 1;
