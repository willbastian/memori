-- +goose Up
-- +goose NO TRANSACTION

PRAGMA foreign_keys=OFF;

CREATE TABLE work_items_new (
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
    last_event_id TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    acceptance_criteria TEXT NOT NULL DEFAULT '',
    references_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(references_json))
);

INSERT INTO work_items_new(
    id, type, title, parent_id, status, priority, labels_json,
    current_cycle_no, active_gate_set_id, created_at, updated_at,
    last_event_id, description, acceptance_criteria, references_json
)
SELECT
    id, type, title, parent_id, status, priority, labels_json,
    current_cycle_no, active_gate_set_id, created_at, updated_at,
    last_event_id, description, acceptance_criteria, references_json
FROM work_items;

DROP TABLE work_items;

ALTER TABLE work_items_new RENAME TO work_items;

CREATE INDEX idx_work_items_type_status ON work_items(type, status);
CREATE INDEX idx_work_items_parent ON work_items(parent_id);

PRAGMA foreign_keys=ON;

-- +goose Down
SELECT 1;
