-- +goose Up

CREATE TABLE IF NOT EXISTS gate_status_projection (
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

CREATE INDEX IF NOT EXISTS idx_gate_status_projection_issue
    ON gate_status_projection(issue_id, gate_set_id);

-- +goose Down
SELECT 1;
