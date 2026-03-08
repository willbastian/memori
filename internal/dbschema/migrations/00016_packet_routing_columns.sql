-- +goose Up

ALTER TABLE rehydrate_packets ADD COLUMN scope_id TEXT;
ALTER TABLE rehydrate_packets ADD COLUMN issue_id TEXT;
ALTER TABLE rehydrate_packets ADD COLUMN session_id TEXT;
ALTER TABLE rehydrate_packets ADD COLUMN issue_cycle_no INTEGER;

UPDATE rehydrate_packets
SET
    scope_id = COALESCE(scope_id, json_extract(packet_json, '$.scope_id')),
    issue_id = COALESCE(
        issue_id,
        CASE
            WHEN scope = 'issue' THEN json_extract(packet_json, '$.scope_id')
            ELSE json_extract(packet_json, '$.provenance.issue_id')
        END
    ),
    session_id = COALESCE(
        session_id,
        CASE
            WHEN scope = 'session' THEN json_extract(packet_json, '$.scope_id')
            ELSE NULL
        END
    ),
    issue_cycle_no = COALESCE(
        issue_cycle_no,
        CAST(json_extract(packet_json, '$.provenance.issue_cycle_no') AS INTEGER),
        CAST(json_extract(packet_json, '$.state.cycle_no') AS INTEGER)
    );

CREATE INDEX IF NOT EXISTS idx_rehydrate_packets_scope_scope_id_time ON rehydrate_packets(scope, scope_id, created_at);
CREATE INDEX IF NOT EXISTS idx_rehydrate_packets_issue_cycle_time ON rehydrate_packets(issue_id, issue_cycle_no, created_at);
CREATE INDEX IF NOT EXISTS idx_rehydrate_packets_session_time ON rehydrate_packets(session_id, created_at);

-- +goose Down
SELECT 1;
