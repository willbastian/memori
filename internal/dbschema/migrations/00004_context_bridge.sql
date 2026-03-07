-- +goose Up

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    trigger TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    summary_event_id TEXT,
    checkpoint_json TEXT CHECK(checkpoint_json IS NULL OR json_valid(checkpoint_json)),
    created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rehydrate_packets (
    packet_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL CHECK(scope IN ('issue','session')),
    packet_json TEXT NOT NULL CHECK(json_valid(packet_json)),
    packet_schema_version INTEGER NOT NULL CHECK(packet_schema_version > 0),
    built_from_event_id TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rehydrate_packets_scope_time ON rehydrate_packets(scope, created_at);

CREATE TABLE IF NOT EXISTS agent_focus (
    agent_id TEXT PRIMARY KEY,
    active_issue_id TEXT,
    active_cycle_no INTEGER CHECK(active_cycle_no IS NULL OR active_cycle_no > 0),
    last_packet_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(last_packet_id) REFERENCES rehydrate_packets(packet_id)
);

-- +goose Down
SELECT 1;
