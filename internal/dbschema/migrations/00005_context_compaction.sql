-- +goose Up

CREATE TABLE IF NOT EXISTS context_chunks (
    chunk_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    kind TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json)),
    embedding_ref TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_context_chunks_session_time ON context_chunks(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_context_chunks_entity_time ON context_chunks(entity_type, entity_id, created_at);

CREATE TABLE IF NOT EXISTS issue_summaries (
    summary_id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL CHECK(cycle_no > 0),
    summary_level TEXT NOT NULL,
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
    from_entity_seq INTEGER NOT NULL CHECK(from_entity_seq >= 0),
    to_entity_seq INTEGER NOT NULL CHECK(to_entity_seq >= from_entity_seq),
    parent_summary_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(issue_id) REFERENCES work_items(id),
    FOREIGN KEY(parent_summary_id) REFERENCES issue_summaries(summary_id)
);

CREATE INDEX IF NOT EXISTS idx_issue_summaries_issue_cycle_time ON issue_summaries(issue_id, cycle_no, created_at);

CREATE TABLE IF NOT EXISTS open_loops (
    loop_id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL CHECK(cycle_no > 0),
    loop_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('Open','Resolved')),
    owner TEXT,
    priority TEXT,
    source_event_id TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(issue_id) REFERENCES work_items(id)
);

CREATE INDEX IF NOT EXISTS idx_open_loops_issue_cycle_status ON open_loops(issue_id, cycle_no, status, updated_at);

-- +goose Down
SELECT 1;
