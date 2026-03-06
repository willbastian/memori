# Local Context Bridge + Agile Issue Ledger (Go/SQLite)

## Summary
Build a lightweight local system that preserves agent continuity across context limits/resets while making Agile work items first-class (`Epic`, `Story`, `Task`, `Bug`).
The system is append-only at the event layer, so every update is immutable, replayable, and traceable.
Issue completion is guarded by immutable success gates instantiated from versioned templates, so tickets cannot be closed by weakening gates mid-flight.

## Implementation Changes
- **Architecture**
  - Create a Go CLI app (`memori`) with SQLite as the only required runtime dependency.
  - Use event sourcing as the source of truth; materialized read models are derived from events.
  - Keep raw conversational/context artifacts in a local store and generate compact "rehydration packets" for reset recovery.
- **Core domain model**
  - `work_items`: logical identity (`id`, `type`, `title`, `parent_id`, `status`, `priority`, `labels`, `created_at`).
  - `events` (append-only): (`event_id`, `entity_type`, `entity_id`, `entity_seq`, `event_type`, `payload_json`, `actor`, `causation_id`, `correlation_id`, `created_at`, `hash`, `prev_hash`).
  - `events` constraints and indexes: `UNIQUE(entity_type, entity_id, entity_seq)` for deterministic per-entity ordering, plus index on (`entity_type`, `entity_id`, `created_at`).
  - write rule for `entity_seq`: assigned in the same transaction as event insert as `max(entity_seq)+1` per entity (or equivalent atomic counter table) to avoid timestamp/id tie ambiguity.
  - `gate_templates`: versioned reusable definitions (`template_id`, `version`, `applies_to`, `definition_json`, `created_at`) with immutable versions (new version on change).
  - `gate_sets`: instantiated immutable completion contracts per issue-cycle (`gate_set_id`, `issue_id`, `cycle_no`, `template_refs`, `frozen_definition_json`, `gate_set_hash`, `locked_at`, `created_at`).
  - `sessions`: agent session boundaries (`session_id`, `started_at`, `ended_at`, `trigger`, `summary_event_id`).
  - `context_chunks`: compressed context artifacts linked to events/work items (`chunk_id`, `session_id`, `kind`, `content`, `embedding_ref`, `created_at`).
  - `snapshots` (derived, replaceable): current projections for fast CLI reads (`work_item_view`, `backlog_view`, `epic_rollup_view`, `open_loops_view`).
- **Agile lexicon behavior**
  - Support first-class types: `Epic`, `Story`, `Task`, `Bug`.
  - Parent/child constraints: `Epic -> Story`, `Story -> Task|Bug`; allow standalone `Task|Bug`.
  - Status workflow default: `Todo`, `InProgress`, `Blocked`, `Done`.
  - Every field/state change emits events (no in-place mutation).
  - Completion gate policy: each issue cycle references one locked `gate_set`; `Done` transition is rejected unless all required gates in that set have current `PASS`.
  - Reopen policy: reopening an issue creates a new cycle with a new gate set instance; previous cycle gates/events remain immutable and auditable.
- **Context-bridge behavior**
  - On each meaningful agent update, write events plus optional context chunk references.
  - When context budget threshold is exceeded, run compaction to produce:
    - `decision_summary`
    - `open_questions`
    - `next_actions`
    - `linked_work_items`
  - On reset, rehydrate from latest session summary + unresolved work-item graph + recent high-relevance chunks.
  - Token budget policy in rehydrate packet: 50% active work graph, 30% recent decisions, 20% unresolved/open risks.
- **CLI surface (v1)**
  - global output contract: all read/list commands support `--json` and output stable key order + documented schema version (`schema_version`) for machine parsing.
  - `memori init`
  - `memori gate template create --id ... --version ... --applies-to ... --file ... [--json]`
  - `memori gate template list [--type ...] [--json]`
  - `memori gate set instantiate --issue ... --template id@version [--template ...] [--json]`
  - `memori gate set lock --issue ... [--cycle ...] [--json]`
  - `memori gate evaluate --issue ... --gate ... --result pass|fail|blocked --evidence ... [--json]`
  - `memori gate status --issue ... [--cycle ...] [--json]`
  - `memori issue create --type epic|story|task|bug --title ... [--parent ...] [--json]`
  - `memori issue update --id ... --status ... --priority ... --label ... [--json]`
  - `memori issue link --child ... --parent ... [--json]`
  - `memori issue show --id ... [--json]`
  - `memori backlog [--json]`
  - `memori event log --entity ... [--json]`
  - `memori context checkpoint --session ... [--json]`
  - `memori context rehydrate --session ... [--max-tokens ...] [--json]`
- **Traceability guarantees**
  - Event hash chain (`hash`, `prev_hash`) for tamper evidence.
  - Monotonic sequence per entity for deterministic replay.
  - Replay order contract: sort by (`entity_seq`) per entity and by (`created_at`, `event_id`) only for cross-entity timeline views.
  - Gate immutability contract: after `gate_set lock`, no gate definition mutation events are accepted for that `gate_set_id`.
  - Anti-cheat close contract: `issue status -> Done` event must reference a locked `gate_set_hash` and a verifier proof that all required gates are `PASS`.
  - No hard deletes; use tombstone/deprecation events.
  - Correlation metadata required for multi-step operations.

## Delivery Vertical Slices
- `Slice 1: Ledger foundation + basic issue flow`
  - Deliver `memori init`, event store schema, and minimal projection pipeline.
  - Deliver `issue create`, `issue show`, and `event log` with optional `--json`.
  - Done when a user can create one item and replay events to rebuild the same state from an empty DB.
- `Slice 2: Agile hierarchy + backlog workflow`
  - Deliver `Epic/Story/Task/Bug` typing, parent-child linking rules, and status transitions.
  - Deliver `issue update`, `issue link`, and `backlog` views in human and json formats.
  - Done when hierarchy constraints and cycle prevention are enforced and backlog is queryable by status/type.
- `Slice 3: Immutable success gates (template + instance model)`
  - Deliver versioned gate templates and per-issue-cycle gate-set instantiation/locking.
  - Deliver gate evaluation events (`PASS|FAIL|BLOCKED`) with evidence references and gate status projection.
  - Deliver close validator that blocks `Done` until all required locked gates pass.
  - Done when attempts to modify locked gates are rejected and closure succeeds only with full gate pass proof.
- `Slice 4: Deterministic replay + audit integrity`
  - Deliver `entity_seq` assignment logic, uniqueness enforcement, and replay engine contract.
  - Deliver hash chain write/verify path and deterministic projection rebuild command.
  - Done when tamper checks fail on modified events and replay output is byte-stable across repeated rebuilds.
- `Slice 5: Session checkpoints + reset recovery`
  - Deliver `sessions` model and `context checkpoint` command that emits decision/open-loop/next-action artifacts.
  - Deliver `context rehydrate` command that reconstructs working context from latest session and unresolved items.
  - Done when a simulated reset recovers unresolved work graph and next actions without manual reconstruction.
- `Slice 6: Context compaction + relevance retrieval`
  - Deliver context chunk persistence, relevance ranking (keyword-first), and compaction trigger policy.
  - Deliver rehydrate packet budgeting (50/30/20) and linked-work-item inclusion guarantees.
  - Done when large histories produce compact packets that preserve decisions, blockers, and active tasks.
- `Slice 7: Hardening + operational ergonomics`
  - Deliver schema versioning in json output, migration path for DB upgrades, and consistency checks.
  - Deliver full automated tests for replay integrity, CLI json stability, and reset continuity scenarios.
  - Done when all test plan cases pass in CI and local reruns are deterministic.

## Test Plan
- `Issue lifecycle replay`: create/update/close items and rebuild projections from empty DB; projection state must match expected.
- `Hierarchy rules`: reject invalid parent-child links and cycles.
- `Immutability`: confirm updates append events only; prior events unchanged.
- `Gate immutability`: verify locked gate sets reject definition changes and preserve historical cycle evidence.
- `Gate close enforcement`: verify `Done` transition fails with any required gate not passed and succeeds only with full required gate pass set.
- `Template versioning`: verify template changes require a new version and do not mutate previously instantiated gate sets.
- `Trace chain`: verify hash-chain integrity and detection on tampered row.
- `Entity ordering`: verify `UNIQUE(entity_type, entity_id, entity_seq)` enforcement and deterministic replay after import/rebuild.
- `Reset continuity`: simulate session cutoff, run checkpoint + rehydrate, confirm unresolved items/next actions preserved.
- `Compaction quality`: ensure compaction output includes decisions, blockers, and next actions tied to entity IDs.
- `CLI usability`: validate key commands produce deterministic machine-readable output (`--json`, stable keys, `schema_version`) and human-readable output.

## Assumptions and Defaults
- Single-user local-first system in v1; `actor` defaults to local profile.
- SQLite in WAL mode; all timestamps stored in UTC.
- Go + SQLite chosen for runtime.
- No Sprint/Milestone entities in v1 (can be added as new entity/event types later).
- Embeddings/relevance can start keyword-based in v1.0 and be upgraded without schema break by adding new chunk-index tables.
