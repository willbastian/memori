# Continuity Inspection Facet for Human-Auditable Session State

## Summary
`memori` already surfaces continuity hints in `issue show`, `issue next`, and `board`, but the current experience is optimized for guidance, not auditability. A human can tell that continuity exists, yet cannot easily answer deeper trust questions such as:

- Which exact session or packet would resume use right now?
- Why was that artifact selected instead of another candidate?
- What continuity writes have happened recently, and what is missing?
- Which alternate sessions, packets, or fallback sources are available if the current path looks wrong?

This plan proposes a dedicated continuity-inspection facet inside the board TUI. The facet stays read-only, remains derived from the event ledger plus existing projections, and helps a human inspect the selected issue's continuity chain before resuming, handing off, or closing work.

## Current Product Baseline
Today the continuity story is spread across a few compact surfaces:

- `issue show` renders `Continuity State`, `Continuity Pressure`, and resume guidance.
- `issue next` uses continuity signals to rank work and emits short resume/start suggestions.
- `board` shows continuity pressure in the likely-next lane and renders compact tokens in the signal deck.
- `store.ContinuitySnapshot` returns one issue snapshot, one agent focus snapshot, and one latest relevant session snapshot.

That baseline is useful for "what should I do next?" but too compressed for "can I trust this continuity path?" The main limitation is that most read paths are latest-only:

- one latest issue packet, not the recent candidate packets
- one latest open or latest historical session, not the alternate sessions for the same issue
- one saved focus target, not the relationship between focus, resume resolution, and fallback paths
- status text, not a write timeline that shows checkpoints, summaries, packet builds, or focus updates

## Audit Questions the Facet Must Answer
The facet should let a human answer these questions without leaving the TUI:

1. What continuity path is currently resolved for this issue?
2. Which agent focus, packet, session, summary, and issue cycle are in effect?
3. Why did the resolver choose that path?
4. What other sessions or packets are available for the same issue?
5. Which continuity writes happened most recently, and which expected writes are missing?
6. Is the selected path fresh, stale, ambiguous, or internally inconsistent?
7. If the current path is wrong, what is the safest alternate artifact to inspect or resume from?

## Core Objects to Expose
The facet should make the following objects first-class:

- Issue continuity state
  - issue id
  - current cycle number
  - latest issue event id
  - fresh/stale/missing issue packet state
  - open loop and gate pressure counts
- Agent focus
  - agent id
  - focused issue id and cycle
  - last packet id
  - whether the focus packet matches the selected issue and current cycle
- Sessions
  - all recent sessions for the selected issue, not only the latest one
  - active vs closed lifecycle state
  - started/ended timestamps
  - trigger
  - summary presence
  - latest session packet presence
- Packets
  - recent issue packets for the selected issue
  - recent session packets for each candidate session
  - built-from event id
  - creation time
  - issue cycle number
  - packet source hints where relevant
- Recent writes
  - issue status transitions that affect continuity
  - session checkpoint, summary, and close events
  - packet build events
  - focus.use events
- Integrity alerts
  - explicit derived warnings when continuity looks unsafe or ambiguous

## Proposed TUI Interaction Model
The board should keep the issue list as the primary navigation surface and add continuity inspection as a sibling of issue detail, not as a separate screen.

### Wide Layout
For widths that already show a split panel, the right pane becomes a mode switch:

- `Detail` mode: today's issue detail view
- `Continuity` mode: the new inspection facet

Suggested key:

- `c` toggles `Detail` and `Continuity` for the selected issue

This fits the current board architecture better than adding a full new lane or modal. The list stays stable on the left, while the right pane changes from descriptive issue context to auditable continuity context.

### Narrow Layout
For narrow terminals, `c` replaces the lower stacked detail pane with the continuity pane. This preserves the existing mental model:

- `space` or `enter` still opens/closes the lower pane
- `c` changes the pane's content between `Detail` and `Continuity`

### Information Architecture Inside the Continuity Pane
The pane should use five compact sections in priority order.

1. Resolved Path
   - One-line answer to "what will resume use now?"
   - Example shape: `agent focus -> issue packet -> session sess-123 -> source: agent-focus-session`
   - Show whether the path is `fresh`, `fallback`, `stale`, or `ambiguous`

2. Audit Status
   - Short bullet list of integrity checks
   - Examples: `issue packet is stale`, `two open sessions exist`, `focus packet cycle mismatches current cycle`

3. Session Candidates
   - Compact table ordered by resolver preference
   - Columns: marker, session id, state, packet, summary, age, resolver note
   - The first row is the currently selected or inferred candidate

4. Packet Candidates
   - Recent issue and session packets with cycle, built-from event, and freshness
   - Make it obvious whether a packet is latest, stale, or attached to a closed session

5. Recent Writes
   - Reverse-chronological continuity event list
   - Focus on checkpoint, summarize, close, packet build, focus use, and continuity-relevant issue status changes

The pane can end with command hints for deep inspection:

- `memori context resume --session <id>`
- `memori context packet show --packet <id>`
- `memori event log --entity session:<id>`

## Proposed Visual Language
The continuity pane should feel like an audit console layered into the existing signal deck:

- use the current chrome and panel framing so it feels native to the board
- reserve bright accents for the resolved path and explicit alerts
- use terse ASCII-safe status chips such as `FRESH`, `STALE`, `MISS`, `AMB`, `FALLBACK`
- prefer stable columns and terse labels over wrapped prose
- keep section order consistent so operators can build muscle memory

The right-pane headline should always answer the trust question first, not bury it in metadata.

## Data Model Recommendation
The current `store.ContinuitySnapshot` shape is too small for this facet. Add a separate read model instead of overloading the existing lightweight snapshot used by `issue show`, `issue next`, and ranking.

Suggested store API:

```go
type ContinuityAuditSnapshotParams struct {
    IssueID string
    AgentID string
}

type ContinuityAuditSnapshot struct {
    Resolution     ContinuityResolution
    Issue          IssueContinuitySnapshot
    Agent          AgentContinuitySnapshot
    Session        SessionContinuitySnapshot
    Sessions       []ContinuitySessionCandidate
    IssuePackets   []ContinuityPacketCandidate
    SessionPackets []ContinuityPacketCandidate
    RecentWrites   []ContinuityWrite
    Alerts         []ContinuityAlert
}
```

Important additions beyond the current snapshot:

- `Resolution`
  - the exact chain the resume resolver would take now
  - includes `source`, selected `session_id`, selected `packet_id`, and fallback status
- `Sessions`
  - recent sessions for the issue, ordered by resolver preference
  - include an explicit `candidate_reason`
- `IssuePackets` and `SessionPackets`
  - recent packet history, not only the latest packet
- `RecentWrites`
  - continuity-relevant writes with event type, event id, actor, command id, and timestamp
- `Alerts`
  - precomputed integrity findings so the TUI stays simple

## Resolver Semantics to Surface Explicitly
The facet should expose the same logic already used by the CLI resolver helpers. Humans should be able to see which branch won:

- explicit session
- agent focus session
- agent focus issue open session
- agent focus issue latest session
- latest open session for issue
- latest session for issue
- latest open session globally
- latest session globally
- packet-first rehydrate
- relevant-chunks fallback
- raw-events fallback
- closed-session summary fallback

Showing the winning branch is important because a path can be correct yet still surprising.

## Integrity Concerns the Facet Must Detect
The inspection surface should call out at least these conditions:

- issue packet exists but is stale relative to the latest continuity-relevant event
- no issue packet exists for an active or blocked issue
- multiple open sessions exist for the same issue
- agent focus points at a different issue than the selected row
- agent focus references a packet that no longer exists
- focus packet cycle does not match the issue's current cycle
- latest session is closed and has no session packet
- closed session packet no longer matches the recorded session close lifecycle
- session has context chunks but no summary and no packet
- packet built-from event id cannot be resolved

These checks should be derived from the ledger and projections only. The facet must never become a manual source of truth.

## Data Sources
The continuity facet can be assembled from existing tables and helper logic:

- `work_items` for issue status, cycle, and last event metadata
- `open_loops` and gate projections for continuity pressure
- `sessions` for lifecycle state
- `context_chunks` for evidence that a session has resumable material
- `rehydrate_packets` for issue and session packets
- `agent_focus` for current focus bindings
- `events` for recent writes, causation, and correlation lineage
- existing resolver helpers in `internal/cli/context_session_resolution.go`
- existing freshness logic in `internal/store/continuity_snapshot.go`

## Non-Goals
- editing or overriding continuity state directly from the TUI
- introducing a second authority outside the event ledger
- replacing `issue show`, `context resume`, or `event log`
- rendering full packet JSON in the main pane

Deep inspection should still hand off to existing commands when needed.

## Implementation Slices
Recommended delivery order:

1. Store read model
   - add `ContinuityAuditSnapshot`
   - collect candidate sessions, packet history, recent writes, and derived alerts
   - lock behavior with store tests for stale, ambiguous, and fallback-heavy cases

2. Board state and rendering
   - add a panel mode toggle for `Detail` vs `Continuity`
   - render the five continuity sections in wide and narrow layouts
   - add focused board TUI rendering tests

3. Narrow-layout hardening
   - verify truncation and section prioritization still answer the primary audit question
   - make sure the pane degrades cleanly in 80x24 and smaller terminals

4. Command and docs follow-through
   - document the new key and continuity pane in `README.md`
   - align any help text or human output references if needed

## Follow-Up Issues Worth Creating
This investigation should naturally decompose into implementation work:

- store: continuity audit snapshot and alert derivation
- cli: board TUI continuity facet and keybinding
- tests/docs: continuity facet rendering coverage and README update

## Acceptance Mapping
This plan satisfies the story when it is used as the implementation brief for follow-up work:

- core objects and audit questions are defined above
- the TUI interaction model and pane information architecture are defined above
- relevant data sources and integrity concerns are explicitly listed above

