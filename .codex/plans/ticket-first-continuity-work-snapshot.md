# Ticket-First Continuity Work Snapshot

Issue: `mem-384dfe1`
Parent: `mem-dccbb32`
Packaged Epic: `mem-73d0ddb`
Date: 2026-03-21

## Tracking Package
This design is packaged for implementation under epic `mem-73d0ddb`, "Make continuity ticket-first and session-secondary."

Initial child stories:

- `mem-a42832b` - Add issue work capture events and ticket work snapshot projection
- `mem-e329d7b` - Surface ticket work snapshots in issue show and board detail
- `mem-bebeca0` - Add ticket-first continuity commands backed by active sessions
- `mem-e2266ef` - Enrich packets and continuity evidence with ticket work snapshots

## Summary
Memori's continuity engine is structurally sound, but its human-facing meaning is currently attached too heavily to sessions. Session summaries and session packets are good at preserving resumability and provenance, but weak at answering the questions a human actually asks on a ticket:

- What is going on with this work right now?
- What changed since the last handoff?
- What should happen next?
- Is this safe to hand off or resume?
- Are there alternate attempts in flight?

This design proposes a ticket-first continuity model:

- the ticket becomes the primary human-facing continuity surface
- sessions remain real, but are demoted to attempt/provenance artifacts
- one write path produces both session-local and ticket-level continuity projections

The main product shift is:

- ticket = current best work state
- session = attempt window that produced or is competing to produce that state
- packet = deterministic resume artifact

## Problem Statement
Today, Memori stores continuity semantics mostly in session-level artifacts:

- `session.checkpointed`
- `session.summarized`
- `packet.built` for session scope

The data recorded by these artifacts is mostly continuity metadata:

- session identity
- start/close timestamps
- trigger
- context chunk counts
- recent chunk kinds
- latest event markers
- packet provenance and compaction metadata

That is useful for:

- deterministic resume
- packet-first rehydrate
- audit/debugging
- distinguishing multiple attempts

But it underdelivers for humans because it does not strongly capture:

- what changed
- what decision was made
- what remains
- what the next worker should do
- whether the ticket state should change because of this session

This is why the current continuity pane can feel like a parallel mechanic rather than part of the ticket workflow.

## Design Goals
- make the ticket the primary place to understand work state
- preserve sessions as real attempt boundaries for resume and audit
- avoid duplicate writes and drift between ticket and session continuity
- keep the event ledger authoritative
- keep packet-backed resume deterministic
- make parallel attempts legible without letting them overwrite the ticket's current best state automatically

## Non-Goals
- removing sessions
- removing packets
- replacing the event ledger with mutable note blobs
- making the board continuity pane the only continuity surface
- forcing humans to reason about session ids for normal ticket work

## Product Model
### Mental Model
- Ticket: "What is true about this work right now?"
- Session: "Which attempt produced that state, and what other attempts exist?"
- Packet: "What artifact should a machine use to resume deterministically?"

### Principle
Humans should usually read the ticket first.
Humans should inspect sessions only when they need to understand alternate attempts, provenance, or resume-path internals.

### Example
For one issue, Memori may have:

- `sess-a`: the stable implementation attempt
- `sess-b`: a newer experimental branch

The ticket should show the current best summary from `sess-a` until `sess-b` is explicitly promoted or becomes the selected best attempt by policy.

The session layer should still show:

- `sess-a` is stable and handoff-ready
- `sess-b` is active but risky or incomplete

That is the acute value of sessions:

- they preserve parallel attempts without collapsing them into one ticket stream
- they let the system distinguish "current truth" from "alternate attempt"

## Proposed Event Model
Add a new issue-scoped continuity event family that is authored through an active session.

### New Event
`issue.work_captured`

Purpose:
- record a human-meaningful work/handoff update for a ticket
- tie that update to the active session that produced it
- drive both session and issue continuity projections

Suggested payload:

```json
{
  "issue_id": "mem-dccbb32",
  "session_id": "sess_85471748e4fa",
  "captured_at": "2026-03-21T12:00:00Z",
  "capture_kind": "checkpoint|handoff|note|close-ready",
  "summary": "Continuity pane now renders, but current session artifacts remain too metadata-heavy.",
  "what_changed": [
    "Added continuity audit snapshot read model",
    "Added board continuity pane",
    "Reworked pane copy to be decision-first"
  ],
  "next_step": "Design an issue-level work snapshot projection fed by session updates.",
  "blockers": [],
  "risks": [
    "Session summaries still do not capture substantive work content well"
  ],
  "open_questions": [
    "Should ticket continuity be projected only from selected attempts?"
  ],
  "handoff_readiness": "usable",
  "resume_preference": {
    "scope": "session",
    "packet_id": "pkt_be472654ed22f307"
  },
  "evidence_refs": [
    "event:evt_09474350006c6dbc",
    "packet:pkt_be472654ed22f307"
  ]
}
```

### Why A New Event Instead Of Reusing `session.summarized`
`session.summarized` is attempt-local and currently optimized around context artifacts.
We need a first-class event whose semantics are:

- "capture the current best human work state for this ticket"

That is not the same thing as:

- "summarize recent session chunks"

`session.summarized` can remain as a lower-level continuity artifact.
`issue.work_captured` becomes the higher-value human-facing continuity write.

### Optional Follow-On Event
If promotion between competing attempts needs explicit governance, add:

- `issue.work_selected`

Purpose:
- explicitly select which captured work snapshot is the ticket's current best truth when multiple sessions are active

This can be deferred if simple recency + open-session policy is sufficient in v1.

## Proposed Projection Model
### New Projection
`issue_work_snapshots`

Suggested shape:

```go
type IssueWorkSnapshot struct {
    IssueID             string
    SessionID           string
    CaptureKind         string
    UpdatedAt           string
    UpdatedBy           string
    Summary             string
    WhatChanged         []string
    NextStep            string
    Blockers            []string
    Risks               []string
    OpenQuestions       []string
    HandoffReadiness    string // weak | usable | strong
    WorkStatus          string // active | blocked | ready-handoff | stale | ambiguous
    ResumeSource        string // issue-packet | session-packet | summary | fallback
    ResumePacketID      string
    EvidenceRefs        []string
    AlternateSessionIDs []string
    LastEventID         string
}
```

### Projection Semantics
The projection should answer "current best work state for this issue."

Initial policy:
- latest `issue.work_captured` wins
- if multiple open sessions exist, `WorkStatus` becomes `ambiguous`
- `AlternateSessionIDs` lists other active sessions for the issue
- if no issue work capture exists yet, projection is empty and ticket falls back to older continuity surfaces

Later policy option:
- introduce explicit attempt selection via `issue.work_selected`

### Derived Fields
`WorkStatus` is derived from:

- issue status
- handoff readiness
- open session count
- packet freshness
- whether the selected session has a packet

Example mapping:

- `active`: issue is `InProgress`, one active selected attempt
- `blocked`: issue is `Blocked`
- `ready-handoff`: strong or usable handoff readiness with packet-backed resume
- `stale`: ticket continuity exists but issue packet is stale
- `ambiguous`: more than one active session exists and no explicit disambiguation has occurred

## Session Model Changes
Sessions remain as they are, but their meaning is clarified:

- attempt boundaries
- event lineage
- packet presence
- summary history
- raw context evidence

The current session schema is mostly sufficient.

Possible additive fields later:
- branch name
- worktree path
- touched file summary
- validation summary

These should remain attempt-local, not ticket-global.

## Packet Model Changes
### Issue Packets
Issue packets should start carrying the latest projected issue work snapshot in their `decision_summary` or adjacent human-facing fields.

Add fields such as:

- `current_work.summary`
- `current_work.what_changed`
- `current_work.next_step`
- `current_work.handoff_readiness`
- `current_work.session_id`
- `current_work.alternate_attempt_count`

This makes issue packets actually answer:
- what matters on this ticket right now

### Session Packets
Session packets should stay resume-focused, but they can include a ticket-facing excerpt:

- `ticket_summary`
- `next_step`
- `handoff_readiness`

That way a resume artifact is still human-legible, but does not become the primary ticket surface.

## CLI Design
### New Ticket-First Commands
Humans and agents should not need to think in session-first terms for normal continuity updates.

Add:

- `memori issue checkpoint --key <issue> --summary ...`
- `memori issue handoff --key <issue> --summary ... --changed ... --next-step ...`
- `memori issue note --key <issue> --summary ...`

These commands:

- resolve the active session for the issue
- append `issue.work_captured`
- optionally append `session.summarized` for session-local continuity
- refresh issue and/or session packets as needed

### Command Examples
Checkpoint during active work:

```bash
memori issue checkpoint --key mem-dccbb32 \
  --summary "Continuity pane renders and tests pass." \
  --changed "Added audit snapshot read model" \
  --next-step "Improve human-facing ticket continuity summary"
```

Handoff:

```bash
memori issue handoff --key mem-dccbb32 \
  --summary "Continuity pane works and reads better, but ticket-level state is still under-modeled." \
  --changed "Built board continuity pane" \
  --changed "Changed continuity pane to decision-first copy" \
  --next-step "Design issue work snapshot projection" \
  --risk "Session artifacts still over-emphasize metadata" \
  --readiness usable
```

Quick note:

```bash
memori issue note --key mem-dccbb32 \
  --summary "Tried a more compact continuity pane layout; not ready to promote yet."
```

### Relationship To Existing Commands
- keep `context summarize`, `context packet build`, `context resume` for lower-level control
- make `issue checkpoint|handoff|note` the preferred human-facing write path
- keep `issue update --status ...` auto-continuity behavior, but teach it to maintain the issue work snapshot on pause/close transitions

## TUI Design
### Board Detail Pane
Board detail should become the primary human continuity surface.

For the selected issue, show:

- Current State: `Active, resumable, handoff usable`
- Summary: 1-3 sentence plain English status
- What Changed: top 1-3 bullets
- Next Step: one strong next action
- Resume: selected session/packet summary
- Risks: only the most important ones

### Continuity Pane
Continuity mode should become evidence-first, not meaning-first.

It should answer:
- why this resume path won
- what alternate sessions exist
- what packet lineage exists
- which writes happened recently
- whether continuity is stale/ambiguous/inconsistent

In other words:

- detail pane = "what is going on?"
- continuity pane = "prove it"

### Issue Show
`issue show` should render the projected issue work snapshot near the top, above lower-level continuity hints.

Suggested sections:

- `Current Work`
- `Resume`
- `Continuity Health`
- `Proof / Gates`

## Resolver Interaction
The resume resolver does not need a major rewrite.

It should continue using:
- explicit session
- focus-bound session
- issue-scoped latest session
- fallback heuristics

But the human narrative shown around that resolver should be issue-scoped whenever possible.

The issue work snapshot should include:
- which session is currently selected
- whether alternates exist
- whether the selected path is packet-backed

That gives the human context without replacing the resolver.

## Migration Path
### Phase 1
- add `issue.work_captured` event and `issue_work_snapshots` projection
- keep all current session summary/packet behavior unchanged
- render issue work snapshot in `issue show` and board detail
- keep continuity pane as evidence

### Phase 2
- add ticket-first commands: `issue checkpoint`, `issue handoff`, `issue note`
- update docs to recommend these commands for normal human/agent handoff
- preserve `context *` commands as advanced controls

### Phase 3
- enrich issue packet builder to include projected work snapshot
- teach ranking/explanations to reference work snapshot when present

### Optional Phase 4
- add `issue.work_selected` if multiple active attempts need explicit promotion rules

### Backfill Strategy
Do not try to synthesize high-quality historical `issue.work_captured` events from old session summaries.
Instead:

- start writing the new event going forward
- render "no captured work snapshot yet" when none exists
- continue showing existing continuity signals as fallback

This keeps historical correctness high and avoids inventing meaning that was never written.

## Risks
### Drift Risk
If we update ticket state and session state through different write paths, they will drift.

Mitigation:
- one high-level write command
- one event append
- two projections

### Overwrite Risk With Parallel Sessions
Multiple active sessions may race to update the ticket's current work state.

Mitigation:
- surface `ambiguous` state in the issue work snapshot
- later add explicit promotion/selection if needed

### Over-Structuring Risk
Too many required fields could make capture burdensome.

Mitigation:
- require only `summary`
- keep `changed`, `risk`, `question`, `next-step` additive
- allow repeated flags rather than big JSON payloads

### Resume Regression Risk
Ticket-first continuity must not break packet-backed resume.

Mitigation:
- leave resolver semantics unchanged in phase 1
- treat new issue work snapshot as additive human-facing projection first

## Acceptance Criteria
- ticket-level continuity can answer "what is going on with this work?" without reading session internals
- sessions remain useful for alternate attempts, provenance, and deterministic resume
- a worker can record one ticket-scoped handoff update that feeds both ticket and session continuity
- board detail and `issue show` become the default continuity surfaces for humans
- continuity mode remains available as advanced audit evidence
- the migration path preserves existing session/packet behavior while introducing the new model incrementally

## Recommended Implementation Slices
1. Define `issue.work_captured` event payload, validation, and projection.
2. Add `issue_work_snapshots` read model and tests.
3. Render ticket work snapshot in `issue show`.
4. Render ticket work snapshot in board detail.
5. Add `issue checkpoint|handoff|note` CLI commands.
6. Update issue packet builder to include projected work snapshot.
7. Reframe continuity pane and docs around ticket-first continuity.

## Open Questions
- Is recency enough to select the current best work snapshot, or do we need explicit attempt promotion from day one?
- Should `issue update --status blocked|done` auto-emit `issue.work_captured` using the same note/reason payload?
- Do we want issue work snapshots to reference validation commands/results directly, or keep those only in evidence refs?
- Should ticket detail surface one alternate session headline when `ambiguous`, or keep alternates only in continuity mode?
