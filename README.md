# memori (Slice 1 scaffold)

This repo now contains the first implementation slice for a local event-ledger-backed issue CLI.

## Implemented in this slice

- `memori init`
- Event store foundation (`events` append-only, hash chain columns, idempotency key)
- Immutable gate schema foundation (`gate_templates`, `gate_sets`, `gate_set_items` + immutability triggers)
- Close-validator contract on `issue update --status done` (checks required locked gates from source-of-truth data)
- Minimal projection table (`work_items`)
- `memori issue create`
- `memori issue link`
- `memori issue update`
- `memori issue show`
- `memori gate template create`
- `memori gate template list`
- `memori gate set instantiate`
- `memori gate set lock`
- `memori gate evaluate`
- `memori gate status`
- `memori context checkpoint`
- `memori context rehydrate`
- `memori context packet build|show|use`
- `memori context loops`
- `memori event log`
- `memori db status`
- `memori db migrate`
- `memori db verify`
- `memori db backup`
- `memori db replay` (rebuild projections from the event ledger)

## Quick start

```bash
# initialize DB
memori init --issue-prefix mem

# inspect migration status
memori db status

# apply pending migrations (uses goose)
memori db migrate

# verify version alignment
memori db verify

# write restorable snapshot
memori db backup --out /tmp/memori-backup.db

# create an issue
memori issue create --type task --title "First ticket" --command-id "cli-create-01"

# create with richer context
memori issue create \
  --type task \
  --title "Improve issue context" \
  --description "Add richer fields for agent/human handoff" \
  --acceptance-criteria "Issue show returns context-rich payload" \
  --reference "https://example.com/spec" \
  --command-id "cli-create-02"

# show issue
memori issue show --key <issue-key>

# move issue to in-progress
memori issue update --key <issue-key> --status inprogress --command-id "cli-update-01"

# update context fields without changing status
memori issue update \
  --key <issue-key> \
  --description "Updated context" \
  --acceptance-criteria "Readable in issue show output" \
  --reference "notes.md" \
  --command-id "cli-update-02"

# re-link child to parent
memori issue link --child <child-key> --parent <parent-key> --command-id "cli-link-01"

# inspect event ledger for the issue
memori event log --entity <issue-key>

# evaluate a gate
memori gate evaluate \
  --issue <issue-key> \
  --gate build \
  --result PASS \
  --evidence "ci://run/123" \
  --command-id "cli-gate-eval-01"

# inspect gate status for current locked gate set (or a specific cycle)
memori gate status --issue <issue-key> [--cycle <n>]

# close validation requires:
# - a locked gate set for the issue's current cycle
# - PASS for every required gate
# - proof-bearing evaluations (non-empty evidence refs) for those PASS results

# create template definition file and register versioned gate template
cat > /tmp/quality-gates.json <<'JSON'
{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false}]}
JSON
memori gate template create \
  --id quality \
  --version 1 \
  --applies-to task \
  --file /tmp/quality-gates.json

# instantiate and lock a gate set for the issue's current cycle
memori gate set instantiate --issue <issue-key> --template quality@1
memori gate set lock --issue <issue-key>

# list templates (optionally filtered by issue type)
memori gate template list --type task

# checkpoint and rehydrate context
memori context checkpoint --session sess-1 --trigger manual
memori context packet build --scope issue --id <issue-key>
memori context loops --issue <issue-key>
memori context rehydrate --session sess-1

# rebuild projections from events
memori db replay
```

`--json` is supported on read/list commands in this slice and on `init`/`issue create`/`issue update` for structured automation.

Mutating commands (`issue create`, `issue update`, `issue link`, `gate evaluate`) require `--command-id` for idempotency tracking.

Issue key format:

- `{prefix}-{shortSHA}` (example: `wrk-a1b2c3d`)
- Issue type is stored in `--type`; do not encode type names (`epic|story|task|bug`) into the key prefix.
- Prefix is project-wide and set at init (`--issue-prefix`); all new issue keys must use that prefix.
