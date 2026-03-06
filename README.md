# memori (Slice 1 scaffold)

This repo now contains the first implementation slice for a local event-ledger-backed issue CLI.

## Implemented in this slice

- `memori init`
- Event store foundation (`events` append-only, hash chain columns, idempotency key)
- Minimal projection table (`work_items`)
- `memori issue create`
- `memori issue link`
- `memori issue update`
- `memori issue show`
- `memori event log`
- `memori db replay` (rebuild projections from the event ledger)

## Quick start

```bash
# initialize DB
memori init --issue-prefix mem

# create an issue
memori issue create --type task --title "First ticket" --command-id "cli-create-01"

# show issue
memori issue show --key <issue-key>

# move issue to in-progress
memori issue update --key <issue-key> --status inprogress --command-id "cli-update-01"

# re-link child to parent
memori issue link --child <child-key> --parent <parent-key> --command-id "cli-link-01"

# inspect event ledger for the issue
memori event log --entity <issue-key>

# rebuild projections from events
memori db replay
```

`--json` is supported on read/list commands in this slice and on `init`/`issue create`/`issue update` for structured automation.

Mutating issue commands (`issue create`, `issue update`, `issue link`) require `--command-id` for idempotency tracking.

Issue key format:

- `{prefix}-{shortSHA}` (example: `wrk-a1b2c3d`)
- Issue type is stored in `--type`; do not encode type names (`epic|story|task|bug`) into the key prefix.
- Prefix is project-wide and set at init (`--issue-prefix`); all new issue keys must use that prefix.
