# Running ACD Alongside Another Auto-Committer

This document covers what happens when `acd` shares a repository with another
process that also creates commits automatically — for example, the Claude Code
Automatic Atomic Commits hook or the Codex ACD hook.

See [docs/capture-replay.md](capture-replay.md) for the underlying replay and
conflict-resolution mechanics this document references.

---

## How parallel committers interact with ACD

`acd` captures file operations at the moment they happen and queues them as
`pending` events in SQLite. The replay loop drains the queue on each poll tick.
With `ACD_COMMIT_STRATEGY=event`, it considers one event at a time. With
`ACD_COMMIT_STRATEGY=intent`, it may wait for `ACD_INTENT_MIN_PENDING`, the
`ACD_INTENT_MAX_PENDING_AGE` escape hatch, or an explicit flush before offering
at most `ACD_INTENT_WINDOW` visible captures to the planner. It may then publish
a selected group as one commit, but the same safety checks and idempotent settle
rules still apply to each selected capture.
When another tool (Claude Code hook, Codex hook, or any `git commit` call) lands
a commit *before* `acd`'s replay tick, the branch ref has already advanced. On
the next replay pass `acd` detects this via its idempotent publish probe
(`alreadyPublishedAtHEAD`, `internal/daemon/replay.go`):

1. `acd` reads the current `HEAD` tree.
2. For every op in the queued event, it checks whether the desired final state
   is already present — file blob matches, or path is already absent for
   deletes.
3. An ancestry guard confirms `HEAD` descends from the replay parent that the
   event was chained off. This prevents a coincidental tree match from masking
   a real divergence.
4. If every op passes, the event is marked `published` with `commit_oid = HEAD`.
   No new commit is created.

Trace decision strings emitted by this path:

| Decision string | Trigger |
|---|---|
| `already_published_by_external_committer` | Before-state probe would have blocked; HEAD tree already matches — external tool landed the change. |
| `already_published_no_op_tree` | Op set produces an empty tree diff (no content change); settled at HEAD without a commit. |
| `already_published_after_cas_exhaustion` | CAS retries exhausted; HEAD already reflects the captured change — treated as a parallel publish. |

---

## Claude Code Automatic Atomic Commits hook

The Claude Code Automatic Atomic Commits (AAC) plugin commits each file
immediately after every `Edit` or `Write` tool use. These commits land on the
branch *before* `acd`'s next replay tick. Typical sequence:

```text
[Claude Code hook] Edit tool use fires
[Claude Code hook] git commit -m "modify auth.go"   ← branch advances
[acd replay tick ] pending event for auth.go
                   → alreadyPublishedAtHEAD = true
                   → event marked published (no new commit)
```

The net result is the same file state without a duplicate ACD commit. `acd`
marks the event published against the external commit, so author, message, and
timing come from the tool that committed first. `acd status` will show
`pending_events: 0` within one tick after the hook runs.

---

## Codex ACD hook

The Codex harness (`acd init codex`) installs a `PostToolUse` hook that calls
`acd wake`, nudging `acd` to replay immediately after each Codex tool event.
If Codex itself also has an auto-commit plugin active, the same idempotent
publish logic applies: `acd` detects the already-landed commit and settles
without creating a duplicate.

---

## Edge cases that still produce `blocked_conflict`

The idempotent publish probe only passes when the live `HEAD` tree exactly
matches the captured intent. The following scenarios bypass it and become
`blocked_conflict`:

| Scenario | Why it blocks |
|---|---|
| **Mode-only change** where the external committer used a different file mode | Blob OIDs match but the recorded `after_mode` diverges from what `HEAD` shows; the probe fails the per-op check. |
| **Rename source unreachable** | External tool deleted the rename source before `acd` could move it; the path is absent but `after_oid` does not match an absent entry. |
| **Symlink target mismatch** | Symlink (mode `120000`) blob encodes the target string; an external tool that wrote a different target produces a different OID, failing the probe. |
| **Ancestry divergence** | `HEAD` does not descend from the replay parent (force-push, hard reset to an unrelated commit); the ancestry guard returns `false` before the tree is even checked. |

Resolve `blocked_conflict` rows with the standard workflow:

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
acd diagnose --repo .
acd fix --dry-run
acd fix --yes
~~~

Use `recover` or `purge-events` only as advanced fallbacks when `diagnose` or
`fix --dry-run` points at stale branch anchors, obsolete terminal barriers, or
failed terminal barriers that are blocking later pending replay:

~~~bash
acd recover --repo . --auto --dry-run
acd purge-events --repo . --blocked --dry-run
acd wake --session-id "$ACD_SESSION_ID"
~~~

`wake` requires a non-empty session id. Use it from harness shells that set
`ACD_SESSION_ID`; otherwise wait for the next daemon tick after applying a safe
plan.

---

## Recommended configurations

### Option A — let `acd` be the sole committer (simplest)

Disable any per-file auto-commit hooks in your harness and let `acd` be the
only tool writing commits. No idempotent probe is needed; replay always has
a clean before-state.

~~~bash
# Claude Code: remove or disable the Automatic Atomic Commits plugin
# Codex: do not install a separate auto-commit hook alongside acd init codex
acd init codex   # wake hook only — no separate commit hook
~~~

### Option B — accept idempotent settle as the steady state

Run both hooks simultaneously. The idempotent publish probe absorbs the
parallel committer's commits silently. Watch `acd status` to confirm
`blocked_conflicts` stays at `0` and no failed terminal barrier is blocking
pending replay, or stream the decision ledger:

~~~bash
acd events --watch
acd explain --commit HEAD
~~~

`acd events --watch` starts at the current ledger tail when `--since` is
omitted, so it prints only decisions appended after watch starts.

Enable trace logging only when you need internal replay decisions:

~~~bash
ACD_TRACE=1 acd start --repo . --session-id debug --harness claude-code
# decisions appear in .git/acd/trace/YYYY-MM-DD.jsonl
~~~

Filter for the settle path:

~~~bash
grep already_published .git/acd/trace/*.jsonl | python3 -c \
  "import sys,json; [print(json.loads(l)['decision'], json.loads(l)['input']) for l in sys.stdin]"
~~~

---

## See also

- [User workflows](user-workflows.md) — daily status, events, explain, fix, and
  support diagnostics workflows.
- [Revert workflows](capture-replay.md#revert-workflows) — git revert, reset,
  and rebase with ACD running.
- [Replay mechanics](capture-replay.md#replay-how-a-pending-event-becomes-a-commit)
  — scratch-index, conflict probe, and CAS ref update.
- [Trace event classes](capture-replay.md#trace-event-classes) — full
  enumeration of `event_class` and `decision` strings.
