# Capture, Replay, and Conflict Resolution

This document explains how `acd` stores changes, turns them into commits, and
why replay sometimes blocks. For task-oriented daily recovery commands, start
with [user-workflows.md](user-workflows.md).

---

## Storage model

`acd` uses two stores for every captured change:

| Store | What lives there |
|---|---|
| SQLite (`<repo>/.git/acd/state.db`) | Event metadata, per-op blob OIDs, branch generation tokens, daemon state, client refcounts |
| Git object database (`.git/objects/`) | Actual file contents, written by `git hash-object -w` at capture time |

**SQLite never holds file contents.** A capture event row records the
`before_oid` and `after_oid` for each file operation; git already holds the
corresponding blobs. This means a captured change is durable even if the
worktree moves on — the blobs are in the object store and cannot be lost until
`git gc` prunes unreachable objects (which will not happen while the replay
queue still references them).

### Event states

Each row in `capture_events` passes through these states:

```
pending  →  published     (normal: commit was written and ref advanced)
         →  blocked_conflict  (terminal: see below)
         →  failed         (terminal: bad op data, no ops attached, etc.)
```

`pending` is the only non-terminal state. The replay loop processes only
`pending` rows. `blocked_conflict` and `failed` rows are terminal — they are
counted but never retried automatically. A terminal `blocked_conflict` or
`failed` row also acts as a sequence barrier for the same branch ref and
generation: later pending rows stay held until the operator deletes or otherwise
resolves the terminal predecessor. The retention pruner can delete old terminal
rows only after they no longer act as the active barrier.

### Product decision ledger

The user-facing `acd status`, `acd events`, and `acd explain` commands read an
append-only decision ledger. It is separate from raw daemon JSONL logs and uses
product terms:

| Decision | Meaning |
|---|---|
| `captured` | A worktree change was queued for replay. |
| `committed` | ACD published the queued event as a git commit. |
| `skipped` | ACD intentionally left a path uncommitted. |
| `protected` | A sensitive or generated path was protected without synthesizing a delete. |
| `handled_external` | Another commit already contains the captured after-state. |
| `superseded_external` | External history made queued work obsolete. |
| `blocked` | Replay stopped because applying the event was not provably safe. |
| `paused` / `resumed` | Capture or replay paused/resumed because of manual pause, rewind grace, or git operation state. |

Use `acd events --watch` to stream decisions appended after watch starts,
`acd explain --path FILE` to answer why one path was or was not committed, and
`acd explain --commit HEAD` to
explain a commit. See [user-workflows.md](user-workflows.md) for complete daily
flows.

---

## Replay: how a pending event becomes a commit

The daemon drains `pending` events on every poll tick by calling `Replay`. A
single pass works as follows:

1. **Seed a scratch index.** The daemon creates an isolated per-pass tempfile
   under `<gitDir>/acd/replay-*.index` and seeds it from the current `BaseHead`
   via `git read-tree`. This index is private to the replay pass; the repo's
   live working-tree index is never touched.

2. **For each pending event in sequence:**

   a. **Branch-generation guard.** The event's recorded `branch_ref` and
      `branch_generation` are compared against the daemon's current context. A
      mismatch means the branch was rewritten since capture (rebase, reset,
      branch switch) — see [Branch-generation safety](#branch-generation-safety).

   b. **Conflict probe.** The scratch index (advanced by every prior event in
      this pass) is queried via `git ls-files -s` (`LsFilesIndex`). Each op's
      `before_oid`/`before_mode` is compared against what the scratch index
      holds. If they disagree the event is a candidate for `blocked_conflict` —
      but step 2c (Idempotent publish check) may still settle it as `published`
      when `HEAD` already reflects the captured after-state. Real before-state
      mismatches that survive the idempotent check terminate the pass with
      `blocked_conflict`; later events are NOT replayed because they were
      captured assuming this one would land first.

   c. **Idempotent publish check.** Before recording a before-state mismatch,
      replay checks the current `HEAD` tree. If every op's desired final state is
      already present — including absent paths for deletes and rename cleanup —
      the event is marked `published` with `commit_oid = HEAD`. No new commit is
      created. This handles parallel committers that already landed the same
      change.

   d. **Apply ops.** The ops are fed to `git update-index --index-info` against
      the scratch index (via `GIT_INDEX_FILE`), advancing it atomically.

   e. **Build tree and commit.** `git write-tree` produces a tree OID from the
      updated scratch index. A commit is created via `git commit-tree` with the
      AI or deterministic message. The new commit becomes the parent for the
      next event in the pass.

   f. **Advance the branch ref.** `git update-ref` atomically advances the
      branch ref from `parent` to the new commit OID (compare-and-swap).
      If the CAS fails (someone else moved the ref), the event is
      `blocked_conflict` and the pass halts.

   g. **Record the outcome.** The event row is updated to `published` with the
      commit OID, and `publish_state` is upserted with `status = "published"`.

   h. **Reconcile the live index.** After a successful publish settlement, the
      daemon may update only the paths owned by that event in the repo's live
      index. This is an IDE-facing cleanup step: replay correctness still comes
      from the scratch index and the `update-ref` CAS above.

The scratch index is deleted when the pass returns. Every new pass creates a
fresh tempfile, so a crash mid-pass never poisons the next one.

### Live-index reconciliation safety matrix

ACD treats the live index as protected user state. Reconciliation is path-scoped
and guarded; broad `git reset`, `git checkout`, or `git read-tree` operations
against the user-facing index are intentionally out of bounds.

| Live-index state for event path | Automatic action |
|---|---|
| Create: path absent before replay and `HEAD` now has the captured after blob/mode | Add the captured `HEAD` entry to the live index |
| Modify/mode: index entry exactly matches the captured before blob/mode | Replace it with the captured after blob/mode |
| Delete: index entry exactly matches the captured before blob/mode | Remove the path from the live index |
| Rename: old path exactly matches captured before blob/mode and new path is absent or already matches captured after blob/mode | Remove old path and add the captured new-path entry |
| Same-path index entry differs from the captured before state | Skip; this is user staging or an external mutation |
| Any conflict-stage entry, intent-to-add entry, or malformed index shape | Skip and report; never flatten or overwrite it |
| Unrelated staged paths | Leave untouched |

---

## Branch-generation safety

Every captured event records the branch ref and a generation counter at the
moment of capture. The daemon classifies each HEAD movement as:

| Transition | Classification | Effect on queue |
|---|---|---|
| New HEAD descends from previous HEAD on the same branch ref | Fast-forward | Generation unchanged; queue remains valid |
| New HEAD does NOT descend from previous HEAD, or branch ref changes even at the same SHA | Diverged (rebase / reset / branch-switch) | Generation bumped; stale pending rows from the old generation are dropped. Terminal rows are preserved while the old ref still exists; if that ref was deleted, ACD prunes stale `pending`, `blocked_conflict`, and `failed` rows for the old ref/generation. |
| HEAD transitions to or from `missing` (orphan) | Diverged | Same as above |

The generation counter is persisted in `daemon_meta` under `branch.generation`
so a daemon restart picks up the last-known value rather than resetting to 1
(which would cause stale events to appear fresh). The last observed HEAD is
stored as `branch.head`, and the raw token is stored as `branch_token`. Token
shape is `rev:<sha> <branch-ref>` while attached, `rev:<sha>` while detached,
and `missing <branch-ref>` for an attached unborn branch.

ACD's own commits always fast-forward, so normal operation never bumps the
generation. Only external branch surgery does. If the branch moves backward on
the same branch ref, the daemon writes `daemon_meta.replay.paused_until` and
pauses replay for `ACD_REWIND_GRACE_SECONDS` seconds. The default is 60 seconds;
set it to `0` to disable the grace.

At startup the daemon classifies the persisted `branch.head` against the
current HEAD before overwriting metadata. If the branch was reset or rebased
while the daemon was offline, generation bumps and `shadow_paths` is reseeded
before capture resumes. Detached HEAD is treated as a pause: `acd start`
refuses to register, the daemon stamps `detached_head_paused`, and capture plus
replay stay disabled until HEAD is attached to a branch again.

Startup also sweeps stale terminal rows whose branch refs no longer exist. That
cleanup runs after the daemon has published `running`, so it does not sit on the
start path. It preserves the active branch/generation, preserves any branch ref
that still exists, and skips entirely when a manual pause marker is active. Set
`ACD_KEEP_DEAD_BRANCH_BARRIERS=1` before starting the daemon to keep deleted
branch rows for forensic inspection.

### Replay pauses

Replay can be paused from two sources:

| Source | Storage | Behavior |
|---|---|---|
| Manual operator pause | `<gitDir>/acd/paused` JSON marker | `acd pause` writes it, `acd resume --yes` removes it. The daemon reads it but never deletes it. |
| Rewind grace | `daemon_meta.replay.paused_until` | Set when the daemon detects a same-branch rewind. Pauses **both** capture and replay so a transient revert+re-edit cycle does not race the operator. Expired values are cleared by replay. |

Manual pause wins when both sources exist. Malformed manual markers and
unparseable rewind-grace timestamps fail open with a warning so a bad marker
does not lock the daemon permanently.

### Shadow generation retention

`shadow_paths` is keyed by `(branch_ref, branch_generation, path)`. A successful
reseed calls `PruneShadowGenerations`, keeping the current generation plus
`ACD_SHADOW_RETENTION_GENERATIONS` prior generations. The default is `1`, which
keeps one prior generation for inspection while bounding SQLite growth across
repeated rebases.

---

## AI diff from captured blobs

When an AI provider can use diff context, `acd` reconstructs a unified diff only
if both conditions are true: the provider declares `NeedsDiff`, and the operator
has opted in with `ACD_AI_DIFF_EGRESS=1`. The diff is built from captured
`before_oid` / `after_oid` blobs rather than inspecting the live worktree. This
means:

- The diff reflects exactly what was captured, even if the file has changed
  many times since.
- The rendered diff is capped at `DiffCap` (4000 bytes) while sections are
  appended. Each per-op git diff has a smaller git-layer cap of `2 * DiffCap`
  and a 5s timeout, so a large blob cannot stall the whole message build.
- `create` and `delete` ops use git's well-known empty-blob OID
  (`e69de29bb2d1d6434b8b29ae775ad8c2e48c5391`) for the missing side.

The deterministic provider declares that it does not need diffs, so default
replay skips reconstruction. Selecting `openai-compat` or `subprocess:<name>` is
not enough by itself; without `ACD_AI_DIFF_EGRESS=1`, ACD sends metadata only.

---

## Commit strategy

`ACD_COMMIT_STRATEGY=event` is the default replay strategy. It preserves the
original invariant that every pending capture event is considered separately
and, when published, produces at most one commit.

`ACD_COMMIT_STRATEGY=intent` keeps capture durability unchanged but changes how
pending events are offered to replay. ACD builds a bounded window of pending
captures, adds recent branch and path-aware commit context, and asks the AI
provider for a structured plan. `ACD_INTENT_WINDOW` is the maximum offered,
`ACD_INTENT_MIN_PENDING` is the preferred pending-count trigger, and
`ACD_INTENT_MAX_PENDING_AGE` is the bounded wait escape hatch for sparse queues.
The plan can select exactly one capture or any larger non-empty subset. Every
offered seq must be either selected or deferred.

Intent planning waits for a real grouping window before calling the planner:
if the visible pending queue is below `ACD_INTENT_MIN_PENDING` and the oldest
visible capture is younger than `ACD_INTENT_MAX_PENDING_AGE`, replay records a
`skipped_due_intent_batch_wait` no-op instead of planning. Reaching the count
trigger offers up to `ACD_INTENT_WINDOW`; reaching the age trigger offers the
currently visible pending rows up to that same maximum. Explicit `acd wake` /
flush requests bypass only this wait, so a user-requested flush plans whatever
is visible immediately. Flush does not bypass planner validation, terminal
barriers, safe ordering checks, or the forced-aging path for captures that were
already deferred too many times.

`acd status`, `acd diagnose`, and `acd doctor` surface this wait state when
intent mode is active. The reports include the visible pending count,
`min_pending`, oldest visible pending age, `max_pending_age`, and the estimated
age-trigger countdown when available. `doctor` also suggests the operational
choices: wait for another capture or the age trigger, flush/wake to publish the
current visible batch, lower the intent batching thresholds for sparse repos, or
switch back to `ACD_COMMIT_STRATEGY=event` for immediate one-event commits.

ACD remains the authority on safety. It rejects malformed plans, unknown seqs,
omissions, duplicate seqs, overlapping selected/deferred seqs, and selected
events that would leapfrog an earlier same-path or nested-path dependency
(`foo` before `foo/bar`, including rename sources). Selected captures are sorted
by seq, applied through the scratch index in order, written as one tree,
committed once, and settled with a shared `commit_oid`. Deferred captures stay
pending and get durable `planner_state` with `defer_count`, `last_planned_ts`,
and `last_defer_reason`.

When `defer_count >= ACD_INTENT_DEFER_LIMIT`, the oldest overdue capture is
forced through a one-item planning window unless an earlier pending related-path
capture must land first. This prevents starvation while preserving ordered
path-dependent replay.

Intent-specific observability:

- `acd status` shows the active commit strategy and planner deferral summary.
- `acd diagnose --json` reports deferred count, forced-aging readiness, and the
  last planner error without mutating state.
- `acd events` and `acd explain` expose grouped seqs, deferral reasons,
  forced-aging windows, and planner validation failures from
  `decision_records`.
- `ACD_TRACE=1` records planner input/output summaries, selected seqs, deferred
  seqs, batch-wait skips, and validation failures without writing captured
  source diffs, full AI prompts, or provider request envelopes.
- `ACD_AI_PROMPT_TRACE=1` records provider prompt/request diagnostics when a
  non-deterministic AI provider sends a request. The default deterministic
  provider emits no prompt trace because it does not send an AI request. Records
  live under `<gitDir>/acd/prompt-trace/` after redaction/truncation, but may
  still contain source code; inspect them with `acd prompt --last` or
  `acd prompt --seq <seq>`.

### One-shot path-sorted capture (`acd commit-all`)

`acd commit-all` uses the same capture pipeline as the live daemon but passes
`SortByPath: true` to the capture options. This causes captured events to be
ordered lexicographically by file path before they are inserted into the replay
queue. The live daemon does not use this option; it relies on fsnotify and poll
timestamps instead.

Path sorting matters because `commit-all` runs against a repo that may have
accumulated many changes while the daemon was off and has no reliable mtime
ordering. By emitting events in path order, sibling files in the same directory
cluster together in the commit sequence — for example, all `pkg/a/*.go` files
land in adjacent commits. With `ACD_COMMIT_STRATEGY=intent`, the intent planner
receives coherent windows of related siblings, which improves grouping quality
even without historical timing information.

Sort order interacts with the pending-depth cap (`ACD_MAX_PENDING_EVENTS`).
`SortByPath` reorders ops BEFORE the cap is applied, so events that overflow
the cap mid-pass are the lex-largest paths, not the most recently edited.
`acd commit-all` sets `DisablePendingCap: true` for its single capture call,
so the cap does not affect commit-all in practice; the daemon run loop
leaves `SortByPath` false and relies on the live walk's iteration order.

**Reseed before capture.** Unlike the live daemon, `commit-all` calls
`ReseedShadowFromHead` before capture rather than the idempotent
`BootstrapShadow`. The reseed deletes any existing shadow rows for the
active `(branch_ref, branch_generation)`, removes the bootstrap completion
marker, and re-bootstraps from `HEAD`'s tree. It also drops any stale
`pending` capture events for that branch+generation via
`state.DeletePendingForBranchGeneration`. This guarantees the diff vs HEAD
is what `commit-all` ends up committing, even when an earlier daemon
session absorbed worktree edits into shadow without successfully replaying
them. Without the reseed, the bootstrap marker is honored, the poisoned
shadow already mirrors live state, and Capture sees zero diff — the user
would observe "0 pending, no commits" while the worktree was still dirty.

---

## `blocked_conflict`: terminal state, operator action required

A `blocked_conflict` event will never be retried automatically. It signals that
the daemon could not reconcile the captured snapshot with the current state of
the branch. Common causes:

- **Generation mismatch**: the branch was rebased, reset, or switched since the
  event was captured.
- **Before-state mismatch**: the scratch index held a different OID/mode for a
  file than the op expected. This can happen when an external tool modified the
  file and those changes were never captured by `acd`.
- **CAS failure on `update-ref`**: another process pushed to the branch between
  the daemon's write-tree and update-ref calls.
- **Ancestry failure**: the event's `BaseHead` is no longer reachable from the
  current replay parent (e.g. the branch was force-pushed and the old commits
  were GC'd).

### Batch-halt behavior

The pass halts on the first `blocked_conflict` or failed replay-build row.
Events that came after the blocker in the capture sequence are left `pending`,
but `PendingEvents` hides them behind the terminal predecessor on later passes.
They do not leapfrog the broken event even when their paths are disjoint. The
entire backlog clears only after the operator resolves or deletes the root
conflict row.

### Retention pruning

The daemon prunes old `published` rows after `ACD_EVENT_RETENTION_DAYS`
(default 7 days). It also prunes stale terminal `blocked_conflict` and `failed`
rows past the same cutoff, but only when deleting them would not expose a later
pending row that still depends on the terminal barrier. Active barriers remain
until the operator resolves the conflict or deletes the row intentionally.

Dead-branch cleanup is separate from time-based retention. When the old branch
ref has been deleted, ACD can remove stale `pending`, `blocked_conflict`, and
`failed` rows for that old ref/generation together. If the removed blocked row
is the current `publish_state` blocker, ACD clears that stale singleton too.
Blockers for live refs are left intact.

---

## Operator commands

### Inspect the queue

~~~bash
acd status              # daemon, queue, pause, branch, and recent decisions
acd status --watch      # refresh the same repo until Ctrl-C
acd status --json       # machine-readable version
acd events              # durable product decision ledger
acd events --watch      # stream decisions appended after watch starts
acd explain --path FILE # explain why a path was captured, skipped, or blocked
acd explain --commit HEAD # explain decisions linked to a commit
acd list                # PENDING + BLOCKED columns across all repos
acd list --watch        # refresh the repo table until Ctrl-C
acd list --watch --interval 5s
acd logs                # tail the current repo daemon log as raw JSONL
acd logs --lines 200    # choose the initial tail length
acd logs --follow       # stream appended raw JSONL lines until Ctrl-C
acd doctor              # full diagnostics, including queue blockers and failures
acd doctor --bundle     # write a diagnostics zip to ~/Downloads for issue reports
~~~

### Recover or pause replay

~~~bash
acd diagnose --repo . --json
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
acd pause --repo . --reason "manual reset" --yes
acd resume --repo . --yes
~~~

`acd fix` is the single recovery entrypoint. Review the dry-run plan before
applying with `--yes`; `--force` opts into purging blocked barriers that still
have pending successors (use only when the captured changes already exist in
HEAD via an external committer). The legacy `acd recover` and
`acd purge-events` commands remain as hidden, deprecated aliases that forward
into `acd fix` for one release.

ACD keys lifecycle state by the canonical Git worktree root. `acd start` from
`repo/sub/dir` registers `repo`, not the subdirectory, and later `acd status` or
`acd diagnose --repo repo/sub/dir` look up that same root. Commands that need a
repo (`start`, `status`, `diagnose`, and related current-repo operations) refuse
directories outside a Git worktree instead of creating central-registry entries
for arbitrary paths.

`acd list --watch` redraws plain table frames on the requested interval; it is
for watching daemon liveness and queue counts, not an interactive TUI. `acd
logs` prints the daemon log exactly as stored: raw JSONL from the per-repo log
file. It does not include full AI prompt traces; those are written only when
`ACD_AI_PROMPT_TRACE=1` is enabled and a non-deterministic provider sends a
request. Inspect them with `acd prompt`. Use `acd doctor` or
`acd doctor --bundle` when you need the bundled diagnostics view, sanitized
paths, safe-ignore details, and log tail snippets for issue reports.

`acd doctor` human output includes:

```
      pending    : N
      blocked    : N
      last conflict : path/to/file.go  47s ago  "before-state mismatch for path/to/file.go"
      failed     : N
      failed blockers : N pending successors; run acd fix --dry-run
```

`acd doctor --json` and doctor bundles also include the active safe-ignore
patterns. By default ACD prunes generated dependency/cache trees before capture
or watcher descent: `node_modules/`, `target/`, `.venv/`, `venv/`,
`__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, and
`.gradle/`. This guard is internal to ACD and never edits `.gitignore`.
Set `ACD_SAFE_IGNORE=0` to restore the older behavior, or append additional
generated trees with `ACD_SAFE_IGNORE_EXTRA`, for example `dist/,build/`.
Those variables are read when the daemon starts. Stop and restart an existing
daemon before expecting safe-ignore changes to affect capture or watcher
pruning.

#### `acd status --json` shape

```json
{
  "repo": "/path/to/repo",
  "repo_hash": "abc123",
  "daemon": "running",
  "stale": false,
  "pid": 12345,
  "started_ts": 1746000000,
  "uptime_seconds": 300,
  "heartbeat_ts": 1746000300,
  "heartbeat_age_seconds": 2,
  "branch_ref": "refs/heads/main",
  "branch_generation_token": "rev:abc123def456 refs/heads/main",
  "clients": [
    {
      "session_id": "abc1...",
      "harness": "claude-code",
      "watch_pid": 9876,
      "last_seen_ts": 1746000295,
      "last_seen_age_seconds": 5,
      "ttl_remaining_seconds": 55
    }
  ],
  "pending_events": 2,
  "blocked_conflicts": 0,
  "failed_events": 1,
  "failed_blocking_pending": 1,
  "last_commit_oid": "deadbeef...",
  "last_commit_ts": 1746000250,
  "last_commit_message": "modify auth.go",
  "capture_errors": 0,
  "intent_strategy": {
    "strategy": "intent",
    "active": true,
    "window": 10,
    "recent_commits": 5,
    "defer_limit": 2,
    "deferred_events": 1,
    "max_defer_count": 1,
    "forced_aging_ready": 0
  },
  "decision_counts": {
    "captured": 8,
    "committed": 7,
    "intent_deferred": 1,
    "blocked": 1
  },
  "recent_decisions": [
    {
      "id": 42,
      "timestamp": 1746000301,
      "time": "2026-04-30T10:05:01Z",
      "kind": "blocked",
      "path": "internal/state/schema.go",
      "reason": "before-state mismatch",
      "event_seq": 12,
      "head_sha": "abc123...",
      "commit_oid": "deadbeef...",
      "branch_ref": "refs/heads/main",
      "branch_generation": 3,
      "action_taken": "blocked_conflict",
      "user_message": "Replay stopped before publishing this path.",
      "decision_ts": 1746000301.5
    }
  ],
  "decision_cursor": 42,
  "paused": true,
  "pause": {
    "source": "manual",
    "reason": "manual reset in progress",
    "set_at": "2026-04-30T10:00:00Z",
    "expires_at": "2026-04-30T10:10:00Z",
    "remaining_seconds": 42
  }
}
```

`failed_events` counts terminal failed replay rows. `failed_blocking_pending`
is non-zero when failed terminal rows have later pending successors hidden
behind the sequence barrier; inspect with `acd diagnose` and preview cleanup
with `acd fix --dry-run`.

`decision_counts`, `recent_decisions`, and `decision_cursor` are present when
the decision ledger exists and has rows. `recent_decisions` uses the same entry
shape as `acd events --json`; `decision_cursor` is the newest decision ID in
that recent set and can be passed to `acd events --since`.

`intent_strategy` is always present. In `event` mode it reports
`{"strategy":"event","active":false}` plus resolved planner defaults. In
`intent` mode it includes active window settings, pending deferral counts,
forced-aging readiness, and last planner error fields when available.

`paused` and `pause` are omitted when replay is not paused. The `pause` object fields:

| Field | Type | Description |
|---|---|---|
| `source` | string | `"manual"` — active operator pause; `"manual_expired"` — TTL elapsed but marker still on disk (run `acd resume --yes`); `"rewind_grace"` — daemon detected a same-branch rewind |
| `reason` | string | Human note from `acd pause --reason`. Omitted for `rewind_grace`. |
| `set_at` | string | RFC3339 timestamp when the marker was written. Omitted for `rewind_grace`. |
| `expires_at` | string | RFC3339 expiry. Omitted when no TTL was set. |
| `remaining_seconds` | int | Seconds until `expires_at`. `0` when `source` is `manual_expired`. Omitted when no `expires_at`. |

#### `acd list --json` shape

`acd list --json` wraps all known canonical Git worktree roots in a `repos`
array. Each entry adds `status`, `status_note`, and `stale_heartbeat` on top of
the pause fields:

```json
{
  "repos": [
    {
      "path": "/path/to/repo",
      "repo_hash": "abc123",
      "daemon": "running",
      "pid": 12345,
      "clients": 1,
      "last_seq": 7,
      "last_commit_oid": "deadbeef...",
      "heartbeat_age_seconds": 2.1,
      "pending_events": 0,
      "blocked_conflicts": 0,
      "status": "paused",
      "status_note": "manual; daemon stale 3h",
      "paused": true,
      "stale_heartbeat": true,
      "pause": {
        "source": "manual",
        "reason": "branch surgery",
        "set_at": "2026-04-30T10:00:00Z"
      }
    }
  ]
}
```

`status` string values:

| Value | Meaning |
|---|---|
| `"OK"` | Daemon running, no pause, no stale heartbeat |
| `"paused"` | Replay paused (operator or rewind grace). Takes priority over `stale`. |
| `"stale"` | Daemon heartbeat expired or PID dead, at least one live client present |
| `"missing"` | Repo directory or `state.db` not found on disk |
| `"unreadable"` | `state.db` exists but could not be opened |

`status_note` combines the pause source and stale information into a human-readable
string when both apply (e.g. `"manual; daemon stale 3h"`). `stale_heartbeat` is
`true` whenever the daemon heartbeat is expired regardless of whether the row is
also paused. `paused`, `stale_heartbeat`, and `pause` are omitted from JSON when
`false` / `nil`.

### Wake the daemon (reduce latency)

`acd` uses `fsnotify` for low-latency file-system events. When `fsnotify` is
unavailable or falls back to polling, `acd wake` can be called from a harness
hook to nudge the daemon without waiting for the next poll tick:

~~~bash
acd wake --session-id "$ACD_SESSION_ID"
~~~

This refreshes the session's `last_seen_ts` heartbeat (keeping the client row
alive) and sends `SIGUSR1` to the daemon process, which triggers an immediate
capture + replay pass. Harnesses that call `acd wake` on `PostToolUse` events
reduce commit latency to near-zero.

### Pause and resume replay

Use a manual pause before branch surgery that should not be immediately
replayed:

~~~bash
acd pause --repo . --reason "manual reset" --yes
# reset, rebase, inspect, or stage changes
acd resume --repo . --yes
acd wake --repo . --session-id "$ACD_SESSION_ID"
~~~

`acd pause --ttl 10m --yes` creates a marker that expires automatically for
replay purposes. Expired markers remain on disk until `acd resume --yes`
removes them.

### Restart the daemon with updated env

Environment variables (`ACD_AI_PROVIDER`, `ACD_AI_API_KEY`, etc.) are read at
daemon startup. To apply changes during manual use:

~~~bash
acd stop                    # stop the current repo daemon
# … set new env vars in your shell or harness …
acd start                   # start the current repo daemon with the new env
~~~

If the daemon does not exit after the graceful stop window, use
`acd stop --force`. Harness hooks should keep using `--session-id` with
`acd start`, `acd wake`, and `acd stop`; that mode refreshes client heartbeats
and makes `acd stop --session-id "$ACD_SESSION_ID"` refcount-aware so the daemon
stays alive while peer harness sessions remain. Use `acd stop --all` when you
need to stop every registered repo daemon.

The queue is persisted in SQLite and survives the restart. The daemon will
drain pending events with the new provider on its first poll tick.

### Prune stale registry entries

~~~bash
acd gc
~~~

Removes central-registry entries for repos that no longer exist on disk, whose
`state.db` is missing, or whose daemon has been dead for more than 30 days.
Before pruning, `acd gc` also merges legacy duplicate rows that identify the
same repo by canonical Git toplevel or by shared `state.db` path; merges are
reported in `acd gc --json` under a `merged[]` array of
`{kept_path, dropped_path, reason}` entries (reason is `same-git-toplevel` or
`same-state-db`). Current `acd start` calls write canonical Git worktree roots,
so `acd gc` is only needed for stale or legacy rows. It does not touch the git
object database.

---

## Diagnosing a stalled queue

Use this checklist when commits stop appearing. The shorter, task-oriented
version lives in [user-workflows.md](user-workflows.md).

1. **Check the counts.**

   ~~~bash
   acd status
   acd events
   ~~~

   - `Pending events: 0` and `Blocked conflicts: 0` → the queue is empty;
     check whether the harness is calling `acd start` / `acd wake` correctly.
   - `Pending events: N` → events are waiting; check whether the daemon is
     running (`Daemon: running`) and its heartbeat is recent.
   - `Blocked conflicts: N` → see step 3.

2. **Check daemon liveness.**

   ~~~bash
   acd status              # Daemon field: running / stale / stopped
   acd list --watch        # all registered repos, refreshed until Ctrl-C
   acd doctor              # daemon_alive boolean per repo
   ~~~

   A `stale` daemon has a recent-looking heartbeat but a dead PID (crashed
   without updating state). Run `acd stop` then `acd start` to restart it; use
   `acd stop --force` if the graceful stop path cannot clear the stale entry.

   To inspect the daemon's own messages while you restart or wait for replay,
   use raw JSONL log tailing:

   ~~~bash
   acd logs --lines 200
   acd logs --follow
   ~~~

   `acd doctor` still includes bundled diagnostics and log tail snippets; prefer
   it for issue reports or when you need sanitized, summarized context.

3. **Ask why one path or commit behaved that way.**

   ~~~bash
   acd explain --path path/to/file
   acd explain --commit HEAD
   ~~~

   `explain` summarizes recent decisions and recommends the next command. For
   example, `handled_external` means another commit already contained the
   captured after-state, while `protected` means a sensitive or generated path
   was intentionally left alone.

4. **Check for stale live-index repair candidates.**

   ~~~bash
   acd doctor
   acd fix --dry-run
   acd fix --yes
   ~~~

   A legacy stale-index shape looks like `D  path` plus `?? path` even though
   `HEAD:path` and the worktree file match. Current daemon startup and
   `acd fix --yes` (via the retarget_stale_anchor action) repair only
   ACD-owned published paths proved from `capture_events`/`capture_ops`,
   current `HEAD` ancestry, and matching worktree content. Ambiguous same-path
   staged work is skipped; use normal git inspection to decide whether it is
   user intent.

5. **Resolve blocked conflicts.**

   ~~~bash
   acd doctor      # last conflict/failure plus failed blocker counts when present
   ~~~

   `blocked_conflict` and `failed` events are terminal and may hold later
   pending rows behind a sequence barrier. `status`, `diagnose`, and `doctor`
   surface failed terminal barriers as failed event counts and
   `failed_blocking_pending`. Use built-in recovery before editing SQLite
   directly:

   ~~~bash
   acd diagnose --repo .
   acd fix --dry-run
   acd fix --force --dry-run
   ~~~

   If the dry-run plan is correct, rerun with `--yes`. `acd fix` covers safe
   cleanup (resolve_already_landed_barrier promotes blocked rows whose
   captured after-state already exists at HEAD; retarget_stale_anchor handles
   branch surgery; delete_obsolete_barrier removes barriers without pending
   successors; mark_external_published settles externally-handled rows).
   `acd fix --force --yes` adds purge_barrier_with_successors for terminal
   barriers that still block later pending rows.

   After clearing the blockers, trigger a replay:
   `acd wake --session-id "$ACD_SESSION_ID"`.

6. **Check fsnotify mode.**

   ~~~bash
   acd doctor --json | python3 -c "import json,sys; [print(r['path'], r.get('fsnotify_mode'), r.get('fsnotify_fallback_reason')) for r in json.load(sys.stdin)['repos']]"
   ~~~

   If `fsnotify_mode` is `poll`, events are captured on a timer rather than
   immediately. Increase `acd wake` call frequency in the harness or investigate
   why native watching was unavailable (Linux: check `inotify_max_user_watches`
   via `acd doctor`).

7. **Check AI provider and intent-planner status.**

   If commits are appearing but messages look generic, the AI provider may be
   falling back to deterministic. If `ACD_COMMIT_STRATEGY=intent` is active and
   groups are not forming, inspect planner deferrals and errors:

   ~~~bash
   acd status --json
   acd events --watch
   ~~~

   Set `ACD_AI_PROVIDER=deterministic` explicitly if you want the default
   behavior, or check `ACD_AI_API_KEY` / network connectivity and restart the
   daemon.

---

## Revert workflows

This section describes what happens to the `acd` queue for each common revert
pattern. For all of them, the safest approach is to pause `acd` first:

~~~bash
acd pause --repo . --reason "branch surgery" --yes
# … do the revert / reset / rebase …
acd resume --repo . --yes
acd wake --repo . --session-id "$ACD_SESSION_ID"
~~~

If you do not pause first, the daemon handles most scenarios automatically — but
read the sub-sections below to understand where it still blocks.

### Revert via `git revert` (forward commit)

`git revert <commit>` creates a *new* commit that inverts the changes of the
target commit. From `acd`'s perspective:

- The revert commit is an ordinary fast-forward; branch generation is unchanged.
- Any pending `acd` events that captured the original changes now face a
  before-state probe where `HEAD` already shows the inverse — the desired
  final state matches the current `HEAD` tree.
- `alreadyPublishedAtHEAD` (`internal/daemon/replay.go`) returns `true`;
  the event is marked `published` with `commit_oid = HEAD` and no new commit is
  created. Trace decision: `already_published_by_external_committer`.

If you plan to make additional edits immediately after the revert, pause `acd`
first to prevent the revert commit itself from being double-captured as a
phantom change.

### Revert via `git reset --soft` or `--mixed`

Both variants move `HEAD` backward on the same branch ref without touching the
working tree (`--soft`) or touching staged state but not the tree (`--mixed`).
`acd` detects the backward HEAD movement as a **rewind** and fires rewind grace:

- `maybeSetRewindGrace` (`internal/daemon/branch_token.go:327`) writes
  `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS` (default
  60 seconds).
- For the duration of the grace window, **both capture and replay are paused**
  (`internal/daemon/daemon.go:906-945`). `acd` will not enqueue the transient
  worktree state produced by fsnotify events during the rewind, which prevents
  the post-grace replay from resurrecting work the operator just rewound.
- After the grace window expires the daemon resumes normally, picks up the
  current HEAD, and reseeds shadow state if the branch generation bumped.

During the grace window `acd status` shows:

```json
"paused": true,
"pause": { "source": "rewind_grace", "remaining_seconds": 42 }
```

Operator workflow: re-stage and re-edit your files during the grace window.
`acd` will capture the clean post-rewind state after the grace expires.

### Revert via `git reset --hard`

`git reset --hard` additionally overwrites the working tree. `acd` handles this
identically to `--soft`/`--mixed`: a rewind is detected, rewind grace fires,
and both capture and replay are paused for `ACD_REWIND_GRACE_SECONDS` seconds.

Re-edit the files you want to keep during the grace window. The daemon will
pick up a clean diff when the grace expires.

### Revert + delete: rescued by idempotent publish

Before this branch, a `delete` op queued while a file still existed at the
capture time would become `blocked_conflict` if the file was already gone by
replay time. After this branch, `alreadyPublishedAtHEAD`
(`internal/daemon/replay.go`, `alreadyPublishedAtHEAD`) checks each `delete` op:

- If the path is **absent** in the current `HEAD` tree, the delete is already
  accomplished; the event settles as `published` against `HEAD` without a new
  commit.
- If the path is present as any non-blob entry (tree, submodule), the probe
  returns `false` and the event becomes `blocked_conflict` as before — a real
  divergence rather than a parallel publish.

This makes the classic scenario (operator deletes a file, external tool commits
the deletion, `acd`'s queued delete event would otherwise block) self-healing in
the common case.

### Editing an old commit via `git rebase -i`

An interactive rebase creates one or more git operation markers under
`.git/rebase-merge/` or `.git/rebase-apply/`. The daemon detects these on every
poll tick and activates `operation_in_progress`:

- Both capture and replay are paused (`internal/daemon/daemon.go:794`).
  Trace event: `daemon.pause` with `decision: "paused"`.
- While paused, `acd status` shows `Daemon: running` but no new commits appear.
- After the rebase completes and the markers are removed, the daemon resumes.
  It classifies the new HEAD as **Diverged** (branch generation bumps) because
  the rebase rewrites history.
- Pre-rebase `pending` rows for the old generation are **dropped** on the next
  poll tick (stale events cannot replay on top of a rewritten branch).
  `blocked_conflict` and `failed` rows from before the rebase are **preserved**
  while the old branch ref still exists. If the old ref has been deleted, ACD
  prunes stale rows for that old ref/generation unless
  `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` is set.
- `shadow_paths` is reseeded from the new HEAD via `BootstrapShadow`, and
  capture resumes from the clean post-rebase state.

---

## Why is AI on deterministic fallback?

The daemon always falls back to the `deterministic` provider. A message
generated by the fallback has `Result.Source = "deterministic"` and typically
follows the pattern `<op> <basename>` (e.g., `modify auth.go`). Reasons
the primary provider is skipped:

| Symptom | Likely cause | Fix |
|---|---|---|
| Generic `modify file.go` messages | `ACD_AI_PROVIDER` unset | Set `ACD_AI_PROVIDER=openai-compat` and `ACD_AI_API_KEY` |
| Generic messages after working AI | Daemon not restarted after env change | `acd stop` + `acd start` |
| Generic messages + network provider set | Missing or expired API key | Check `ACD_AI_API_KEY`; restart daemon |
| Generic messages on every event | Subprocess plugin crash / timeout | Check plugin binary on `$PATH`; see `acd doctor` log tail |
| AI sees empty diff | Op has no `before_oid`/`after_oid` (e.g. oversize file) | Expected; deterministic fallback is correct |

See [docs/ai-providers.md](ai-providers.md) for the full provider reference.

---

## Trace event classes

Enable `ACD_TRACE=1` to write JSONL decision records to `<gitDir>/acd/trace/`
(see `CLAUDE.md` Trace log format for the full record schema). Every record
has an `event_class` field that identifies the decision point. Current event
classes:

| `event_class` | When emitted | Key `input` fields | Key `output` fields |
|---|---|---|---|
| `bootstrap_shadow.reseed` | Shadow state reseeded after Diverged or at startup | — | `rows` |
| `capture.classify` | After comparing live worktree to shadow state | — | `ops`, `walked_files`, `oversize`, `errors` |
| `capture.event` | Each op persisted to `capture_events` (decision `appended`) or dropped at queue cap (decision `dropped`) | `op`, `path`, `old_path`, `fidelity` | `seq` (appended) or `pending_depth`, `cap` (dropped) |
| `capture.pause` | Capture pass skipped because replay is paused | — | `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `replay.commit` | Capture event published as a git commit, or idempotent-publish at HEAD | `operation`, `path` | `commit`, `parent` |
| `replay.self_heal` | Blocked row promoted to `published` because HEAD already reflects the captured after-state (daemon-side `probeBlockedSelfHeal`) | `operation`, `path` | `commit`, `head`, `source_head`, `branch_ref`, `generation` |
| `replay.conflict` | Event becomes `blocked_conflict` (before-state mismatch, CAS failure, or generation mismatch) | `operation`, `path` | `expected_sha`, `actual_sha`, `ref` |
| `replay.failed` | Event becomes `failed` (bad op data, ancestry error, write-tree failure) | `operation`, `path` | — |
| `replay.update_ref` | Each `git update-ref` attempt during commit publish (per-retry) | — | `attempt`, `max_attempts`, `retry`, `ref`, `commit`, `expected_sha`, `actual_sha` |
| `replay.live_index` | Path-scoped live-index reconciliation after publish or startup repair | `operation`, `path` | `decision`, `reason` |
| `replay.pause` | Replay drain skipped because paused (manual or rewind grace) | — | `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `intent.batch_wait` | Intent replay skips planning while waiting for more pending captures or the age trigger | `visible_pending`, `min_pending`, `oldest_seq`, `oldest_age_seconds`, `max_pending_age_seconds`, `window` | — |
| `intent.planner.input` | A normal or forced intent window is about to be offered to the planner | `offered`, `forced_aging`, `include_captured_diffs`, `latest_commit_present`, `path_commit_context_count`, `window`, `min_pending`, `max_pending_age_seconds`, `recent_commits`, `defer_limit` | — |
| `intent.planner.output` | The planner returned a syntactically valid plan | `offered_seqs` | `selected_seqs`, `deferred_seqs`, `source` |
| `intent.planner.validation_failed` | A planner result failed safety validation before git was touched | `offered_seqs` | Top-level `error` contains the validation message |
| `intent.forced_aging` | A repeatedly deferred capture is forced through a one-item planning window | `seq`, `path`, `defer_count`, `defer_limit` | — |
| `branch_token.transition` | HEAD movement classified at startup or per poll tick | `previous`, `current` | `prev_generation`, `new_generation`, `dropped_pending` |
| `daemon.pause` | Git operation in progress (rebase, merge, cherry-pick, bisect) detected (decision `paused`) or cleared (decision `resumed`) | `operation` | — |

`capture.pause` and `replay.pause` are emitted once per daemon poll cycle while
the pause is active; they share the same output shape as the `pause` object in
`acd status --json` and `acd list --json`.

`ACD_TRACE` and `ACD_AI_PROMPT_TRACE` are separate diagnostics. `ACD_TRACE`
writes daemon decision summaries to `<gitDir>/acd/trace/` and intentionally
avoids captured source diffs, full AI prompts, and provider request envelopes.
`ACD_AI_PROMPT_TRACE` writes local AI prompt/request records to
`<gitDir>/acd/prompt-trace/` only when a non-deterministic provider sends a
request; deterministic event messages and deterministic fallback intent plans do
not create prompt records. Prompt trace files are daily JSONL logs and are not
pruned automatically. The writer keeps a 256-record in-memory buffer and drops
the oldest buffered record if it falls behind. Records are post-redaction and
truncation but may still contain source code and provider responses. Use
`acd prompt --last` for the newest request, or `acd prompt --seq <seq>` to
inspect either an event prompt or an intent planner window that offered that
seq.

---

## Multi-tool coexistence

See [docs/multi-tool.md](multi-tool.md) for a full guide on running `acd`
alongside another auto-committer (Claude Code Automatic Atomic Commits,
Codex ACD hook, or any process that lands commits on the active branch).

Summary: `acd` uses its idempotent publish probe
(`alreadyPublishedAtHEAD`, `internal/daemon/replay.go`) to detect when an
external tool already landed a queued event. The event is settled as `published`
against `HEAD` with no new commit, and the trace record carries
`decision: "already_published_by_external_committer"`. Real before-state
mismatches (mode divergence, ancestry gap, symlink target mismatch) still
produce `blocked_conflict` and require operator resolution.
