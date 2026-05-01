# Capture, Replay, and Conflict Resolution

This document explains how `acd` stores changes, turns them into commits, and
how to diagnose and fix a stalled queue.

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

The scratch index is deleted when the pass returns. Every new pass creates a
fresh tempfile, so a crash mid-pass never poisons the next one.

---

## Branch-generation safety

Every captured event records the branch ref and a generation counter at the
moment of capture. The daemon classifies each HEAD movement as:

| Transition | Classification | Effect on queue |
|---|---|---|
| New HEAD descends from previous HEAD on the same branch ref | Fast-forward | Generation unchanged; queue remains valid |
| New HEAD does NOT descend from previous HEAD, or branch ref changes even at the same SHA | Diverged (rebase / reset / branch-switch) | Generation bumped; old-generation events become `blocked_conflict` at replay time |
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

When a network-bound AI provider is configured (`openai-compat` or
`subprocess:<name>`), `acd` reconstructs a unified diff from the captured
`before_oid` / `after_oid` blobs rather than inspecting the live worktree.
This means:

- The diff reflects exactly what was captured, even if the file has changed
  many times since.
- The diff is capped at `DiffCap` (4000 bytes) while it is rendered. Long diffs
  stop at a line boundary while preserving the diff header(s) so the model
  still sees which file is being described.
- `create` and `delete` ops use git's well-known empty-blob OID
  (`e69de29bb2d1d6434b8b29ae775ad8c2e48c5391`) for the missing side.

Diff reconstruction is opt-in via `ACD_AI_SEND_DIFF=1`. The deterministic
provider declares that it does not need diffs, so default replay skips
reconstruction entirely and only builds diff text for providers that can use it.

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

---

## Operator commands

### Inspect the queue

~~~bash
acd status              # pending_events + blocked_conflicts count for cwd repo
acd status --json       # machine-readable version
acd list                # PENDING + BLOCKED columns across all repos
acd doctor              # full diagnostics, including last_conflict path + age + error
acd doctor --bundle     # write a zip to ~/Downloads for issue reports
~~~

`acd doctor` human output includes:

```
      pending    : N
      blocked    : N
      last conflict : path/to/file.go  47s ago  "before-state mismatch for path/to/file.go"
```

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
  "last_commit_oid": "deadbeef...",
  "last_commit_ts": 1746000250,
  "last_commit_message": "modify auth.go",
  "capture_errors": 0,
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

`paused` and `pause` are omitted when replay is not paused. The `pause` object fields:

| Field | Type | Description |
|---|---|---|
| `source` | string | `"manual"` — active operator pause; `"manual_expired"` — TTL elapsed but marker still on disk (run `acd resume --yes`); `"rewind_grace"` — daemon detected a same-branch rewind |
| `reason` | string | Human note from `acd pause --reason`. Omitted for `rewind_grace`. |
| `set_at` | string | RFC3339 timestamp when the marker was written. Omitted for `rewind_grace`. |
| `expires_at` | string | RFC3339 expiry. Omitted when no TTL was set. |
| `remaining_seconds` | int | Seconds until `expires_at`. `0` when `source` is `manual_expired`. Omitted when no `expires_at`. |

#### `acd list --json` shape

`acd list --json` wraps all known repos in a `repos` array. Each entry adds
`status`, `status_note`, and `stale_heartbeat` on top of the pause fields:

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
daemon startup. To apply changes:

~~~bash
acd stop --force            # kill the current daemon
# … set new env vars in your shell or harness …
acd start --session-id "$ACD_SESSION_ID" --harness claude-code
~~~

The queue is persisted in SQLite and survives the restart. The daemon will
drain pending events with the new provider on its first poll tick.

### Prune stale registry entries

~~~bash
acd gc
~~~

Removes central-registry entries for repos that no longer exist on disk, whose
`state.db` is missing, or whose daemon has been dead for more than 30 days.
Does not touch the git object database.

---

## Diagnosing a stalled queue

Use this checklist when commits stop appearing:

1. **Check the counts.**

   ~~~bash
   acd status
   ~~~

   - `Pending events: 0` and `Blocked conflicts: 0` → the queue is empty;
     check whether the harness is calling `acd start` / `acd wake` correctly.
   - `Pending events: N` → events are waiting; check whether the daemon is
     running (`Daemon: running`) and its heartbeat is recent.
   - `Blocked conflicts: N` → see step 3.

2. **Check daemon liveness.**

   ~~~bash
   acd status      # Daemon field: running / stale / stopped
   acd doctor      # daemon_alive boolean per repo
   ~~~

   A `stale` daemon has a recent-looking heartbeat but a dead PID (crashed
   without updating state). Run `acd stop --force` then `acd start` to
   restart it.

3. **Resolve blocked conflicts.**

   ~~~bash
   acd doctor      # last conflict: <path> <age> "<error>"
   ~~~

   `blocked_conflict` events are terminal and may hold later pending rows behind
   a sequence barrier. Resolution options:

   - If the error is a **generation mismatch** (after a rebase or reset): the
     captured events are stale. Remove them manually from `capture_events` (set
     `state = 'failed'` in the SQLite DB) or restart the daemon — the next
     capture pass will record the current worktree state from scratch.
   - If the error is a **before-state mismatch**: the file was modified outside
     `acd`'s knowledge. The same manual resolution applies.
   - After clearing the blockers, trigger a replay: `acd wake --session-id "$ACD_SESSION_ID"`.

4. **Check fsnotify mode.**

   ~~~bash
   acd doctor --json | python3 -c "import json,sys; [print(r['path'], r.get('fsnotify_mode'), r.get('fsnotify_fallback_reason')) for r in json.load(sys.stdin)['repos']]"
   ~~~

   If `fsnotify_mode` is `poll`, events are captured on a timer rather than
   immediately. Increase `acd wake` call frequency in the harness or investigate
   why native watching was unavailable (Linux: check `inotify_max_user_watches`
   via `acd doctor`).

5. **Check AI provider status.**

   If commits are appearing but messages look generic, the AI provider may be
   falling back to deterministic. Set `ACD_AI_PROVIDER=deterministic` explicitly
   if you want the default behavior, or check `ACD_AI_API_KEY` / network
   connectivity and restart the daemon.

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
(`internal/daemon/replay.go:643`) checks each `delete` op:

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
  for operator inspection.
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
| Generic messages after working AI | Daemon not restarted after env change | `acd stop --force` + `acd start` |
| Generic messages + network provider set | Missing or expired API key | Check `ACD_AI_API_KEY`; restart daemon |
| Generic messages on every event | Subprocess plugin crash / timeout | Check plugin binary on `$PATH`; see `acd doctor` log tail |
| AI sees empty diff | Op has no `before_oid`/`after_oid` (e.g. oversize file) | Expected; deterministic fallback is correct |

See [docs/ai-providers.md](ai-providers.md) for the full provider reference.

---

## Trace event classes

Enable `ACD_TRACE=1` to write JSONL decision records to `<gitDir>/acd/trace/`
(see `CLAUDE.md` Trace log format for the full record schema). Every record
has an `event_class` field that identifies the decision point. The complete
enumeration:

| `event_class` | When emitted | Key `input` fields | Key `output` fields |
|---|---|---|---|
| `bootstrap_shadow.reseed` | Shadow state reseeded after Diverged or at startup | — | `rows` |
| `capture.classify` | After comparing live worktree to shadow state | — | `ops`, `walked_files`, `oversize`, `errors` |
| `capture.event` | Each op persisted to `capture_events` (decision `appended`) or dropped at queue cap (decision `dropped`) | `op`, `path`, `old_path`, `fidelity` | `seq` (appended) or `pending_depth`, `cap` (dropped) |
| `capture.pause` | Capture pass skipped because replay is paused | — | `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `replay.commit` | Capture event published as a git commit, or idempotent-publish at HEAD | `operation`, `path` | `commit`, `parent` |
| `replay.conflict` | Event becomes `blocked_conflict` (before-state mismatch, CAS failure, or generation mismatch) | `operation`, `path` | `expected_sha`, `actual_sha`, `ref` |
| `replay.failed` | Event becomes `failed` (bad op data, ancestry error, write-tree failure) | `operation`, `path` | — |
| `replay.update_ref` | Each `git update-ref` attempt during commit publish (per-retry) | — | `attempt`, `max_attempts`, `retry`, `ref`, `commit`, `expected_sha`, `actual_sha` |
| `replay.pause` | Replay drain skipped because paused (manual or rewind grace) | — | `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `branch_token.transition` | HEAD movement classified at startup or per poll tick | `previous`, `current` | `prev_generation`, `new_generation`, `dropped_pending` |
| `daemon.pause` | Git operation in progress (rebase, merge, cherry-pick, bisect) detected (decision `paused`) or cleared (decision `resumed`) | `operation` | — |

`capture.pause` and `replay.pause` are emitted once per daemon poll cycle while
the pause is active; they share the same output shape as the `pause` object in
`acd status --json` and `acd list --json`.

---

## Multi-tool coexistence

See [docs/multi-tool.md](multi-tool.md) for a full guide on running `acd`
alongside another auto-committer (Claude Code Automatic Atomic Commits,
Codex ACD hook, or any process that lands commits on the active branch).

Summary: `acd` uses its idempotent publish probe
(`internal/daemon/replay.go:643`, `alreadyPublishedAtHEAD`) to detect when an
external tool already landed a queued event. The event is settled as `published`
against `HEAD` with no new commit, and the trace record carries
`decision: "already_published_by_external_committer"`. Real before-state
mismatches (mode divergence, ancestry gap, symlink target mismatch) still
produce `blocked_conflict` and require operator resolution.
