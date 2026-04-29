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
      holds. If they disagree the event is `blocked_conflict` and the pass
      halts — later events are NOT replayed because they were captured assuming
      this one would land first.

   c. **Apply ops.** The ops are fed to `git update-index --index-info` against
      the scratch index (via `GIT_INDEX_FILE`), advancing it atomically.

   d. **Build tree and commit.** `git write-tree` produces a tree OID from the
      updated scratch index. A commit is created via `git commit-tree` with the
      AI or deterministic message. The new commit becomes the parent for the
      next event in the pass.

   e. **Advance the branch ref.** `git update-ref` atomically advances the
      branch ref from `parent` to the new commit OID (compare-and-swap).
      If the CAS fails (someone else moved the ref), the event is
      `blocked_conflict` and the pass halts.

   f. **Record the outcome.** The event row is updated to `published` with the
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
generation. Only external branch surgery does.

At startup the daemon classifies the persisted `branch.head` against the
current HEAD before overwriting metadata. If the branch was reset or rebased
while the daemon was offline, generation bumps and `shadow_paths` is reseeded
before capture resumes. Detached HEAD is treated as a pause: `acd start`
refuses to register, the daemon stamps `detached_head_paused`, and capture plus
replay stay disabled until HEAD is attached to a branch again.

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
