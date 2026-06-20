# acd adapter: cursor

User-global Cursor agent hooks only. This document is the harness contract for
implementers (`acd setup cursor`, inline hook commands, adapter detection, and
E2E).

## Scope

| In scope | Out of scope |
|----------|--------------|
| `~/.cursor/hooks.json` (canonical install path) | Repo-local `.cursor/hooks.json` |
| Inline lifecycle command bodies | `acd setup cursor --apply` merge installer |
| Agent lifecycle events below | Tab hooks (`beforeTabFileRead`, `afterTabFileEdit`) |
| `watch-pid 0` (no harness refcount) | Project hooks checked into repositories |

Cursor runs user hooks from `~/.cursor/`. The shipped commands are inline shell
commands, so no auxiliary script path is required.

## Layout

After install:

~~~text
~/.cursor/hooks.json          # version 1, hooks event -> command entries
~~~

Hook log (convention, same as other harnesses):
`~/.local/state/acd/cursor-hook.log`.

## Stdin precedence

Cursor delivers one JSON object on stdin per hook invocation. Inline hook
commands use `acd hook-cursor-extract` to resolve fields in this order.

### Session id

| Priority | Source | Rule |
|----------|--------|------|
| 1 | `conversation_id` | Required top-level string. This is the acd `--session-id` for the Cursor conversation. |

There is no `session_id` alias on Cursor stdin. When `conversation_id` is
missing or `hook-cursor-extract` fails for any reason, the hook command logs and
exits **0** (fail-soft, same as Codex `hook-stdin-extract` failures) so Cursor
is not blocked.

### Repository path

| Priority | Source | Rule |
|----------|--------|------|
| 1 | `workspace_roots` | JSON array of directory paths. Walk in array order; use the first entry where `git rev-parse --show-toplevel` succeeds. Use that top-level path as `--repo`. |
| 2 | `cwd` | Top-level string on tool-related events (`postToolUse`, `afterFileEdit`, and any other event that includes it). Used when no `workspace_roots` entry resolves to a git worktree. |
| 3 | Process cwd | Final fallback: the hook process working directory (`$PWD`) when neither rule above yields a repo. |

Notes:

- `workspace_roots` may list multiple folders (monorepo roots, worktrees). Only
  the **first** resolvable git root wins; do not merge or union paths.
- All resolved repo paths are canonicalized (`EvalSymlinks`) so macOS `/var` and
  `/private/var` aliases match across hooks in one session.
- When `workspace_roots` is missing, empty, or has no git root, fall through to
  `cwd` then `$PWD`.
- When the resolved path is not inside a git worktree, `start`/`wake`/`flush`
  log and exit **0** without calling acd (repo autodiscovery skip). `stop`
  still attempts deregistration when extract succeeds.

### watch-pid

Always pass `--watch-pid 0` to `acd start`. Cursor does not provide a stable
parent PID for refcount sweep; session end is driven by `sessionEnd` ->
`acd stop`, not Codex-style `PPID` watching.

## Wired events

Register exactly these Cursor agent hook events in `~/.cursor/hooks.json`
(schema `version: 1`, camelCase event names):

| Cursor event | acd action |
|--------------|------------|
| `sessionStart` | `acd start --harness cursor --session-id <conversation_id> --watch-pid 0 --repo <resolved>` |
| `postToolUse` | Idempotent `acd start` then `acd wake` (same session/repo) |
| `afterFileEdit` | Same as `postToolUse` |
| `stop` | `acd flush --logical` (prompt-end commit boundary; bypasses intent batch wait when drained) |
| `sessionEnd` | `acd stop --session-id <conversation_id> --repo <resolved>` |

Behavioral alignment with Claude Code / OpenCode / Pi:

- **Active hooks** (`postToolUse`, `afterFileEdit`) call `start` before `wake` so
  a later tool can recover if you ran `acd stop` while the Cursor session stayed
  open.
- **`stop`** maps to `acd flush --logical`, not `acd touch` (Codex uses touch
  because `Stop` fires every assistant turn).
- **`sessionEnd`** deregisters the session; do not rely on `watch-pid` cleanup.
- **`postToolUse` + `afterFileEdit`** both wire `wake`; a single edit may fire
  both events and run start+wake twice. This is intentional for v1; narrow with
  matchers only if Cursor documents overlapping events as noisy.

Do **not** wire `preToolUse`, `subagentStart`, `beforeShellExecution`, or Tab
events in v1.

Suggested timeouts (implementer default): `sessionStart` / active hooks 15s;
`stop` / `sessionEnd` 5s.

## Install

1. Install acd:
   `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Install `~/.cursor/hooks.json`:

   - **Merge (recommended):** if you already have custom hooks, copy the five
     event entries from `acd setup cursor --raw` (or `templates/cursor/hooks.json`)
     into your file. Ensure top-level `"version": 1` and keep the ACD hook
     commands intact so `acd doctor` can recognize the install.
   - **Replace:** only when the file has no non-acd hooks:

     ~~~bash
     acd setup cursor --raw > ~/.cursor/hooks.json
     ~~~

   **Overwrite warning:** redirecting `acd setup cursor --raw > ~/.cursor/hooks.json`
   replaces the **entire** file. Back up first and merge manually if you have
   non-acd hooks.

3. Restart Cursor (or rely on hooks hot-reload; restart if hooks do not load).
4. Approve hooks in Cursor **Settings → Hooks** when prompted.

Do **not** commit repo-local `.cursor/hooks.json` for acd; the canonical path
is user-global only (same policy as Claude Code, OpenCode, and Pi).

Repo autodiscovery is enabled by default. If you disable it with
`repo_lifecycle.autodiscovery` in `~/.config/acd/config.json` or with
`ACD_REPO_AUTODISCOVERY=disabled`, Cursor hooks in unregistered repos skip
without creating `.git/acd`. Run `acd repo init` in each repo you want Cursor
to manage, or temporarily set `ACD_REPO_AUTODISCOVERY=enabled` before starting
the session.

`acd wake` refreshes the heartbeat and nudges capture/replay; it does not bypass
`ACD_INTENT_MIN_PENDING` or `ACD_INTENT_MAX_PENDING_AGE`. Only drained
`acd flush --logical` (from `stop`) sets `IntentBypassBatchWait`.

## Verify

- Open Cursor Agent in any git repo (or a workspace whose `workspace_roots`
  includes a git root).
- From another shell: `acd status --repo <that-repo>`
- Expect one client with `harness=cursor` and `session-id` matching
  `conversation_id`.
- On hook failure, inspect `~/.local/state/acd/cursor-hook.log`.
- Run `acd doctor` for template drift, missing lifecycle script, and missing
  `start`/`wake` wiring.

## Uninstall

See [uninstall.md](uninstall.md).
