# acd adapter: codex

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Write the snippet straight to disk: `acd setup codex --raw > ~/.codex/hooks.json`. Codex now reads `hooks.json` before `~/.codex/config.toml`. (`acd setup codex` without `--raw` prints the same JSON wrapped in `// `-prefixed instructions; copy only the JSON block if you go that route — JSON does not allow comments.)

   **Overwrite warning:** the shell redirect above replaces the entire file. If
   you have custom non-acd hooks in `~/.codex/hooks.json`, back up that file
   first and then merge the acd JSON block in manually rather than using `>`.

3. Restart Codex.
4. **Approve the hooks.** Codex flags every newly-added hook entry as "review required" and refuses to run them until you approve. On first launch you will see `5 hooks need review before they can run. Open /hooks to review them.` Run `/hooks` inside Codex and approve all five (`SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PostToolUse`, `Stop`). Until you approve, the daemon never starts and `acd status` will show no Codex client.

   **Re-approval on update:** Codex re-flags every hook entry as review-required
   whenever `hooks.json` content changes — even if only the acd block was
   re-written. After any `acd setup codex --raw > ~/.codex/hooks.json` re-run
   (including migrations), open Codex and run `/hooks` again to approve the
   updated entries. Until you do, the daemon will not start for new sessions.

   Run `acd doctor` to check whether your installed snippet is current; it warns
   when active hooks are missing `acd start` or `acd wake` and shows the
   remediation command.

If you previously installed the legacy TOML snippet, delete the
`# acd-managed: true` block from `~/.codex/config.toml` or
`~/.config/codex/config.toml`. Codex merges every hook source it finds
(`hooks.json` and inline `[hooks]` in `config.toml` both fire), so leaving the
legacy block in place causes every event to fire twice — doubled
`acd start`/`acd wake`/`acd touch` per turn. `acd doctor` warns when the JSON
file and a legacy TOML config both carry an acd marker.

Note: Codex deprecated `[features].codex_hooks = true` in favor of
`[features].hooks = true`. The new `hooks.json` install does not need a
`[features]` block at all. If you keep a legacy TOML install for any reason,
rename the feature flag to silence the deprecation warning.

## Wired events

`hooks.json` registers all five Codex hook events:

- `SessionStart` -> `acd start` (timeout 15s)
- `UserPromptSubmit` -> idempotent `acd start`, then `acd wake` (timeout 15s)
- `PreToolUse` -> idempotent `acd start`, then `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 15s)
- `PostToolUse` -> idempotent `acd start`, then `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 15s)
- `Stop` -> `acd touch` (timeout 5s)

`Stop` calls `acd touch` (mirrors the claude-code adapter) so the daemon is
not killed mid-replay drain. The refcount sweep on the `watch_pid` still
cleans up once Codex exits.

The active wake hooks call `acd start` first so a later prompt or tool event can
recover if you manually ran `acd stop` while the Codex session stayed open.

The repo path is read from the JSON `cwd` field on stdin (consumed in one
pass via `acd hook-stdin-extract session_id cwd?`). When `cwd` is missing,
the hook falls back to the hook process working directory; `CODEX_PROJECT_DIR`
is no longer required.

## Verify

- Open Codex in any git repo
- From another shell, run `acd status`
- One client with `harness=codex` should appear
- If a hook fails, inspect `~/.local/state/acd/codex-hook.log`

## Uninstall

See [uninstall.md](uninstall.md).
