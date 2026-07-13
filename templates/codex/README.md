# acd adapter: codex

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Ensure Codex lifecycle hooks are enabled. They are on by default, but if
   your `~/.codex/config.toml` pins feature flags, use the canonical key:
   ~~~toml
   [features]
   hooks = true
   ~~~
   Do not set `hooks = false`; that disables Codex lifecycle hooks.
3. Write the snippet straight to disk: `acd setup codex --raw > ~/.codex/hooks.json`. The raw output is strict Codex-compatible JSON with only `hooks` at the top level. Codex reads this file before inline hook configuration. `acd setup codex` without `--raw` prints the same JSON with comment-prefixed instructions, so copy only the JSON block when you use that form. JSON does not allow comments.

   **Overwrite warning:** the shell redirect above replaces the entire file. If
   you have custom non-acd hooks in `~/.codex/hooks.json`, back up that file
   first and then merge the acd JSON block in manually rather than using `>`.

4. Restart Codex.
5. **Approve the hooks.** Codex flags every newly-added hook entry as "review required" and refuses to run them until you approve. On first launch you will see `5 hooks need review before they can run. Open /hooks to review them.` Run `/hooks` inside Codex and approve all five (`SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PostToolUse`, `Stop`). Until you approve, the daemon never starts and `acd status` will show no Codex client.

   **Re-approval on update:** Codex re-flags every hook entry as review-required
   whenever `hooks.json` content changes, even if only the ACD block was
   re-written. After any `acd setup codex --raw > ~/.codex/hooks.json` re-run
   (including migrations), open Codex and run `/hooks` again to approve the
   updated entries. Until you do, the daemon will not start for new sessions.

   Run `acd doctor` to check whether your installed snippet is current; it warns
   when active hooks are missing `acd start` or `acd wake` and shows the
   remediation command.

Run `acd doctor` after setup. If it reports ACD hooks in both `hooks.json` and
an inline `config.toml` block, remove the ACD-managed inline block. Codex loads
both sources, so keeping duplicate ACD hooks makes every event run twice.

## Wired events

`hooks.json` registers all five Codex hook events:

- `SessionStart` -> `acd start` (timeout 15s)
- `UserPromptSubmit` -> idempotent `acd start`, then `acd wake` (timeout 15s)
- `PreToolUse` -> idempotent `acd start`, then `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 15s)
- `PostToolUse` -> idempotent `acd start`, then `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 15s)
- `Stop` -> `acd touch` (timeout 5s)

`Stop` calls `acd touch` because Codex fires `Stop` at turn scope rather than
only at true session idle. The refcount sweep on the `watch_pid` still cleans
up once Codex exits.

The active wake hooks call `acd start` first so a later prompt or tool event can
recover if you manually ran `acd stop` while the Codex session stayed open.

Repo autodiscovery is enabled by default. If you disable it with
`repo_lifecycle.autodiscovery` in `~/.config/acd/config.json` or with
`ACD_REPO_AUTODISCOVERY=disabled`, Codex hooks in unregistered repos skip
without creating `.git/acd`. Run `acd repo init` in each repo you want Codex to
manage, or temporarily set `ACD_REPO_AUTODISCOVERY=enabled` before starting the
session.

The repo path is read from the JSON `cwd` field on stdin (consumed in one
pass via `acd hook-stdin-extract session_id cwd?`). When `cwd` is missing,
the hook falls back to the hook process working directory; `CODEX_PROJECT_DIR`
is not used.

## Verify

- Open Codex in any git repo
- From another shell, run `acd status`
- One client with `harness=codex` should appear
- If a hook fails, inspect `~/.local/state/acd/codex-hook.log`

## Uninstall

See [uninstall.md](uninstall.md).
