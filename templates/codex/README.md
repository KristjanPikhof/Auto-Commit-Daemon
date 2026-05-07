# acd adapter: codex

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup codex`
3. Save the printed JSON to `~/.codex/hooks.json`. Codex now reads `hooks.json` before `~/.codex/config.toml`.
4. Restart Codex.

If you previously installed the legacy TOML snippet, delete the
`# acd-managed: true` block from `~/.codex/config.toml` so it does not shadow
`hooks.json`. `acd doctor` warns when both files carry an acd marker.

## Wired events

`hooks.json` registers all five Codex hook events:

- `SessionStart` -> `acd start` (timeout 15s)
- `UserPromptSubmit` -> `acd wake` (timeout 5s)
- `PreToolUse` -> `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 5s)
- `PostToolUse` -> `acd wake` (matcher `apply_patch|Edit|Write|Bash`, timeout 5s)
- `Stop` -> `acd touch` (timeout 5s)

`Stop` calls `acd touch` (mirrors the claude-code adapter) so the daemon is
not killed mid-replay drain. The refcount sweep on the `watch_pid` still
cleans up once Codex exits.

The repo path is read from the JSON `cwd` field on stdin (consumed in one
pass via `acd hook-stdin-extract session_id cwd`). When `cwd` is missing,
the hook falls back to the hook process working directory; `CODEX_PROJECT_DIR`
is no longer required.

## Verify

- Open Codex in any git repo
- From another shell, run `acd status`
- One client with `harness=codex` should appear
- If a hook fails, inspect `~/.local/state/acd/codex-hook.log`

## Uninstall

See [uninstall.md](uninstall.md).
