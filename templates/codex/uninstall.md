# Uninstall acd from Codex

1. Prefer `acd uninstall`, which removes only verified ACD-owned entries from
   `~/.codex/hooks.json`. For manual cleanup, if the file
   contains only the acd block you can delete it outright. If you have merged
   custom (non-acd) hooks, remove only hook objects whose command contains
   `acd internal integration stdin-extract` and calls
   `acd internal session open`, `acd internal hint --kind wake`, or
   `acd internal hint --kind soft_boundary` (including legacy
   heartbeat-only Stop hooks),
   then delete an event key (`SessionStart`, `UserPromptSubmit`, `PreToolUse`,
   `PostToolUse`, `Stop`) only if no hooks remain under it. Older installs may
   also have a top-level `_acd_managed` key; remove it if present.
2. If you still have the legacy TOML install, remove the `# acd-managed: true`
   block from `~/.codex/config.toml` or `~/.config/codex/config.toml`.
3. If you no longer use any Codex hooks and had explicitly enabled them, you can
   also remove `hooks = true` from the `[features]` table in
   `~/.codex/config.toml`. Do not add `hooks = false` unless you want to disable
   all Codex lifecycle hooks.
4. Preview the transactional uninstall, which checkpoints enabled repositories
   before stopping the supervisor:
   ~~~bash
   acd uninstall --dry-run
   ~~~
5. Remove protected data only through `acd uninstall --purge-data`, after
   reviewing the exact plan and supplying its second confirmation.

The hook log lives at `~/.local/state/acd/codex-hook.log`; remove it if you want a fully clean slate.
