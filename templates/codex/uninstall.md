# Uninstall acd from Codex

1. Remove the acd-managed entries from `~/.codex/hooks.json`. If the file
   contains only the acd block you can delete it outright. If you have merged
   custom (non-acd) hooks, remove only hook objects whose command contains
   `acd hook-stdin-extract` and calls `acd start`, `acd wake`, or
   `acd touch` (including current `acd touch --soft-boundary` and legacy
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
4. The daemon shuts down on its own once Codex exits and the refcount sweep clears the `watch_pid` row. Force-stop any survivors with:
   ~~~bash
   acd stop --all
   ~~~
5. (Optional) Remove the acd binary:
   ~~~bash
   rm ~/.local/bin/acd
   # or
   brew uninstall acd
   ~~~
6. (Optional) Remove all acd state and logs:
   ~~~bash
   rm -rf ~/.local/share/acd ~/.local/state/acd ~/.config/acd
   ~~~

The hook log lives at `~/.local/state/acd/codex-hook.log`; remove it if you want a fully clean slate.
