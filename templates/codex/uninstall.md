# Uninstall acd from Codex

1. Remove the acd-managed entries from `~/.codex/hooks.json`. If the file
   contains only the acd block you can delete it outright. If you have merged
   custom (non-acd) hooks, remove the five acd event entries
   (`SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PostToolUse`, `Stop`)
   and the top-level `_acd_managed` key instead of deleting the file, so your
   other hooks are preserved.
2. If you still have the legacy TOML install, remove the `# acd-managed: true` block from `~/.codex/config.toml`.
3. The daemon shuts down on its own once Codex exits and the refcount sweep clears the `watch_pid` row. Force-stop any survivors with:
   ~~~bash
   acd stop --all
   ~~~
4. (Optional) Remove the acd binary:
   ~~~bash
   rm ~/.local/bin/acd
   # or
   brew uninstall acd
   ~~~
5. (Optional) Remove all acd state and logs:
   ~~~bash
   rm -rf ~/.local/share/acd ~/.local/state/acd ~/.config/acd
   ~~~

The hook log lives at `~/.local/state/acd/codex-hook.log`; remove it if you want a fully clean slate.
