# Uninstall acd from Cursor

1. Remove the acd-managed entries from `~/.cursor/hooks.json`. If the file
   contains only the acd block you can delete it outright. If you have merged
   custom (non-acd) hooks, remove the five acd event entries
   (`sessionStart`, `postToolUse`, `afterFileEdit`, `stop`, `sessionEnd`) and
   the top-level `_acd_managed` key instead of deleting the file, so your other
   hooks are preserved.
2. Remove the helper script:
   ~~~bash
   rm -f ~/.cursor/hooks/acd-lifecycle.sh
   ~~~
   Remove `~/.cursor/hooks/` only if no other tools use scripts in that
   directory.
3. Stop any running daemons (Cursor does not use `watch-pid` refcount; sessions
   may linger until explicitly stopped):
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

The hook log lives at `~/.local/state/acd/cursor-hook.log`; remove it if you
want a fully clean slate.

Do not confuse this with repo-local `.cursor/hooks.json`; acd never installs
there. If you added acd entries under a project `.cursor/` tree manually, remove
them separately.
