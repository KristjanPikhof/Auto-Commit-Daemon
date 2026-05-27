# Uninstall acd from Cursor

1. Remove the acd-managed entries from `~/.cursor/hooks.json`. The shipped
   template (`templates/cursor/hooks.json`) wires exactly five events:
   `sessionStart`, `postToolUse`, `afterFileEdit`, `stop`, and `sessionEnd`,
   plus top-level `"version": 1` and `"_acd_managed": true`. If the file
   contains only the acd block you can delete it outright. If you merged custom
   (non-acd) hooks, remove those five event entries and `_acd_managed` (and
   `version` if nothing else needs it) instead of deleting the file, so your
   other hooks are preserved.
2. Stop any running daemons (Cursor does not use `watch-pid` refcount; sessions
   may linger until explicitly stopped):
   ~~~bash
   acd stop --all
   ~~~
3. (Optional) Remove the acd binary:
   ~~~bash
   rm ~/.local/bin/acd
   # or
   brew uninstall acd
   ~~~
4. (Optional) Remove all acd state and logs:
   ~~~bash
   rm -rf ~/.local/share/acd ~/.local/state/acd ~/.config/acd
   ~~~

The hook log lives at `~/.local/state/acd/cursor-hook.log`; remove it if you
want a fully clean slate.

Do not confuse this with repo-local `.cursor/hooks.json`; acd never installs
there. If you added acd entries under a project `.cursor/` tree manually, remove
them separately.
