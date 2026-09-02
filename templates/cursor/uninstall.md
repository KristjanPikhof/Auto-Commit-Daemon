# Uninstall acd from Cursor

1. Prefer `acd uninstall`, which removes only verified ACD-owned entries from
   `~/.cursor/hooks.json`. The shipped
   template (`templates/cursor/hooks.json`) wires exactly five events:
   `sessionStart`, `postToolUse`, `afterFileEdit`, `stop`, and `sessionEnd`,
   plus top-level `"version": 1`. If the file contains only the acd block you
   can delete it outright. If you merged custom (non-acd) hooks, remove only
   hook objects whose command contains
   `acd internal integration event --harness cursor`. Older entries can contain
   `acd internal integration cursor-extract`, `acd internal session open`,
   `acd internal hint`, or `acd internal session close`; remove those ACD-owned
   entries too. Then delete
   an event key only if no hooks remain under it. Remove `version` only if
   nothing else needs it. Older installs may also have a top-level
   `_acd_managed` key; remove it if present.
2. Preview the transactional uninstall, which checkpoints enabled repositories
   before stopping the supervisor:
   ~~~bash
   acd uninstall --dry-run
   ~~~
3. Remove protected data only through `acd uninstall --purge-data`, after
   reviewing the exact plan and supplying its second confirmation.

The hook log lives at `~/.local/state/acd/cursor-hook.log`; remove it if you
want a fully clean slate.

Do not confuse this with repo-local `.cursor/hooks.json`; acd never installs
there. If you added acd entries under a project `.cursor/` tree manually, remove
them separately.
