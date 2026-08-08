# Uninstall acd from Claude Code

1. Prefer `acd uninstall`, which removes only verified ACD-owned hook entries.
   For manual cleanup, remove entries whose command contains
   `acd internal integration stdin-extract` and calls `acd internal session open`,
   `acd internal hint --kind wake`, `acd internal hint --kind logical_boundary`,
   or `acd internal session close`. Delete an event key (`SessionStart`,
   `PreToolUse`, `PostToolUse`, `Stop`, or `SessionEnd`) only if no hooks
   remain under it. Older installs may also have a top-level `_acd_managed`
   key; remove it if present.
2. Stop the user supervisor with `acd uninstall`; do not kill workers or delete
   ownership locks manually.
3. For a preview that preserves all checkpoint data, run:
   ~~~bash
   acd uninstall --dry-run
   ~~~
4. Remove protected data only through `acd uninstall --purge-data`, after
   reviewing the exact plan and supplying its second confirmation.
