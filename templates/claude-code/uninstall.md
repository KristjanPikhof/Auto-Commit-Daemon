# Uninstall acd from Claude Code

1. Remove the ACD hook objects from `~/.claude/settings.json`: entries whose
   command contains both `acd hook-stdin-extract` and `--harness claude-code`.
   Delete an event key (`SessionStart`, `PreToolUse`, `PostToolUse`, `Stop`,
   or `SessionEnd`) only if no hooks remain under it. Older installs may also
   have a top-level `_acd_managed` key; remove it if present.
2. Stop any running daemons:
   ~~~bash
   acd stop --all
   ~~~
3. (Optional) Remove the acd binary:
   ~~~bash
   rm ~/.local/bin/acd
   # or
   brew uninstall acd
   ~~~
4. (Optional) Remove all acd state:
   ~~~bash
   rm -rf ~/.local/share/acd ~/.local/state/acd ~/.config/acd
   ~~~
