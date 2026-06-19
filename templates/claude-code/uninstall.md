# Uninstall acd from Claude Code

1. Remove the ACD hook entries from `~/.claude/settings.json`: `SessionStart`, `PreToolUse`, `PostToolUse`, `Stop`, and `SessionEnd`. Older installs may also have a top-level `_acd_managed` key; remove it if present.
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
