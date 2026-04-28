# Uninstall acd from Claude Code

1. Remove the `_acd_managed` block (and the matching `SessionStart` / `PreToolUse` / `PostToolUse` / `Stop` / `SessionEnd` entries it added) from `~/.claude/settings.json`.
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
