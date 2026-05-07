# Uninstall acd from Codex

1. Remove the `# acd-managed: true` block from your Codex config (`~/.codex/config.toml` or the path your Codex install uses).
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
