# Uninstall acd from OpenCode

1. Remove the `# acd-managed: true` block and its five `acd-*` hooks from
   `~/.config/opencode/hook/hooks.yaml`.
2. Stop any running daemons:
   ~~~bash
   acd stop --all
   ~~~
3. To delete captured state for a repo, do this before removing the binary:
   ~~~bash
   acd repo remove --repo /path/to/repo --yes --purge-state
   ~~~
4. (Optional) Remove the binary and global ACD data:
   ~~~bash
   rm ~/.local/bin/acd
   rm -rf ~/.local/share/acd ~/.local/state/acd ~/.config/acd
   # or remove the Homebrew binary with: brew uninstall acd
   ~~~

Per-repo `.git/acd` directories remain unless you remove them explicitly.
