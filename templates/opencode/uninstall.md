# Uninstall acd from OpenCode

1. Prefer `acd uninstall`, which removes the verified ACD-owned hook block
   from `~/.config/opencode/hook/hooks.yaml`. For manual cleanup, remove only
   the `# acd-managed: true` block and its five `acd-*` hooks.
2. Preview the transactional uninstall, which checkpoints enabled repositories
   before stopping the supervisor:
   ~~~bash
   acd uninstall --dry-run
   ~~~
3. Remove protected data only through `acd uninstall --purge-data`, after
   reviewing the exact plan and supplying its second confirmation.

Purge removes ACD state and private refs across registered worktrees. Project
Git branches, commits, staging, and working files stay unchanged.

Uninstall takes a final checkpoint before stopping protection; it never calls
`acd finish` or publishes pending work. Saved checkpoints and history remain
unless you explicitly approve data removal.
