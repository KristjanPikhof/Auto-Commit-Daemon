# Uninstall acd from Pi

1. Prefer `acd uninstall`, which removes the verified ACD-owned hook block
   from `~/.pi/agent/hook/hooks.yaml`. For manual cleanup, remove only the
   `# acd-managed: true` block and its five `acd-*` hooks.
2. Preview the transactional uninstall, which checkpoints enabled repositories
   before stopping the supervisor:
   ~~~bash
   acd uninstall --dry-run
   ~~~
3. Remove protected data only through `acd uninstall --purge-data`, after
   reviewing the exact plan and supplying its second confirmation.
