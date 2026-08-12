# OpenCode integration

Install or update transactionally:

~~~bash
acd setup --integrations=opencode
~~~

ACD merges one bounded, versioned owned block into
`~/.config/opencode/hook/hooks.yaml`. Unrelated YAML remains unchanged, and a
modified owned block requires a newly approved plan before replacement or
removal.

Session, tool, idle, and deletion events provide semantic and publication
boundary hints. Filesystem watching and complete polling remain authoritative
for protection.

Use `acd doctor` for drift and `acd uninstall --dry-run` for safe removal.
