# Pi integration

Install or update transactionally:

~~~bash
acd setup --integrations=pi
~~~

ACD merges one bounded, versioned owned block into
`~/.pi/agent/hook/hooks.yaml`. Unrelated YAML remains unchanged. Session IDs
use a stable per-process fallback when Pi does not supply one.

Session, tool, idle, and deletion events provide semantic and publication
boundary hints. Filesystem watching and complete polling remain authoritative
for protection.

Each hook sends one normalized event. ACD first checks the nearest Git root and
quietly ignores repositories where protection is not active.

Use `acd doctor` for drift and `acd uninstall --dry-run` for safe removal.
