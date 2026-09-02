# Claude Code integration

Install or update transactionally:

~~~bash
acd setup --integrations=claude-code
~~~

ACD structurally merges its owned hook entries into
`~/.claude/settings.json` and records their signatures separately. Unrelated
settings and hooks remain untouched. A modified owned entry blocks a later
overwrite or removal until a new plan is explicitly approved.

The integration supplies session start/end, tool activity, and logical-boundary
hints. Filesystem watching and complete polling remain the protection path;
Claude hooks are optional and their absence never leaves edits uncaptured.

Claude can run these hooks in repositories where ACD is not enabled. The hook
checks the nearest Git root first and exits quietly when protection is off. An
active hook sends one event to ACD instead of separate session and wake calls.

Use `acd doctor` to inspect integration drift. Use `acd uninstall --dry-run`
before removing verified owned entries.
