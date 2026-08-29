# Explicit history rewrite flow

History rewrite is an advanced, separately invoked operation:

~~~bash
acd history rewrite
~~~

It is never part of normal protection or publication. Normal ACD behavior does
not invoke this explicit rewrite command automatically. Separately, enabled
Intent repair may rewrite only a proved private, unshared, ACD-owned suffix. It
never rewrites pushed or user-owned history.

Creating a rewrite plan requires Intent mode and an explicitly configured
non-deterministic AI provider that can produce Intent plans. Showing, editing,
or applying an existing saved plan does not call the provider again.

Rewrite planning and apply retain the existing exact-suffix, ownership,
staging, Git-state, verification, backup-ref, preview, and confirmation gates.
The repository must have no active publication and the selected commits must be
a provable private ACD-owned first-parent suffix at exact `HEAD`.

The operation supports saved-plan show, edit, dry-run, and apply. Progress is
human text or JSONL on stderr. Checkpoints remain unchanged, so a rewrite does
not remove restoration history.
