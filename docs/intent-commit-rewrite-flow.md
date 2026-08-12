# Explicit history rewrite flow

History rewrite is an advanced, separately invoked operation:

~~~bash
acd history rewrite
~~~

It is never part of normal protection or publication. Normal ACD behavior does
not rewrite history.

Rewrite planning and apply retain the existing exact-suffix, ownership,
staging, Git-state, verification, backup-ref, preview, and confirmation gates.
The repository must have no active publication and the selected commits must be
a provable private ACD-owned first-parent suffix at exact `HEAD`.

The operation supports saved-plan show, edit, dry-run, and apply. Progress is
human text or JSONL on stderr. Checkpoints remain unchanged, so a rewrite does
not remove restoration history.
