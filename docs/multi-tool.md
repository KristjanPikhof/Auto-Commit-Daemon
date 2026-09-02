# Multiple tools in one repository

ACD protection is tool-independent. Editors, terminals, agent harnesses, and
other local processes can modify the same worktree; watching plus complete
polling observes the resulting filesystem state.

Optional integrations add semantic or boundary hints. Duplicate hints are
idempotent observations and do not create a second protection engine.

Some tools install hooks for every workspace. Each ACD hook checks the nearest
Git root against ACD's registry before it contacts the supervisor. Repositories
where ACD is off, disabled, or not registered are ignored without creating
state or showing a hook error.

One worker owns each Git common directory, so linked worktrees share ownership
while retaining distinct worktree identities and settings. Publication uses
the current exact branch token and suspends during transitions or unsafe Git
state.

When another tool also creates commits, ACD reconciles the external branch
movement before publishing. If exact ancestry/tree proof is impossible, it
reports `needs_action` rather than discarding or guessing.

~~~bash
acd status
acd history --activity
acd doctor
~~~

Avoid running two unrelated automatic commit publishers against the same
branch. Checkpoint protection remains safe, but publication may wait repeatedly
on external movement.
