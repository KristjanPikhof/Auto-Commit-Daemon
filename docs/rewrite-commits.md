# Rewrite commit messages

Use the explicit advanced command to improve messages on a provable
ACD-authored suffix:

~~~bash
acd history rewrite
~~~

The command is never automatic and is isolated from normal checkpoint
protection and publication. It previews the exact commit set, preserves a
private backup ref, verifies ownership and a stable branch, refuses staged
overlap or unsafe Git operations, and requires explicit apply confirmation.

Effective repository configuration controls provider and message format:

~~~bash
acd config get
acd config edit
~~~

Imperative subjects are at most 50 characters with no period, followed by a
blank line and `- ` context bullets wrapped at 72. Conventional mode accepts
scope-less supported types and the same body contract.

Progress defaults to `--progress auto`, which writes human-readable progress
when stderr is a terminal and stays silent otherwise. Use `--progress plain`,
`--progress json`, or `--progress off` to override it. Human-readable proposal
and recreation events include the current and total commit counts:

~~~text
rewrite-commits: proposal [42/169]: proposal accepted
rewrite-commits: apply_recreate_selected [42/169]: recreated selected commit
~~~

JSONL progress is written to stderr so stdout remains the single command
result.
