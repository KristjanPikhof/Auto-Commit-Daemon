# Rewrite commit messages

Use the explicit history command to improve messages on a safe linear range:

~~~bash
acd history rewrite
~~~

Start with a saved plan:

~~~bash
acd history rewrite --last 5 --plan-only
acd history rewrite --show-plan <plan-id-or-file>
acd history rewrite --apply <plan-id-or-file> --dry-run
acd history rewrite --apply <plan-id-or-file> --yes
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

Progress defaults to `--progress auto`, which writes readable progress when
stderr is a terminal and stays silent otherwise. Use `--progress plain`,
`--progress json`, or `--progress off` to override it. Progress includes the
current and total commit counts:

~~~text
History rewrite: Commit messages [42/169]: message ready
History rewrite: Applying messages [42/169]: applied the new message
~~~

JSONL progress is written to stderr so stdout remains the single command
result.
