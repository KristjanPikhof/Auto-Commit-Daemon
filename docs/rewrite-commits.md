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

Creating a new plan requires Intent mode and either an OpenAI-compatible or
local subprocess provider that can produce Intent plans. Local rules cannot
generate a rewrite plan. Showing, editing, or applying an existing saved plan
makes no new AI call.

ACD checks these requirements before it prints the selected commits. When a
repository override masks a usable global provider, the error shows both
settings and offers two choices:

~~~bash
# Use every global setting for this repository.
acd config edit --repo . --inherit

# Keep a repository override and configure it directly.
acd config edit --repo .
~~~

If planning cannot start, no plan is generated and Git history stays
unchanged.

The command is never invoked automatically and is isolated from normal
checkpoint protection and publication. It previews the exact commit set,
preserves a private backup ref, verifies ownership and a stable branch, refuses
staged overlap or unsafe Git operations, and requires explicit apply
confirmation. Separately, enabled Intent repair may rewrite only a proved
private, unshared, ACD-owned suffix. It never rewrites pushed or user-owned
history.

Effective repository configuration controls the provider and message format:

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
