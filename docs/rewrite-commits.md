# Rewrite commit history

Use the explicit history command to group related commits and improve their
messages on a safe linear range:

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

New plans group adjacent commits by intent. A group becomes one commit, so the
result may contain fewer commits than the selected range. Use message-only mode
when you want to preserve every existing boundary:

~~~bash
acd history rewrite --last 5 --messages-only --plan-only
~~~

Grouping keeps commit order and never mixes primary authors. It also refuses a
group whose final tree matches the tree before the group, because combining a
change with its revert would produce an empty commit. Merge commits remain
unsupported.

Creating a new plan requires Intent mode and either an OpenAI-compatible or
local subprocess provider that can produce history rewrite plans. Local rules
cannot generate a plan. A subprocess provider that only supports the older
message proposal request can still be used with `--messages-only`. Showing,
editing, or applying an existing saved plan makes no new AI call.

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

The command never runs automatically and is isolated from normal
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

The saved plan shows the selected and resulting commit counts. Each group lists
its member commits, proposed message, and grouping reason. The text and JSON
editors let you move boundaries between adjacent groups. They reject missing,
duplicate, or reordered commits, author-boundary violations, and empty net
changes.

Progress defaults to `--progress auto`, which writes readable progress when
stderr is a terminal and stays silent otherwise. Use `--progress plain`,
`--progress json`, or `--progress off` to override it. Message-only progress
includes the current and total commit counts:

~~~text
History rewrite: Commit messages [42/169]: message ready
History rewrite: Applying groups [42/169]: grouped 1 selected commit
~~~

JSONL progress is written to stderr so stdout remains the single command
result. Grouped progress looks like this:

~~~text
History rewrite: Grouping commits [29/29]: grouping 29 selected commits
History rewrite: Grouping commits [7/7]: 7 groups ready
History rewrite: Applying groups [3/7]: grouped 4 selected commits
~~~
