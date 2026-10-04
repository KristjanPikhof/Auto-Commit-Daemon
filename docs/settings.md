# Settings

## Edit settings

Run `acd config` to open the settings editor. It starts at **Global defaults**.
Inside a Git worktree, choose **Editing** to switch to **This repository**.
The selected scope stays visible while you edit.

~~~bash
acd config
acd config --scope global
acd config --repo .
acd config edit
acd config --accessible
~~~

Select a field to change it. Model and endpoint remain editable after setup.
The API key field accepts masked input and keeps the key in memory until Save
passes its connection test. Keys use the protected store shared by repositories.
If `ACD_AI_API_KEY` supplies the key, unset that variable before replacing the
stored key through the editor.

**Save changes** shows the changed values, affected repositories, and required
permissions. After approval, ACD tests the connection, saves only edited fields,
and queues runtime changes for the next safe boundary. A failed test keeps the
editor open with your edits and leaves saved settings and credentials unchanged.
If settings change in another terminal during review, reopen the editor to
review those values before saving.

Repository fields show where their value comes from. Select **Use inherited
value** to remove one override. Other repository settings stay as they were.
Advanced settings include verification commands, capture limits, and retention.
Changing the commit mode preserves explicit advanced customizations.

Global saves update enabled repositories that inherit the changed values.
The review lists repositories that keep their own overrides. Stopped workers
stay stopped. Completion distinguishes queued activation, restart-required
fields, and settings that were saved but could not yet be applied. A repository
with another activation pending must finish it before a new save.

## Scope and precedence

For `config get`, `set`, and `reset`, the default is repository scope inside a
Git worktree and global scope outside one. Use `--scope repo|profile|global` to
choose explicitly. The interactive editor always starts at global scope unless
`--repo` or `--scope repo` is supplied.

Resolution order is:

~~~text
invocation override
internal experiment
repository
profile
global
environment
preset
default
~~~

Repository identity uses the v20 worktree ID, so linked worktrees can retain
distinct capture settings while sharing one common-directory worker.

## Commands

~~~bash
acd config get
acd config get commit.preset
acd config set commit.preset fast
acd config set --scope global ai.provider deterministic
acd config reset
acd config credentials
~~~

Interactive editing rejects `--json`. Noninteractive reads and writes use the
stable product envelope. Global operations reject `--repo` rather than
silently ignoring it.

## Defaults

Fresh setup recommends these settings after provider and privacy approval:

~~~text
ai.provider = openai-compat
commit.strategy = intent
commit.preset = balanced
commit.format = imperative
intent.verification = structural
intent.repair.enabled = true
ai.diff_egress = true
~~~

Choosing Local automatic commits instead saves `ai.provider = deterministic`
and `ai.diff_egress = false`, without credentials or network access. A fresh
noninteractive setup must specify the provider even in its dry-run preview.

These are global user defaults. The current repository and future repositories
inherit them without repository-specific overrides. Repair is limited to
recent, private, ACD-owned commits. It never rewrites pushed or user-owned
history. The deterministic path needs no API key. Migration preserves every existing
repository's effective provider, strategy, preset, verification, and repair
values, including Event strategy inherited from old defaults.

A repository override takes precedence over later changes to global defaults.
Use the reviewed inheritance flow when a repository should follow every global
setting again:

~~~bash
acd config edit --repo . --inherit
~~~

`acd config get` shows saved values and their sources. `config set` and
`config reset` save authoring changes; their next-step message names the editor
and matching scope for review and activation. `acd status --verbose` shows the
provider and model the worker is currently using.

An explicit change from AI to local mode may preserve the active unpublished
target in recovery and recapture it under the new verified configuration. The
preview explains this effect; an ambiguous target remains protected and needs
attention. ACD returns to AI only when you select it again.

The preview states whether it will save a repository override, update global
defaults, or remove an override. Local rules need no AI or network access, but
they cannot generate a history rewrite plan. OpenAI-compatible providers can
group selected history by default. Local subprocess providers must support the
grouped history request, or you can use `acd history rewrite --messages-only`.

## Credentials and privacy

Credentials remain in
`${XDG_CONFIG_HOME:-$HOME/.config}/acd/credentials.json`, schema v1, under an
owner-only directory and regular `0600` file. A credential is never written to
repository state, logs, traces, status, diagnostics, errors, or support output.

Environment credentials override the store. Network diff egress requires both
a provider that declares it needs diffs and explicit approval. Setup and its
self-test send no repository source. A selected network provider is tested
with fixed synthetic text before setup writes anything.

Credential replacement is crash recoverable. The prior file stays only inside
the protected credential directory until setup commits. Setup backups, plans,
digests, JSON output, logs, errors, journals, state databases, and configuration
files never contain the token.

## Runtime application

Hot fields apply between safe worker passes after the editor queues them.
Restart-required fields apply when the supervisor next starts that worker.
Saving global settings does not start stopped repositories or restart workers.
For an activation failure after saving, reopen `acd config --repo PATH` and save
the reviewed settings again. Existing active settings remain in use until the
new revision is applied.

## Repository consent

Run `acd on` once in each repository that ACD should protect. Integration hooks
do not register unknown repositories or re-enable disabled ones.

`repo_lifecycle.autodiscovery` and `ACD_REPO_AUTODISCOVERY` are deprecated.
They remain parseable so existing configuration files keep their unknown
fields, but they no longer grant repository consent or override `acd on`.

See the generated [configuration reference](configuration-reference.md) for
every supported setting, environment variable, default, apply boundary,
persistence rule, and sensitivity classification.

## Large files and provider waits

`capture.max_file_bytes` (`ACD_MAX_FILE_BYTES`) defaults to 5 MiB. It is a
buffering threshold: larger regular files stream into durable Git objects.
ACD caps buffering at 32 MiB even if this setting is higher. It does not silently
exclude large or binary files. Git-ignored and sensitive paths remain excluded.

`ai.timeout` (`ACD_AI_TIMEOUT`) defaults to five minutes. Intent uses one
persisted deadline for planning and corrections of unchanged evidence. Restart
cannot renew that deadline. When AI cannot finish, safe local grouping and
messages let publication continue; verification and dependency checks still
apply. Invalid provider credentials remain a configuration error.
