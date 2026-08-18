# Settings

## Scope and precedence

Inside a Git worktree, configuration defaults to repository scope. Outside a
worktree it defaults to global scope. Use `--scope repo|profile|global` to be
explicit.

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
acd config edit
acd config reset
acd config credentials
~~~

Interactive editing rejects `--json`. Noninteractive reads and writes use the
stable product envelope. Global operations reject `--repo` rather than
silently ignoring it.

## Defaults

Fresh setup persists:

~~~text
ai.provider = deterministic
commit.strategy = intent
commit.preset = balanced
commit.format = imperative
intent.verification = structural
intent.repair.enabled = true
ai.diff_egress = false
~~~

These are global user defaults. The current repository and future repositories
inherit them without repository-specific overrides. Repair is limited to
recent, private, ACD-owned commits. It never rewrites pushed or user-owned
history. The deterministic path needs no API key. Migration preserves every existing
repository's effective provider, strategy, preset, verification, and repair
values, including Event strategy inherited from old defaults.

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

Hot fields apply between safe worker passes. Restart-required fields apply
when the supervisor next starts that repository worker. Changing a global
value does not start stopped repositories or fan out an implicit restart.

See the generated [configuration reference](configuration-reference.md) for
every supported setting, environment variable, default, apply boundary,
persistence rule, and sensitivity classification.
