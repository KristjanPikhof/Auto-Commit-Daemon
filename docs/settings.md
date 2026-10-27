# Settings

## Edit settings

~~~bash
acd config
acd config --repo .
acd config --scope global
acd config --accessible
~~~

The editor starts at Global defaults unless you choose repository scope. The
selected scope stays visible. Change a field, then choose Save changes to
review affected repositories and required permissions. Review and apply are
local: they do not contact a provider or run a project command.

Save persists reviewed values and applies supported hot settings synchronously
to approved revisions. It does not start stopped workers, wake publication, or
create a finish. Restart-required fields take effect on the next worker start.
If another terminal changes settings during review, reopen the editor.

Choose Use inherited value to remove one repository override. To review removal
of all repository overrides, save a reset draft and review it:

~~~bash
acd config reset --repo .
acd config --repo .
~~~

## Scope and precedence

Noninteractive `get`, `set`, and `reset` default to repository scope inside a
Git worktree and global scope outside one. Choose `--scope repo|profile|global`
explicitly when needed. Global operations reject `--repo`.

For authored settings, resolution is:

~~~text
editor draft > repository > profile > global > environment
             > translated legacy defaults > built-in default
~~~

An explicit saved value takes precedence over an ordinary environment override.
`acd config get` reports the source and any shadowed environment value. API-key
resolution is separate: the environment credential takes precedence over the
protected store.

A finish uses the approved applied revision, or its last-known-good revision.
Unapproved authoring edits do not replace that snapshot. An accepted finish
keeps its frozen configuration even if settings change later. Linked worktrees
retain separate worktree identities and scoped choices.

If an approval was interrupted, reopen the editor, review the current settings,
and save to replace that pending request. A concurrent change requires a fresh
review. Pending settings remain unavailable to finish until explicitly applied.

~~~bash
acd config get
acd config get commit.strategy
acd config set --scope global commit.strategy local
acd config set --scope global commit.format conventional
acd config reset
~~~

`set` and `reset` save authoring changes. Follow their printed editor command to
review and apply them. Interactive editing rejects `--json`; supported
noninteractive commands use the product JSON envelope.

## Defaults and modes

| Setting | Fresh default |
|---|---|
| `commit.strategy` | `local` |
| `commit.format` | `imperative` |
| `ai.provider` | `deterministic` |
| `ai.diff_egress` | `false`; Intent setup proposes `true` for approval |
| `intent.verification` | `none` |
| `ai.timeout` | `5m` |

Local creates one complete batch commit when you run `acd finish`. It needs no
credentials, network, or provider process. Supplied purpose explains the work;
without context, a deterministic message describes observed changes.

Intent uses an approved `openai-compat` or `subprocess:<name>` provider to plan
completed goals. Deterministic is not an Intent provider. A failed Intent
attempt stays failed until explicit retry; it never changes mode itself.

Both modes support imperative and conventional messages. Subjects are at most
50 characters without a trailing period; conventional subjects use a supported
scope-free `type: subject`. Context bullets use `- ` and wrap at 72 characters.
A valid `--message` is preserved exactly. Under Intent, it still requires AI
planning.

Migration preserves explicit supported choices and their provenance. Legacy
Event strategy translates to Local batch semantics. Presets, planner windows,
experiments, and automatic repair controls are retired; they do not authorize
publication. Archived selectors can supply narrow verification/privacy defaults
below authored values until reviewed settings materialize those choices.

## Project checks

Structural safety checks always run: recorded versions, safe membership and
ordering, complete trees, and exact final target equality.

| `intent.verification` | Project command |
|---|---|
| `none` | None |
| `structural` | None |
| `fast` | Approved `verification.fast.command` |
| `full` | Approved `verification.full.command` |

The editor reviews command execution permission separately from provider and
diff permissions. Applying that approval does not execute the command. Finish
runs it in isolated candidate trees using the configured timeout. A missing
command is reported as missing; a structural pass is not a test-suite pass.
Reused evidence is labelled reused.

## Credentials and privacy

~~~bash
acd config credentials status
acd config credentials set
acd config credentials remove
~~~

Keys live in the protected credential store, not repository configuration.
Intent with an OpenAI-compatible provider requires a key from that provider.
Fresh setup asks for a missing key; the settings editor also asks before saving
Intent settings. The key stays masked and is stored only after approval. An
existing environment or stored key is reused. Local mode and local subprocess
providers do not require this API key.

Credential save and settings Apply do not test the connection. Optional
explicit connection tests use fixed synthetic input, not repository content.
Local mode does not inspect the credential store.

Intent requires redacted code diffs. Setup and `acd config` propose missing
sharing for approval, including when saving existing Intent settings. Sharing
changes only after approval; upgrade preserves existing consent. Local remains
offline with sharing disabled. Binaries remain metadata only. Custom
endpoint credential disclosure, insecure HTTP, provider execution, and project
command execution have separate permissions. See [AI providers](ai-providers.md).

## Repository consent

`acd on` registers and enables the current repository. Hooks cannot register
unknown repositories or re-enable disabled ones. Authored settings, provider
approval, and repository registration are separate decisions.

Run `acd off` to take a final checkpoint and stop protection for that repository.
It does not commit the work. See [user workflows](user-workflows.md).

## Large files and provider waits

`capture.max_file_bytes` (`ACD_MAX_FILE_BYTES`) defaults to 5 MiB and controls
buffering. Larger regular files stream into Git objects; the setting does not
silently exclude them. Git-ignored and sensitive paths remain excluded.

`ai.max_input_bytes` (`ACD_AI_MAX_INPUT_BYTES`) defaults to 8 MiB. It bounds
aggregate complete textual diff evidence. Each review, global plan, and
correction exchange also stays within 512 KiB, or the smaller configured budget.
Large batches review every diff in fragments before one global plan. There is
no separate per-file rejection cap. Supported
values range from 131072 (128 KiB) to 67108864 (64 MiB). For example, to allow
up to 16 MiB of aggregate evidence in the current repository:

~~~bash
acd config set ai.max_input_bytes 16777216
~~~

Choose a budget your provider can accept. Bytes do not directly measure model
tokens, so a provider may reject a request that fits this budget. The setting
applies to new requests; retry preserves the original settings. Oversized input
fails without clipping or omitting changes, and the checkpoint stays protected.

`ai.timeout` (`ACD_AI_TIMEOUT`) defaults to five minutes. An Intent attempt has
one persisted deadline covering evidence preparation, reviews, planning, and
correction. Use `acd finish --timeout 15m` for a larger batch, or
`acd finish --retry REQUEST_ID --timeout 15m` for the same saved target. This
per-attempt override accepts 1 second through 1 hour and leaves provider settings
unchanged. Reconnecting and restarting never extend an active deadline.
Validated reviews survive restart and can be reused by an explicit retry when
their exact evidence, provider configuration, and instructions still match.

The generated [configuration reference](configuration-reference.md) lists
supported fields, environment names, defaults, and apply boundaries.
