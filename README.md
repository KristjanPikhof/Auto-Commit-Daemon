# ACD

ACD saves your eligible file changes in private Git checkpoints while you work.
When a change is ready, run `acd finish` to turn the saved work into local commits.
ACD never pushes.

Fresh installations use Local mode with imperative messages. Local finish works
offline and creates one complete batch commit. Optional Intent mode uses your
chosen AI provider to plan complete goals and meaningful commit boundaries.
Both modes publish only when you ask.

## Requirements

- macOS or Linux on `arm64` or `amd64`
- Git on `PATH` and a normal Git worktree
- A working systemd user manager on Linux

Finish also needs an attached branch and a Git author name and email. Protection
can continue during unsafe Git states, but publication waits until they are safe.
An attached unborn branch can receive its first commit through finish.

~~~bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
~~~

The installed binary needs no Go, Git remote, GitHub account, AI credentials, or
Full Disk Access on macOS for ordinary Local use.

## Install

With Homebrew:

~~~bash
brew install KristjanPikhof/tap/acd
acd --version
~~~

Or use the release installer:

~~~bash
curl -fsSL \
  https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh |
  bash
~~~

The installer selects the platform binary, verifies its SHA-256 checksum, and
installs it in `$HOME/.local/bin` by default. It needs Bash, `curl`, `tar`,
`install`, and `shasum` or `sha256sum`; `jq` is optional. Add the install directory
to `PATH` if needed:

~~~bash
export PATH="$HOME/.local/bin:$PATH"
~~~

## Start protecting, then finish a change

For coding agents, run `acd setup agents` to choose hooks and get the
copy-ready `AGENTS.md` instructions. The guide previews changes before applying
them. Use `acd setup agents --manual` for commands and instructions only.

Run commands from any directory inside the worktree, or select it with
`--repo /path/to/worktree`. Linked worktrees have separate protected batches.

~~~bash
cd /path/to/repository
acd on
# Edit your files normally.
acd status
acd finish "Add offline checkout"
~~~

First use initializes the missing runtime with offline defaults, enables only
this repository, and confirms a durable checkpoint. `acd on` makes no AI request,
reads no provider credentials, runs no project checks, and creates no branch
commit. Repeating it is safe. Starting a worker can recover a previously
authorized prepared publication, but only from its exact saved proof; it never
starts a new finish. Existing installations that need an upgrade direct you to
`acd setup`.

Check `Last observed work saved` in status. Saved work remains separate from
branch commits until finish succeeds. If protection is incomplete, ACD keeps the
last complete checkpoint, saves readable work in partial snapshots, and reports
the paths that need attention. Partial checkpoints cannot become finish targets.

~~~bash
acd list                  # Protection and finish progress across repositories
acd history               # Saved, committed, and recovered checkpoint outcomes
acd doctor                # Explain a problem and the next safe step
acd off                   # Save a final checkpoint, then stop protection
~~~

`acd off` retains the checkpoints and makes no branch commit. It refuses ordinary
success if the final checkpoint cannot be proved. Start again with `acd on`.
`acd list --all` includes idle repositories; unfinished work stays visible after
an editor or agent exits. A worker heartbeat shows liveness, not publication.

## Finish options

~~~bash
acd finish
acd finish --message "Update setup guidance"
acd finish --dry-run "Add offline checkout"
acd finish --include-staged
acd finish --retry REQUEST_ID
~~~

For Intent, give context about what changed and why so ACD can explain useful
steps within the task. Generated messages include a short reason or concrete
impact. If code diffs are not shared, context or an exact message is required.
`--message` deliberately creates one batch and disables automatic splitting.
See the [Intent flow](docs/intent-commit-flow.md) for recorded versions and previews.

Finish freezes a fresh complete checkpoint, the attached branch and HEAD, and
its options. It builds and checks the whole commit chain privately before one
branch update. Later edits keep entering checkpoints for the next finish. The
live worktree stays intact.

Ordinary finish refuses staged changes. `--include-staged` authorizes the exact
current staged selection, including deletions. It consumes that selection when
published and preserves later staging; it does not grant consent for later edits.

`--dry-run` plans and checks without moving the branch or index. It writes private
request evidence and can run approved project checks or contact the Intent
provider. A later ordinary finish freezes a new target.

A failed request leaves its checkpoint protected. Fix the reported cause and use
`--retry ID` for the same frozen target, or run a new finish for current work.
Retry cannot change the saved purpose, message, or staged consent. Branch or index
movement can require a new request or manual inspection; ACD never guesses.

Closing the terminal detaches from the accepted bounded request. After a worker
restart, interrupted planning requires an explicit retry. A prepared publication
recovers only from its exact saved branch and index proof. Failed requests do not
schedule background AI calls, checks, or publication, and Intent never silently
switches to Local mode.

For the first finish-based release, deprecated `acd commit-all --yes` delegates
to finish and retains explicit staged-selection consent. Use `acd finish` for
new workflows. The alias has no separate drain or publication engine.

## Messages, providers, and checks

Local uses one honest batch summary, or your supplied purpose/message. Intent
plans goals before assigning paths. Implementation, callers, tests, generated
output, and relevant docs stay together when they complete one goal. A split
needs a useful review or revert boundary and a valid intermediate tree.

Intent permits one planning call and at most one correction within its saved
`ai.timeout` deadline. Provider rejection, missing credentials, timeout, or
oversized evidence fails that explicit attempt. Binary content is metadata only;
text evidence and serialized requests have fixed byte limits.

Choose Local or Intent and either message style in the settings editor:

~~~bash
acd config --repo .
~~~

| Style | Example |
|---|---|
| Imperative (default) | `Preserve staged content during finish` |
| Conventional | `fix: preserve staged content` |

Conventional messages use the scope-free `type: subject` form. `--message` supplies
the complete message and must match the selected style. It does not bypass the
provider in Intent mode.

Every finish checks coverage, dependencies, Git structure, and exact target
equality. The default `none` policy and `structural` skip project commands.
Approved `fast` or `full` policies run configured commands against each private
candidate, outside your worktree. Status reports commands that ran, identical
results reused, and checks that were not configured. Structural checks alone do
not mean project tests passed.

Git-ignored and sensitive paths stay outside protection. Excluded tracked entries
and submodule entries remain in the finish base tree. Diff egress is off by
default and needs explicit approval. Local sends nothing to a provider. Status,
list, doctor, and diagnose read state without provider construction or migration.
See [providers and privacy](docs/ai-providers.md).

## Settings and optional integrations

~~~bash
acd config                 # Edit global defaults
acd config --repo .        # Edit this repository
acd config get             # Inspect values and their sources
acd setup --dry-run        # Preview runtime changes and optional integrations
~~~

Review the editor's plan and choose Save changes. Saving and applying settings
make no provider request; a connection test is a separate explicit action. Keys
are masked and stored in the credential store. `acd config set` and `reset` save
a draft for review in the editor. See [settings](docs/settings.md) and the
[generated configuration reference](docs/configuration-reference.md).

Coding-tool integrations only wake protection for already enabled repositories.
They cannot enable a repository, call AI, run checks, or finish work.

## Restore and rewrite history

Restore previews before applying:

~~~bash
acd history
acd restore cp-...
acd restore cp-... --yes
~~~

ACD protects the current state before restoring and returns that checkpoint as
the undo target. Restore preserves HEAD and the index.

Explicit history rewriting retains reviewable saved plans and backups:

~~~bash
acd history rewrite --last 3 --plan-only
acd history rewrite --show-plan PLAN_ID
acd history rewrite --apply PLAN_ID --dry-run
acd history rewrite --apply PLAN_ID --yes
~~~

Local rewriting respects recorded order and authors. Intent can propose complete
goals from exact versions. `--messages-only` preserves one commit per selected
commit; purposeful `--new-branch NAME` reconstruction preserves the source branch.
Saved-plan display and apply do not make a new AI request. See
[history rewriting](docs/rewrite-commits.md) and [workflows](docs/user-workflows.md).

## Upgrade and uninstall

Source changes and a CLI upgrade do not replace the running supervisor or workers.
An older active runtime can still create automatic commits until explicitly
upgraded. Review and apply the runtime plan:

~~~bash
brew upgrade acd          # For a Homebrew installation
acd setup --dry-run
acd setup
~~~

Upgrade preserves explicit Intent/style/privacy/verification choices. Legacy Event
settings become Local batches. Old captured versions and prepared publication
proofs remain protected; ambiguous evidence requires attention rather than an
invented migration result. See [the finish contract](docs/finish-contract.md).

Uninstall keeps protected repository data by default:

~~~bash
acd uninstall --dry-run
acd uninstall
~~~

For a clean ACD start across all registered worktrees:

~~~bash
acd uninstall --dry-run --purge-data
acd uninstall --purge-data --confirm-purge-data
~~~

Purge removes the installed runtime, owned integrations, registrations,
repository ACD directories and databases, and private `refs/acd/` archives.
Project branches, tags, commits, staging, and working files stay unchanged.

## Documentation and development

- [Commands](docs/commands.md)
- [Architecture](docs/overview.md)
- [Protection](docs/capture-replay.md)
- [Intent planning](docs/intent-commit-flow.md)
- [Integrations](docs/multi-tool.md)
- [Changelog](CHANGELOG.md)

Building from source requires Go 1.26.9 or later:

~~~bash
make build
make lint
make test
~~~

The local gate uses three core shards and at most two parallel tests per package.
Hosted CI has four core and four production integration shards on Ubuntu and
macOS, a support lane, six Ubuntu daemon stress runners, and two Git/state stress
runners. Each leaf has a five-minute limit. The complete workflow and local gate
have a five-minute target; keep checks sharded rather than dropping scenarios.

Set `ACD_TEST_RESULTS_DIR` outside the checkout to retain JSON timings and shard
manifests. Contribution rules are in [CLAUDE.md](CLAUDE.md). ACD is MIT licensed.
