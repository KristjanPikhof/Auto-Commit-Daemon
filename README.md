# ACD

ACD protects meaningful working-tree changes as durable local checkpoints,
then groups and publishes them as ordinary local Git commits.

> Capture every meaningful change durably first. Group and publish commits
> second. Make protection status and restoration understandable without Git
> expertise.

ACD is a static Go binary for macOS and Linux on `amd64` and `arm64`. It never
pushes, never rewrites normal history, and needs no API key for its default
path.

## v2026-08-25

This release separates machine-wide setup from repository enablement and fixes
Intent publication stalls caused by stale dependency edges or checkpoint
proof. It also improves planner recovery and diagnostics, and isolates large
race-test packages so CI runs them without competing workloads.

The terminal and storage dependencies now include Bubbles 2.2.0, Bubble Tea
2.0.9, and SQLite 1.57.0. See [the changelog](CHANGELOG.md#v2026-08-25) for the
full list.

## Get protected

Install ACD once, then enable each repository you want to protect:

~~~bash
acd setup
cd /path/to/repository
acd on
~~~

First setup asks four short questions about grouping, commit messages, and the
provider. The recommended local provider works offline. A custom
OpenAI-compatible provider asks for its endpoint, model, and bearer token.
ACD then shows one exact plan and explains what can leave the machine. After
you approve it, ACD tests the provider with fixed synthetic text before it
changes anything.

Setup configures one user-level supervisor, merges detected optional
integrations, and runs an isolated checkpoint/publish/restore self-test. It
does not enable the current directory. Run `acd on` once inside every
repository that ACD should protect.

Later compatible setup runs inspect only the shared runtime and integrations.
An incompatible upgrade checkpoints and migrates enabled repositories as one
reviewed transaction. Disabled repositories stay disabled and are migrated
only when you run `acd on` for them. On failure, setup restores every touched
file and prior process or service state.

On macOS, the first ACD mutation starts one shared supervisor for the current
user. Other terminals and agent applications reuse its owner-only socket after
a same-user peer credential check, so switching applications does not require
setup. The process inherits the permissions of the application that started
it and does not require Full Disk Access. After logout or restart, the first
`acd on` or supported agent hook starts it again. Linux uses the persistent
user systemd service.

When the CLI and managed runtime differ, run `acd setup` to review and apply
the upgrade. Ordinary repository commands and integration hooks never replace
the binary or migrate other repositories.

Preview without writes or supervisor/service actions:

~~~bash
acd setup --dry-run
~~~

Noninteractive first setup always requires an exact reviewed JSON plan:

~~~bash
acd setup --dry-run --json
acd setup --yes --non-interactive --expect-plan sha256:... \
  --confirm-intent-repair
~~~

Fresh installations use Everyday Intent/Balanced, imperative commit messages,
the local deterministic provider, structural checks, bounded private repair,
and no diff egress. These are user defaults inherited by this repository and
repositories added later. Existing
repositories retain their effective provider, strategy, preset, verification,
and repair settings during the one-shot v19 to v20 cutover.

## Daily use

Bare `acd` and `acd status` are equivalent and read-only:

~~~text
State: waiting
ACD protection: on
Current changes protected: yes
Published to Git: no
Action needed: no
Status: Current changes are checkpointed; publication is waiting for a safe boundary.
Next: No action needed.
~~~

The five product states are:

| State | Meaning |
|---|---|
| `off` | This repository is intentionally disabled or not configured. |
| `protected` | The latest complete observation is checkpointed and nothing waits for publication. |
| `waiting` | Checkpoints are safe; grouping, verification, Git state, or retry is delaying publication. |
| `publishing` | A selected checkpoint group is being published as a local Git commit. |
| `needs_action` | Protection is incomplete or ACD cannot prove a safe automatic recovery. |

`Current changes protected` is reported separately from the overall state. It
may remain `yes` while publication is `waiting`, `publishing`, or blocked by an
unrelated repair.

An old publication run does not require a branch switch when all of its frozen
changes are already published or safely recovered. ACD proves that state and
completes the run automatically when the worker starts or processes recovery.

## Commands

Root help lists ten everyday commands:

| Command | Purpose |
|---|---|
| `acd setup` | Install or upgrade the shared ACD runtime and integrations. |
| `acd status` | Answer whether changes are enabled, protected, published, and actionable. |
| `acd on` | Register or enable this repository, start its worker, and verify a checkpoint. |
| `acd off` | Complete a final checkpoint, disable protection, and wait for the worker to stop. |
| `acd list` | Show live protection health and commit progress across repositories. |
| `acd commit-all` | Checkpoint now and drain the bounded publication target. |
| `acd history` | List retained checkpoints and their Git publication state. |
| `acd restore ID` | Preview a full-checkpoint restore; add `--yes` to apply it. |
| `acd doctor` | Explain installation or protection problems and the exact next command. |
| `acd uninstall` | Remove managed components while preserving protected data by default. |

`acd list` is the everyday overview. In a terminal it refreshes in place.
Repositories that need action or are working stay visible, and recent
repositories fill the rest of the five-row view. `SAFE` shows checkpoint
coverage, `DRAIN` shows progress through an active publication target, and
`LEFT` shows the remaining target or ordinary pending events. Use `--all` for
every enabled repository, `--verbose` for operational details, or `--once` for
one snapshot. `acd repo list` remains the static registration inventory.

It also links directly to common tasks:

~~~text
acd commit-all --dry-run
acd history rewrite --help
acd config edit
acd repo --help
acd support --help
~~~

Advanced commands are callable under hidden namespaces:

~~~text
acd config get|set|edit|reset|credentials
acd support diagnose|logs|repair|recover|prompt|bundle
acd repo list|remove|gc
acd history activity|explain|rewrite
~~~

Old command names remain hidden compatibility aliases for two releases. A
manual use prints a warning. Optional integrations use hidden local hint APIs
without terminal noise. All aliases invoke the checkpoint-first runtime.

`acd commit-all` does not squash the worktree into one commit. It freezes the
checkpoint-backed event target and lets the configured event or Intent
publication strategy create the same reviewable, atomic local commits it
would create during normal operation. Edits made after the barrier are left
for the next publication pass.

Intent fallback preserves commits that were already published. When new work
depends on a recent private ACD commit, ACD first tries one bounded semantic
replan of that repairable suffix. If replanning fails, ACD keeps the existing
commit OIDs and publishes only the new captures as a dependent commit. That new
commit must have a meaningful generated message. If the provider is
unavailable, publication waits and retries instead of creating a generic
`Update <path>` commit. A completed plan is reused while its repository
fingerprint remains unchanged.

See [the command reference](docs/commands.md) for flags, JSON, exit codes, and
the compatibility map.

## How protection works

Filesystem watching accelerates detection and a complete poll remains the
universal safety path. Optional tool integrations only provide semantic and
session-boundary hints; removing every integration does not reduce capture
coverage.

Each completed checkpoint is a rootless Git commit containing the entire
eligible protected worktree and is retained under:

~~~text
refs/acd/checkpoints/v1/<worktree-id>/<checkpoint-id>
~~~

The ref keeps its Git objects reachable through normal garbage collection.
Checkpoint commits are not parented to `HEAD` or to one another, do not appear
as user branches, and never replace ordinary Git history.

Protection continues during provider failures, verification failures,
detached HEAD, conflicts, branch transitions, and in-progress Git operations.
Publication waits until Git is safe. A checkpoint becomes published only when
all of its captured changes map to completed ordinary local commits.

ACD excludes Git-ignored paths and configured sensitive paths. An unreadable,
unstable, or oversized eligible path makes protection incomplete; ACD never
silently claims the repository is protected after skipping eligible content.

Default retention keeps unpublished checkpoints, restore preimages,
unresolved operations, and the newest checkpoint indefinitely. Published
checkpoints are retained for 30 days and at least the newest 100, with a soft
5 GiB per-worktree content budget. A budget never silently discards
unpublished protection.

See [protection and publication](docs/capture-replay.md) for the durable
protocol and failure behavior.

## Restore

Restore always previews first:

~~~bash
acd history
acd restore cp-...
acd restore cp-... --yes
~~~

Before changing files, ACD checkpoints the current state. It restores the
selected checkpoint into the working tree as a new change, leaves `HEAD` and
the index byte-for-byte unchanged, checkpoints the result, and returns the
pre-restore checkpoint as the undo target.

Restore refuses conflicts, in-progress merge/rebase/cherry-pick/bisect states,
and any staged-path overlap. Detached HEAD is allowed because restore does not
move it. If a post-restore checkpoint is interrupted, run the previewed repair:

~~~bash
acd support repair
acd support repair --yes
~~~

See [user workflows](docs/user-workflows.md) for recovery and support steps.

## Configuration

Inside a worktree, public configuration defaults to repository scope. Outside
a worktree it defaults to global scope. Make scope explicit with
`--scope repo|profile|global`.

~~~bash
acd config get
acd config set commit.preset fast
acd config edit
acd config credentials
~~~

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

Credentials stay in the protected existing credential store. The deterministic
default requires none. Diff egress remains off unless explicitly approved.

See [settings](docs/settings.md), the generated
[configuration reference](docs/configuration-reference.md), and
[AI providers](docs/ai-providers.md).

## Setup, upgrade, and uninstall safety

The v19 to v20 upgrade is one global transaction across every registered
repository. A provable unpublished chain is imported as checkpoint or
recovery-backed history. Any ambiguous chain aborts the complete cutover and
restores all preimages. There is no legacy runtime mode, dual-read period, or
automatic downgrade after commit.

Uninstall preserves repository databases, checkpoint/recovery refs, operation
history, and backups by default:

~~~bash
acd uninstall --dry-run
acd uninstall
~~~

`--purge-data` inventories every target and requires a second explicit
confirmation. Private refs use expected-target CAS, and repository data is
staged reversibly until the uninstall transaction commits.

## Platform files

| Purpose | Location |
|---|---|
| Managed binary | `${XDG_DATA_HOME:-$HOME/.local/share}/acd/bin/acd` |
| Supervisor socket | `${XDG_STATE_HOME:-$HOME/.local/state}/acd/run/supervisor.sock` |
| Supervisor log | `${XDG_STATE_HOME:-$HOME/.local/state}/acd/supervisor.log` |
| Global operation history | `${XDG_DATA_HOME:-$HOME/.local/share}/acd/operations.db` |
| Repository state | `<git-dir>/acd/state.db` |
| macOS lifecycle | Session-owned; no installed service file |
| Linux service | `~/.config/systemd/user/acd-supervisor.service` |

Linux requires a working systemd user manager. ACD does not ship a second
Linux lifecycle implementation.

## Development

~~~bash
make build
make lint
make test
~~~

Repository contribution and verification requirements are in
[`CLAUDE.md`](CLAUDE.md). ACD is MIT licensed.
