# Configure ACD

Use `acd configure` once for global provider and everyday commit defaults. It
stages all choices in memory, shows one final preview, tests the selected
provider, and saves global settings without opening repository state.

~~~bash
acd configure
acd configure --strategy intent --preset balanced
acd configure --accessible
~~~

The global wizard offers two experiences and never runs project tests:

| Experience | Internal mode | Verification |
|---|---|---|
| Everyday work | Intent Balanced | ACD structural and materialization gates |
| Maximum speed | Event Fast | None |

Strict Review is available only when targeting one repository:

~~~bash
acd configure --repo .
acd configure --repo . --strategy intent --preset quality --wait
~~~

Repository Everyday and Maximum Speed still run no project tests. Strict
Review detects a full suite, displays its exact command and provenance, and
queues a durable background validation gate that may take several minutes.

Existing valid provider, model, endpoint, timeout, and credential values are
reused. A fresh or incomplete OpenAI-compatible setup asks for the endpoint
and model, then asks for a masked API key when neither the environment nor the
protected file supplies one. Provider timeout, commit format, and custom
verification commands remain advanced settings. Every effective value is
visible in the final review.

Use `acd configure --dry-run` or
`acd configure --dry-run --json` for a side-effect-free preview. Dry-run does
not call a provider, run a command, write a credential or setting, start the
daemon, or change hooks.

One final approval covers every displayed endpoint, diff-egress permission,
and repair permission. Repository Strict Review additionally includes its
exact verification command. Configure tests the provider before any write.
Global setup saves no runtime revision and starts no daemon. Repository
Everyday and Maximum Speed activate immediately; Strict Review atomically
creates one revision, activation request, and validation job, then returns
while validation runs in the background.

Regular setup never asks the user to invent a shell command. Everyday always
uses built-in structural verification. Repository Strict Review first reuses
an approved full command, then checks a repository manifest, Make targets, and
language defaults. Strict Review is unavailable when no full command can be
detected; custom commands belong in `acd settings`.

Use `acd configure --repo . --wait` with Strict Review to stream the durable
job until it passes, fails, or times out. Re-running repository configuration
resumes a queued job or offers to retry the exact failed check, switch
experience, open advanced settings, or leave capture-only state unchanged.

Use `acd settings` for advanced overrides, profiles, experiments, and revision
recovery. Its first action is **Change strategy or preset**, followed by
**Quick provider setup** and **Advanced settings**.

Saved explicit values do not need to be exported or sourced by your shell.
Environment variables still work and remain the compatibility path for
existing installations. Secrets never enter normal settings or runtime
revisions.

## Choose a preset

Presets are immutable built-ins identified by strategy, name, and version.
Runtime revisions materialize every effective field plus `preset_id`,
`preset_version`, and `customized`, so a later catalog update cannot change an
already activated revision.

| Strategy | Preset | Regular behavior |
|---|---|---|
| Event | Fast | Immediate deterministic one-capture commits, no source egress or project verification. |
| Event | Balanced | Immediate commits with a tested provider, redacted diff context, five recent commits, and one locked message rewrite. |
| Event | Quality | Immediate commits with ten recent commits, stricter message validation, and two rewrite attempts. |
| Intent | Fast | Ten-capture evaluations, 10-second quiet time, 90-second maximum wait, structural gates, and no command verification or repair. |
| Intent | Balanced | Twenty-capture evaluations, 30-second quiet time, three-minute maximum wait, structural verification, and repair of up to three commits within ten minutes. |
| Intent | Quality | Thirty-capture evaluations, 60-second quiet time, ten-minute maximum wait, full verification, and repair of up to five commits within thirty minutes. |

Intent defaults to Balanced. Event defaults to Fast. Changing a preset-owned
advanced field preserves its identity and displays, for example,
`Balanced (customized)`. **Reset to preset** removes only preset-owned
overrides at the selected authoring scope.

## Choose what to edit

Repository scope is the default. Profiles provide reusable settings, while
global scope provides defaults for repositories that do not override them.

| Scope | Command | Effect |
|---|---|---|
| Repository | `acd settings` | Saves overrides for the current repository. |
| Named profile | `acd settings --profile fast` | Creates or edits reusable profile `fast`. |
| Global | `acd settings --global` | Saves defaults only. It does not fan changes out to running repositories. |

Use `--repo /path/to/repo` to target another worktree. `--global` cannot be
combined with `--repo` or `--profile`.

The effective value is resolved in this order:

1. Active experiment
2. Repository override
3. Selected profile
4. Global override
5. Environment variable
6. Selected preset
7. Built-in field default

`Source` names the winning authoring layer. When a saved value wins over a set
environment variable, the lab labels that environment value as shadowed.
Remove an explicit value to inherit from the next layer. JSON `null` is not a
stored inherit marker. An experiment is an immutable runtime candidate above
the authoring layers.

## Save, test, and apply a change

The revision rail makes the lifecycle explicit:

| State | Meaning |
|---|---|
| `DRAFT` | The editor contains values that are not active. |
| `TESTED` | The current draft passed local validation and, when selected, the strict provider test. Editing a provider field invalidates this result. |
| `QUEUED` | An immutable desired config revision is stored and waiting for activation. |
| `ACTIVE` | The daemon acknowledged the complete revision at a safe work boundary. |

Use the keyboard to edit a value, then press `t` or `T` to test and `a` to
apply. A required provider-risk confirmation opens inside the current session,
then retries that operation once. An apply never changes a provider in the
middle of a capture or replay pass. Work already in flight finishes with the
old revision, and the next pass leases the new revision as one complete runtime
bundle.

Saving and applying are separate operations. A saved profile or global value
does not activate running repositories. A repository apply creates an
immutable desired revision. If the daemon is stopped, the revision remains
queued for the next start; `acd settings` does not silently start it.

Press `r` to queue a new revision copied from the last-known-good revision.
Use this after a desired revision is rejected. You can also correct the draft,
test it again, and apply the corrected revision. A rejected revision never
partly replaces the active provider or tuning values.

## Know which changes require a restart

Provider, model, commit format and strategy, and intent tuning activate at the
next safe work boundary. The lab labels these fields `next safe boundary`.

Capture limits and filters, filesystem notifications, tracing, retention,
rewind and shadow retention, and client TTL are labeled `restart required`.
Save these changes, then restart the daemon explicitly. The next daemon process
resolves the saved repository, selected-profile, and global values before
initializing its runtime:

~~~bash
acd off
acd on
~~~

The lab refuses to package restart-required changes into a hot config revision.

## Store a provider credential

`acd configure` can save the OpenAI-compatible API key after the final preview.
Advanced scripts can manage the same protected file directly:

~~~bash
printf '%s\n' "$ACD_AI_API_KEY" | acd auth set --stdin
acd auth status
acd auth status --json
acd auth remove --yes
~~~

Interactive `acd auth set` masks input. No flag accepts a literal secret.
`ACD_AI_API_KEY` has priority over the file.

The credential file is
`${XDG_CONFIG_HOME:-$HOME/.config}/acd/credentials.json`. ACD requires an
owner-only `0700` parent directory and a regular owner-only `0600` file. It
rejects symlinks, wrong ownership, broader permissions, malformed JSON,
multiple JSON values, and future schema versions. Writes use a `0600`
same-directory temporary file, fsync, atomic rename, and directory fsync.

Secrets never enter settings, runtime revisions, SQLite, logs, traces,
fingerprints, status JSON, diagnostics, or error text.

## Test a provider safely

The strict provider test makes exactly one synthetic request. A network
provider test may be billed by the configured service. It never sends a
repository path, diff, captured metadata, prompt trace, commit, or experiment
sample, and it does not change planner circuit health.

The lab displays only whether a credential exists and which source wins.

Some configurations need separate, explicit confirmations. The lab asks for
the relevant confirmation inside the current session. The flags below are
optional pre-authorization for scripts or repeat sessions.

| Risk | When it is required | Optional flag |
|---|---|---|
| Credentials sent to a non-default endpoint | Strict synthetic test, apply, or experiment | `--confirm-endpoint-credentials` |
| Provider subprocess execution | Strict synthetic test, apply, or experiment | `--confirm-subprocess` |
| Repository diff egress | Apply or experiment when diff egress is enabled | `--confirm-diff-egress` |

The synthetic test never includes a repository diff, so diff egress consent
does not block testing. Apply and experiment activation still require it when
the chosen provider can receive redacted diffs. One confirmation never implies
either of the other risks.

## Run an accessible session

Use linear prompts when full-screen redraws are unsuitable:

~~~bash
acd settings --accessible
NO_COLOR=1 acd settings --accessible
~~~

`TERM=dumb` selects accessible mode automatically. Rich mode requires
interactive stdin and stdout. Both modes are keyboard-only, use text labels in
addition to color, hide sensitive values, and ask before discarding a dirty
draft.

Accessible mode asks for the next action first. Press Enter on **Test current
settings** to test the existing provider without walking the field catalog.
Choose **Quick provider setup** to edit only provider, model, base URL, timeout,
and CA file. Choose **Advanced settings** for every non-sensitive setting and
its current value. Revert, profile, and experiment actions remain available in
the first menu.

Do not paste an API key into the settings lab. Use `acd configure`,
`acd auth set`, or `ACD_AI_API_KEY`. Missing-key and provider-test errors return
one sanitized next action without printing credentials or provider response
content.

## Approve candidate verification

Balanced uses the built-in structural and materialization gates and does not
run a project command. Quality requires an exact repository-scoped full
command. Configure cannot activate a detected command until it shows the
complete command and receives approval.

| Mode | Preset | Default timeout |
|---|---|---:|
| `none` | Intent Fast | No command |
| `structural` | Intent Balanced | No shell command |
| `fast` | Advanced repository override | 2 minutes |
| `full` | Intent Quality | 10 minutes |

ACD runs the approved command on the exact candidate tree in an ephemeral
detached worktree. Output is bounded, and only the final sanitized 64 KiB can be
retained. Under Strict Review, failure or timeout leaves the candidate pending
and reports `needs_attention`; it never forces publication. Switch that
repository to Everyday with `acd configure --repo .` to remove command
verification.

## Run a bounded experiment

Press `x` after applying a baseline revision to start a ten-window experiment.
The underlying service accepts a positive budget up to 1000 completed planner
windows, an optional future expiry, and either `continue` or `revert` for the
provider-error policy. The candidate must use `commit.strategy=intent` because
event mode does not produce planner windows. Only completed windows consume the
budget, and only one experiment can be active for a repository.

Completion, cancellation, expiry, or the configured failure policy can queue a
new immutable revision copied from the baseline. Existing commits are never
rewritten or removed. Experiment comparisons report exact revision/profile
window counts, primary and fallback outcomes, retries, median latency,
deferrals, forced singletons, and distinct commit OIDs. The results are
descriptive observations from sequential workloads, not causal A/B evidence.
Normal apply and revert operations are rejected while an experiment is active,
so its candidate cannot be superseded before cleanup returns to the baseline.

## Inspect activation state

Use the read-only status surfaces when a revision stays queued or is rejected:

~~~bash
acd status
acd diagnose
acd status --json
acd diagnose --json
~~~

Human and JSON output add saved generation, desired, applied, and
last-known-good config revision IDs, profile, apply state, pending age,
sanitized failure, safe boundary, and experiment progress. Intent v2 output
also shows migration state, preset identity, candidate counts, verification
attention, and repair recovery. Configuration readiness adds experience,
validation status, command provenance, expected or elapsed duration, attempt,
and a sanitized failure tail. Older pre-v14 databases return an empty settings
projection; pre-v15 databases return unavailable Intent v2, and pre-v16
databases return no setup-validation projection. Read-only commands never
migrate these schemas.

Settings are stored in `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json`.
Runtime revisions and experiments use the repository database at
`<gitDir>/acd/state.db`. The runtime ledger is SQLite `SchemaVersion=16` and
stores only canonical, sanitized, non-secret snapshots and bounded validation
results.

See [AI providers](ai-providers.md) for the environment reference and provider
fallback behavior, and [intent commit flow](intent-commit-flow.md) for planner
window semantics.
