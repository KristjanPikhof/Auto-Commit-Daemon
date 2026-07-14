# Configure ACD with the settings lab

Use `acd settings` to inspect, test, and activate provider, model, commit, and
intent settings. The lab shows the active value beside the draft value, where
the draft came from, and when a change can take effect.

~~~bash
acd settings
~~~

Start with **Test current settings**. It runs the strict synthetic provider
test without making you edit every field first. If the provider is not ready,
the failure tells you which environment value or provider setting to correct.

Saved explicit values do not need to be exported or sourced by your shell.
Environment variables still work and remain the compatibility path for
existing installations. `ACD_AI_API_KEY` is the exception: it is environment
only and is never written to the settings file or revision ledger.

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
6. Built-in default

`Source` names the winning layer. When a saved value wins over a set
environment variable, the lab also labels that environment value as shadowed.
Remove an explicit value in the editor to inherit from the next layer. JSON
`null` is not a stored inherit marker.

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
Save these changes, then restart the daemon explicitly:

~~~bash
acd off
acd on
~~~

The lab refuses to package restart-required changes into a hot config revision.

## Test a provider safely

The strict provider test makes exactly one synthetic request. A network
provider test may be billed by the configured service. It never sends a
repository path, diff, captured metadata, prompt trace, commit, or experiment
sample, and it does not change planner circuit health.

API keys stay in `ACD_AI_API_KEY`. The lab displays only `set` or `unset`, and
errors, fingerprints, saved settings, and runtime revisions never contain the
key.

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

Do not paste an API key into the settings lab. Set `ACD_AI_API_KEY` in the
environment and run `acd settings` again. Missing-key and provider-test errors
return one sanitized next action without printing credentials or provider
response content.

## Run a bounded experiment

Press `x` after applying a baseline revision to start a ten-window experiment.
The underlying service accepts a positive budget up to 1000 completed planner
windows, an optional future expiry, and either `continue` or `revert` for the
provider-error policy. Only completed windows consume the budget, and only one
experiment can be active for a repository.

Completion, cancellation, expiry, or the configured failure policy can queue a
new immutable revision copied from the baseline. Existing commits are never
rewritten or removed. Experiment comparisons report exact revision/profile
window counts, primary and fallback outcomes, retries, median latency,
deferrals, forced singletons, and distinct commit OIDs. The results are
descriptive observations from sequential workloads, not causal A/B evidence.

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
sanitized failure, safe boundary, and experiment progress. Older pre-v14 state
databases return an empty settings projection without migration.

Settings are stored in `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json`.
Runtime revisions and experiments use the repository database at
`<gitDir>/acd/state.db`. The runtime ledger is SQLite `SchemaVersion=14` and
stores only canonical, sanitized, non-secret snapshots.

See [AI providers](ai-providers.md) for the environment reference and provider
fallback behavior, and [intent commit flow](intent-commit-flow.md) for planner
window semantics.
