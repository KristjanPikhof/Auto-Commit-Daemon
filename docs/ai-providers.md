# AI providers

ACD always has a commit-message provider. Event mode can fall back to a
deterministic message. Intent v2 applies the selected preset's planner policy;
Quality deliberately stops replay instead of publishing a fallback candidate.

## Provider choices

| Provider | Configure with | Network? | Diff support |
|---|---|---:|---|
| `deterministic` | Nothing | No | No |
| `openai-compat` | `acd configure` or `ACD_AI_PROVIDER=openai-compat` | Yes | Yes, only with opt-in |
| `subprocess:<name>` | `ACD_AI_PROVIDER=subprocess:<name>` and `acd-provider-<name>` on `$PATH` | Plugin decides | Yes, only with opt-in |

The default is `deterministic`. It uses path and symbol hints, does not call a
model, and does not need a key.

## Quick setup

Prefer bare `acd configure` for global provider, credential, and consent setup.
Use `acd configure --repo .` only for repository overrides or Strict Review,
and `acd settings` for later advanced overrides. Saved non-secret values do not
need shell sourcing. `ACD_AI_API_KEY` can stay in the environment or the key
can be stored with `acd auth set`; the environment remains higher priority.
The settings file, runtime revision ledger, and repository database never store
the secret.

Configure reuses a valid saved provider, model, endpoint, timeout, and
credential. It asks for the provider only when setup is missing or unusable.
A fresh or incomplete OpenAI-compatible setup asks for the endpoint and model,
then asks for a masked API key only when neither the environment nor protected
file has one. Timeout remains available in `acd settings`. All effective
values are shown in the final review.

Offline default:

~~~bash
export ACD_AI_PROVIDER=deterministic
export ACD_COMMIT_STRATEGY=event
~~~

OpenAI-compatible endpoint:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=sk-...
export ACD_AI_BASE_URL=https://api.openai.com/v1
export ACD_AI_MODEL=gpt-5.4-mini
export ACD_AI_TIMEOUT=5m
~~~

Subprocess plugin:

~~~bash
export ACD_AI_PROVIDER=subprocess:mine
export PATH="$PATH:/path/to/plugin/bin"
~~~

Intent grouping:

~~~bash
acd configure --strategy intent --preset balanced
~~~

Diff context is optional for Event commit messages. Every regular Intent preset
requires bounded, redacted captured diffs. A network provider needs explicit
diff-egress approval; a local subprocess can receive diffs without network
egress.

## Diff privacy

Diffs leave the machine only when both checks pass:

| Check | Required value |
|---|---|
| Provider can use diffs | `openai-compat` or `subprocess:<name>` declares `NeedsDiff=true` |
| Operator opts in | `ACD_AI_DIFF_EGRESS=1` or another truthy value |

When either check fails, Event message generation gets metadata only. Regular
Intent v2 configuration fails its prerequisite check and stops replay while
capture continues.

When both checks pass, ACD rebuilds the diff from captured `before_oid` and
`after_oid` blobs. It does not read the live worktree for provider payloads.
Before sending, it redacts common secret shapes and truncates:

| Request type | Diff cap |
|---|---:|
| Commit-message request | 4000 bytes |
| Intent planner or message rewrite request | 16000 bytes |

Redaction is a backstop, not a guarantee. Do not enable diff egress for a
private repo unless the endpoint or plugin is trusted.

## Environment reference

| Variable | Default | Notes |
|---|---:|---|
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic`, `openai-compat`, or `subprocess:<name>`. |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | Must be an absolute HTTPS URL. |
| `ACD_AI_API_KEY` | unset | Overrides the protected credential file. Required for network planning. |
| `ACD_AI_MODEL` | `gpt-5.4-mini` | Sent to the OpenAI-compatible endpoint. |
| `ACD_AI_TIMEOUT` | `5m` | Applies to each HTTP and subprocess provider request. Plain seconds also work. |
| `ACD_AI_CA_FILE` | unset | PEM CA bundle for private HTTPS gateways. |
| `ACD_AI_DIFF_EGRESS` | off | Truthy sends redacted captured diffs when the provider can use them. |
| `ACD_AI_PROMPT_TRACE` | off | Writes local prompt diagnostics under `<gitDir>/acd/prompt-trace/`. |
| `ACD_COMMIT_STRATEGY` | `event` | Set `intent` to ask the planner to group captures. |
| `ACD_COMMIT_PRESET` | strategy default | `fast`, `balanced`, or `quality`. |
| `ACD_COMMIT_FORMAT` | `imperative` | `imperative` uses verb-led subjects; `conventional` uses scope-less Conventional Commit subjects. |
| `ACD_INTENT_WINDOW` | `10` | Max captures offered to one planner pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred pending count before planning. |
| `ACD_INTENT_SETTLE_WINDOW` | `10s` | Burst settle delay after the count gate. `0` disables it. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Age trigger for sparse queues. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent commits sent as compact context. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals before forced one-capture planning. |
| `ACD_INTENT_PATH_COALESCE` | off | Truthy folds consecutive same-path captures into one planner offer. |
| `ACD_INTENT_RETRY_ON_INVALID` | `2` | Max correction retries after typed planner validation errors. `0` or false-like values disable retries. |
| `ACD_INTENT_VERIFICATION` | `none` | `none`, `fast`, or `full`. Presets provide regular defaults. |
| `ACD_VERIFICATION_FAST_COMMAND` | unset | Exact approved repository command used by Balanced. |
| `ACD_VERIFICATION_FAST_TIMEOUT` | `2m` | Fast candidate verification timeout. |
| `ACD_VERIFICATION_FULL_COMMAND` | unset | Exact approved repository command used by Quality. |
| `ACD_VERIFICATION_FULL_TIMEOUT` | `10m` | Full candidate verification timeout. |
| `ACD_INTENT_REPAIR_ENABLED` | off | Enables eligible recent ACD commit repair. |
| `ACD_INTENT_REPAIR_HORIZON` | `10m` | Maximum repair age. |
| `ACD_INTENT_REPAIR_MAX_COMMITS` | `3` | Maximum chain, capped at five. |
| `ACD_INTENT_REJECTS_RAW` | off | Truthy stores raw rejected planner responses. Sensitive. |
| `ACD_PATH_QUIESCENCE_SECONDS` | `0` | Waits for paths to go quiet before planner offer. Capture still persists. |
| `ACD_RECENT_COMMIT_AFFINITY_SECONDS` | `0` | Adds a recent-HEAD hint when enabled. Off avoids extra `git log` work. |

Environment settings remain compatible. Saved settings can shadow them, and
`acd settings` shows both the active source and any shadowed environment value.
Hot provider and intent values activate as one immutable config revision at the
next safe work boundary. Fields labeled `restart required` still need an
explicit daemon restart; the new process resolves their saved scope precedence
before initializing capture and observability.

`ACD_COMMIT_FORMAT=conventional` accepts only `feat`, `fix`, `docs`,
`refactor`, `test`, `build`, `ci`, `chore`, `perf`, `style`, and `revert`
subjects in the form `type: summary`. Scopes such as `feat(ui): ...` and
breaking markers such as `feat!: ...` are rejected. Format selection changes
provider prompts and validation only; it does not change whether diffs leave the
machine. Diff egress is still controlled only by `ACD_AI_DIFF_EGRESS`.

## Prompt traces

Use prompt tracing only while debugging provider behavior:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=intent acd start
acd prompt --last
acd prompt --seq 42 --json
~~~

The deterministic provider emits no prompt trace because it sends no request.
Trace files are local JSONL records. They are redacted and truncated, but they
can still contain source text, private paths, request envelopes, provider
responses, and fallback metadata. Delete them after collecting the evidence you
need.

## Subprocess plugin protocol

ACD starts one plugin process per daemon lifetime. The binary name must be
`acd-provider-<name>`, and `ACD_AI_PROVIDER` must be `subprocess:<name>`.

Messages use JSONL: one JSON object per line on stdin and stdout.

Commit-message request:

~~~json
{
  "version": 1,
  "path": "src/auth.go",
  "op": "modify",
  "old_path": "",
  "diff": "",
  "repo_root": "/abs/path/to/repo",
  "branch": "refs/heads/main",
  "commit_format": "imperative",
  "multi_op": [],
  "now": "2026-04-28T12:00:00Z"
}
~~~

Commit-message response:

~~~json
{
  "version": 1,
  "subject": "Update auth token expiry check",
  "body": "- Keep expired tokens out of the refresh path",
  "error": ""
}
~~~

Intent v2 starts a subprocess with an ordinary v1 request for compatibility. A
plugin advertises native support by returning
`"capabilities":["intent_plan_v2"]`. Later planner requests use `version: 2`,
`request_type: "intent_plan_v2"`, `planner_protocol: "v2"`, and
`planner_request_v2`. The response envelope is:

~~~json
{
  "version": 2,
  "planner_protocol": "v2",
  "intent_plan_v2": {
    "protocol_version": "v2",
    "candidates": [
      {
        "candidate_id": "checkout-code",
        "selected_seqs": [101, 103],
        "purpose": "validate checkout behavior",
        "readiness": "ready",
        "missing_companions": [],
        "depends_on_candidates": [],
        "subject": "Validate checkout behavior",
        "body": "- Cover the implementation with its focused test",
        "grouping_reason": "implementation and test form one change"
      }
    ]
  }
}
~~~

The native request includes persisted candidate summaries, dependency edges,
activity boundaries, recent soft commits, and prior atomicity findings.
Candidate order is publication order. Every visible capture must appear in
exactly one candidate. Hard dependencies that cross candidates must be
declared in `depends_on_candidates`.

Legacy v1 requests use `request_type: "intent_plan"`, include
`commit_format`, and include `planner_request.offered_captures`. The response
must classify every offered seq. A single-group response still works:

~~~json
{
  "version": 1,
  "selected_seqs": [101],
  "deferred_seqs": [102],
  "subject": "Update auth flow",
  "body": "",
  "grouping_reason": "Focused auth change",
  "deferred_reasons": [
    {"seq": 102, "reason": "Separate config change"}
  ],
  "error": ""
}
~~~

For windows that contain several independent intents, return ordered
`commit_groups`:

~~~json
{
  "version": 1,
  "selected_seqs": [101, 103],
  "deferred_seqs": [102],
  "subject": "Add auth retry handling",
  "body": "- Keep transient failures available for retry",
  "grouping_reason": "Auth retry handling and retry docs are separate commits",
  "commit_groups": [
    {
      "selected_seqs": [101],
      "subject": "Add auth retry handling",
      "body": "- Keep transient failures available for retry",
      "grouping_reason": "Auth behavior change"
    },
    {
      "selected_seqs": [103],
      "subject": "Document retry configuration",
      "body": "",
      "grouping_reason": "Documentation-only update"
    }
  ],
  "deferred_reasons": [
    {"seq": 102, "reason": "Separate billing cleanup"}
  ],
  "error": ""
}
~~~

The top-level `selected_seqs`, `subject`, `body`, and `grouping_reason` are
required. When `commit_groups` is present,
`selected_seqs` must be the union of all group selections; the top-level
message can mirror the first group or summarize the selected window.

ACD adapts valid v1 responses to one-pass candidates and reports
`planner_protocol=v1_compat`. Compatibility preserves safety but cannot claim
native v2 readiness quality. New plugins should negotiate and implement v2.

Rules:

| Rule | Why it exists |
|---|---|
| `selected_seqs` must be non-empty | Replay must make progress. |
| Every offered seq must be selected or deferred | The planner cannot ignore work. |
| Native candidates must assign every visible seq exactly once | Candidate state cannot lose or duplicate durable work. |
| Candidate dependencies must be acyclic and topologically ordered | Replay cannot publish a dependent candidate first. |
| Legacy `commit_groups` must be ordered and non-overlapping | The v1 adapter needs deterministic one-pass membership. |
| `deferred_reasons` may mention only deferred seqs | Reasons stay aligned with the plan. |
| `subject` must match `commit_format` | Wrong-format output gets rejected, corrected, or falls back deterministically. |
| Non-empty `error` is a soft error | ACD keeps the plugin alive and falls back for that request. |
| Timeout, EOF, crash, or I/O error is a hard error | ACD kills the plugin. Event mode restarts it on the next request; intent mode waits until the circuit allows a provider probe. |

Minimal smoke test:

~~~bash
printf '%s\n' '{"version":1,"path":"foo.go","op":"modify","old_path":"","diff":"","repo_root":".","branch":"refs/heads/main","multi_op":[],"now":"2026-04-28T00:00:00Z"}' \
  | acd-provider-mine
~~~

Expected output is one JSON line with a non-empty `subject` and an empty
`error`.

## Security notes

The strict test in `acd configure` or `acd settings` sends exactly one fixed
synthetic request and may incur one provider charge. It sends no repository
path, diff, captured metadata, prompt trace, commit, or experiment sample.
Non-default endpoint and subprocess risks require confirmation before the test.
Diff egress is an activation-only confirmation because the synthetic test
contains no diff. The lab asks inside the current session, while CLI flags can
pre-authorize each specific risk. See
[Test a provider safely](settings.md#test-a-provider-safely).

| Risk | What to do |
|---|---|
| Subprocess plugins inherit daemon privileges | Treat them like any unsandboxed binary on `$PATH`. Pin versions and review source. |
| Network providers can receive source diffs | Keep `ACD_AI_DIFF_EGRESS` off unless the endpoint is approved. |
| Prompt traces can contain source text | Keep them local, review before sharing, delete after debugging. |
| `openai-compat` sends bearer tokens | Use HTTPS endpoints only. ACD rejects plain HTTP and refuses redirects. |

## Fallback behavior

| Scenario | Result |
|---|---|
| Provider unset | Deterministic provider. |
| `openai-compat` succeeds | Provider result is used. |
| Provider returns the wrong message format | ACD rejects the response, retries when configured, then falls back deterministically. |
| Intent Fast planner failure | Publish the smallest valid hard-dependency component. |
| Intent Balanced planner failure | Reuse a valid or deterministic dependency partition, reconnect one-to-one persisted test/source or migration companions, then run structural verification. Ambiguous persisted companions stay pending. |
| Intent Quality planner failure | Keep candidates pending and report `needs_attention`. |
| Three consecutive intent validation failures | Open the persisted circuit after configured correction retries are exhausted. |
| Intent circuit open | Skip the remote planner and apply the preset policy without repeated planner-error decisions. |
| Intent circuit half-open | Allow one provider probe; other evaluations apply the preset policy. |
| Intent v2 has no key or diff consent | Capture continues; replay reports `needs_attention`. |
| Subprocess response has `error` | Deterministic fallback, plugin stays alive. |
| Subprocess crashes or times out | Deterministic fallback; the plugin restarts on the next allowed provider probe. |

The deterministic provider remains the final message backstop. Intent
candidate publication still follows the selected preset policy.

### Inspect the intent planner circuit

Use either read-only command:

~~~bash
acd status --repo . --json
acd diagnose --repo . --json
~~~

Both expose `intent_strategy.planner_health`. The useful fields are:

| Field | Meaning |
|---|---|
| `state` | `closed`, `open`, or `half_open`. |
| `consecutive_failures` | Failures counted toward the current open state. |
| `backoff_level` | Cooldown step: 30 seconds, 2 minutes, then 10 minutes. |
| `next_probe_ts` | Unix timestamp when one half-open probe may run. |
| `opened_ts` | Unix timestamp when the current circuit-open period began. |
| `last_failure_ts` | Unix timestamp of the most recent counted failure. |
| `last_failure_class` | `transport` or `validation`. |
| `last_error` | Bounded, redacted diagnostic text. |
| `bypass_count` | Cumulative windows served by fallback while the circuit denied a provider call. |
| `provider_fingerprint` | Hash of provider, model, sanitized endpoint, and provider mode. |
| `updated_ts` | Unix timestamp of the latest persisted health update. |

If the persisted record is empty, malformed, unsafe to expose, or uses an
unsupported version, `status` and `diagnose` omit `planner_health` and return
`planner_health_warning`. The read path never repairs or deletes the meta row.

The circuit record persists across daemon restarts. A successful half-open
probe closes it. A failed probe advances cooldown from 30 seconds to 2 minutes,
then caps at 10 minutes. API keys are never part of the provider fingerprint;
endpoint credentials, query strings, authorization values, and common token
shapes are removed from stored errors.
