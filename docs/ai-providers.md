# AI providers

ACD always has a commit-message provider. If the configured provider fails, it
falls back to the deterministic provider and keeps replay moving.

## Provider choices

| Provider | Configure with | Network? | Diff support |
|---|---|---:|---|
| `deterministic` | Nothing | No | No |
| `openai-compat` | `ACD_AI_PROVIDER=openai-compat` plus `ACD_AI_API_KEY` | Yes | Yes, only with opt-in |
| `subprocess:<name>` | `ACD_AI_PROVIDER=subprocess:<name>` and `acd-provider-<name>` on `$PATH` | Plugin decides | Yes, only with opt-in |

The default is `deterministic`. It uses path and symbol hints, does not call a
model, and does not need a key.

## Quick setup

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
export ACD_AI_TIMEOUT=30s
~~~

Subprocess plugin:

~~~bash
export ACD_AI_PROVIDER=subprocess:mine
export PATH="$PATH:/path/to/plugin/bin"
~~~

Intent grouping:

~~~bash
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=sk-...
export ACD_AI_DIFF_EGRESS=1
~~~

`ACD_AI_DIFF_EGRESS=1` is optional for commit messages, but it usually improves
intent grouping because the planner sees the captured diff instead of metadata
only.

## Diff privacy

Diffs leave the machine only when both checks pass:

| Check | Required value |
|---|---|
| Provider can use diffs | `openai-compat` or `subprocess:<name>` declares `NeedsDiff=true` |
| Operator opts in | `ACD_AI_DIFF_EGRESS=1` or another truthy value |

When either check fails, the provider gets metadata only: path, operation,
branch, repo root, timestamp, and multi-op entries.

When both checks pass, ACD rebuilds the diff from captured `before_oid` and
`after_oid` blobs. It does not read the live worktree for provider payloads.
Before sending, it redacts common secret shapes and truncates:

| Request type | Diff cap |
|---|---:|
| Commit-message request | 4000 bytes |
| Intent planner or message rewrite request | 16000 bytes |

Redaction is a backstop, not a guarantee. Do not enable diff egress for a
private repo unless the endpoint or plugin is trusted.

`ACD_AI_SEND_DIFF` is removed. Use `ACD_AI_DIFF_EGRESS=1`.

## Environment reference

| Variable | Default | Notes |
|---|---:|---|
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic`, `openai-compat`, or `subprocess:<name>`. |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | Must be an absolute HTTPS URL. |
| `ACD_AI_API_KEY` | unset | Required for `openai-compat`; missing key falls back to deterministic. |
| `ACD_AI_MODEL` | `gpt-5.4-mini` | Sent to the OpenAI-compatible endpoint. |
| `ACD_AI_TIMEOUT` | `30s` | Applies to HTTP and subprocess providers. Plain seconds also work. |
| `ACD_AI_CA_FILE` | unset | PEM CA bundle for private HTTPS gateways. |
| `ACD_AI_DIFF_EGRESS` | off | Truthy sends redacted captured diffs when the provider can use them. |
| `ACD_AI_PROMPT_TRACE` | off | Writes local prompt diagnostics under `<gitDir>/acd/prompt-trace/`. |
| `ACD_COMMIT_STRATEGY` | `event` | Set `intent` to ask the planner to group captures. |
| `ACD_COMMIT_FORMAT` | `imperative` | `imperative` keeps the current subject rules; `conventional` opts into scope-less Conventional Commit subjects. |
| `ACD_INTENT_WINDOW` | `10` | Max captures offered to one planner pass. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred pending count before planning. |
| `ACD_INTENT_SETTLE_WINDOW` | `10s` | Burst settle delay after the count gate. `0` disables it. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Age trigger for sparse queues. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent commits sent as compact context. |
| `ACD_INTENT_DEFER_LIMIT` | `1` | Deferrals before forced one-capture planning. |
| `ACD_INTENT_PATH_COALESCE` | off | Truthy restores legacy folding of consecutive same-path captures into one planner offer. |
| `ACD_INTENT_RETRY_ON_INVALID` | `2` | Max correction retries after typed planner validation errors. `0` or false-like values disable retries. |
| `ACD_INTENT_REJECTS_RAW` | off | Truthy stores raw rejected planner responses. Sensitive. |
| `ACD_PATH_QUIESCENCE_SECONDS` | `0` | Waits for paths to go quiet before planner offer. Capture still persists. |
| `ACD_RECENT_COMMIT_AFFINITY_SECONDS` | `0` | Adds a recent-HEAD hint when enabled. Off avoids extra `git log` work. |

Restart the daemon after changing provider, format, or intent environment.

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

Intent planner requests set `request_type` to `intent_plan`, include
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

The top-level `selected_seqs`, `subject`, `body`, and `grouping_reason` remain
required for legacy compatibility. When `commit_groups` is present,
`selected_seqs` must be the union of all group selections; the top-level
message can mirror the first group or summarize the selected window.

ACD's built-in prompt tells the planner to use `commit_groups` for independent
intents inside one visible window. Custom subprocess planners should follow the
same contract instead of returning one broad selected group for unrelated work.

Rules:

| Rule | Why it exists |
|---|---|
| `selected_seqs` must be non-empty | Replay must make progress. |
| Every offered seq must be selected or deferred | The planner cannot ignore work. |
| `commit_groups`, when present, must be ordered and non-overlapping | Replay publishes groups sequentially and must preserve chronology. |
| `deferred_reasons` may mention only deferred seqs | Reasons stay aligned with the plan. |
| `subject` must match `commit_format` | Wrong-format output gets rejected, corrected, or falls back deterministically. |
| Non-empty `error` is a soft error | ACD keeps the plugin alive and falls back for that request. |
| Timeout, EOF, crash, or I/O error is a hard error | ACD kills the plugin and respawns it on the next request. |

Minimal smoke test:

~~~bash
printf '%s\n' '{"version":1,"path":"foo.go","op":"modify","old_path":"","diff":"","repo_root":".","branch":"refs/heads/main","multi_op":[],"now":"2026-04-28T00:00:00Z"}' \
  | acd-provider-mine
~~~

Expected output is one JSON line with a non-empty `subject` and an empty
`error`.

## Security notes

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
| `openai-compat` fails, times out, or has no key | Deterministic fallback. |
| Subprocess response has `error` | Deterministic fallback, plugin stays alive. |
| Subprocess crashes or times out | Deterministic fallback, plugin restarts next time. |

The deterministic provider is the final backstop and should always return a
message.
