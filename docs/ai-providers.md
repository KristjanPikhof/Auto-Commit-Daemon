# AI Providers

`acd` generates commit messages through a `Provider` interface (§10.1). Three implementations ship in v1: `deterministic` (rule-based, always available), `openai-compat` (HTTP to any OpenAI-compatible endpoint), and `subprocess` (JSONL protocol to an external binary). The default is `deterministic`; opt into the others via environment variables. Providers are composed so that any error in the primary falls back to `deterministic` automatically.

By default — regardless of provider — AI providers receive metadata only:
path, operation, branch, repo root, multi-op entries, and timestamp;
`diff` is always empty. Diff egress requires **two** independent signals:

1. The selected provider declares `NeedsDiff=true`. The deterministic
   provider does not; `openai-compat` and `subprocess:<name>` do.
2. The operator has explicitly opted in via `ACD_AI_DIFF_EGRESS=1`.

Both must be true for the daemon to populate the `diff` field.
`ACD_AI_DIFF_EGRESS` is **off by default** even when a network provider
is selected: this is a deliberate privacy floor so an upgrade does not
silently start transmitting source bytes. When a network provider is
selected without the opt-in, the daemon emits a one-shot startup warn
and continues with metadata only.

When both signals are true, the diff handed to AI providers is
reconstructed from the `before_oid` and `after_oid` blobs captured in
SQLite at write time — **not from the live worktree**. This means the
model sees exactly what changed at the moment of capture, even if the
file has been edited many times since. Before transmission, the diff is
scrubbed for obvious secret shapes (AWS access keys, Slack/GitHub
tokens, bearer tokens, JWTs, private-key markers, assigned
password/secret/token values, and high-entropy token-like strings), then
capped at 4000 bytes (`DiffCap` in `internal/ai/prompt.go`) while diff
sections are appended. Large diffs may stop mid-section once the provider
budget is consumed. The deterministic provider does not consult the diff at
all, so its output is identical regardless of diff reconstruction success or
failure. See [capture-replay.md](capture-replay.md) for the full storage model.

Diff size is bounded in two places. Each `before_oid`/`after_oid` pair is
rendered through `git.DiffBlobsLimited` with a per-op cap of
`2 * internal/ai.DiffCap` and a 5s timeout; on overflow the partial prefix is
returned alongside `git.ErrStdoutOverflow` so truncation is observable instead
of silent. `BuildOpsDiff` then keeps the rendered event diff within the
4000-byte provider budget while sections are appended, and callers still apply
redaction plus a final truncate before sending.

### Migration from `ACD_AI_SEND_DIFF`

`ACD_AI_SEND_DIFF` was removed. The daemon emits a one-shot deprecation
warn-log at startup when it is set in environment. Replacement:

- Previously `ACD_AI_SEND_DIFF=1` + `ACD_AI_PROVIDER=openai-compat`
  → now `ACD_AI_DIFF_EGRESS=1` + `ACD_AI_PROVIDER=openai-compat`.
- Previously `ACD_AI_SEND_DIFF` unset + `ACD_AI_PROVIDER=openai-compat`
  → now `ACD_AI_DIFF_EGRESS` unset + `ACD_AI_PROVIDER=openai-compat`
  (metadata only — same effective behavior, but make the absence explicit
  if any tooling reads your env file).

---

## Quick start

**Deterministic (default) — no configuration needed.**

```sh
# Nothing to set. acd commits with rule-based messages out of the box.
```

**openai-compat:**

```sh
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=sk-...
# Diff egress is OFF by default. Opt in explicitly:
export ACD_AI_DIFF_EGRESS=1
# Optional overrides:
# export ACD_AI_BASE_URL=https://api.openai.com/v1
# export ACD_AI_MODEL=gpt-4o-mini
```

**Subprocess plugin:**

```sh
export ACD_AI_PROVIDER=subprocess:my-provider
export PATH=$PATH:/path/to/plugin/dir
# acd will exec acd-provider-my-provider from $PATH
# Subprocess providers declare NeedsDiff=true. To actually send diffs
# (vs. metadata only), also opt in:
export ACD_AI_DIFF_EGRESS=1
```

---

## Environment variables

Source of truth: `internal/ai/config.go` and `internal/daemon/message.go`.

| Variable | Default | Notes |
|---|---|---|
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic` \| `openai-compat` \| `subprocess:<name>` |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | openai-compat only; must be an absolute `https://` URL |
| `ACD_AI_API_KEY` | (none) | openai-compat only; missing key degrades to deterministic with a warning |
| `ACD_AI_MODEL` | `gpt-4o-mini` | openai-compat only |
| `ACD_AI_TIMEOUT` | `30s` | per-request hard timeout; applies to subprocess and openai-compat; accepts Go duration (`30s`) or plain seconds (`30`) |
| `ACD_AI_CA_FILE` | (none) | openai-compat only; optional PEM CA bundle for private HTTPS gateways |
| `ACD_COMMIT_STRATEGY` | `event` | `event` keeps one captured event per commit. `intent` asks the AI planner to select one or more offered captures for the next commit. |
| `ACD_INTENT_WINDOW` | `10` | Maximum pending captures offered to the planner in a normal window. |
| `ACD_INTENT_MIN_PENDING` | `10` | Preferred pending-count gate before a normal planner window starts. |
| `ACD_INTENT_MAX_PENDING_AGE` | `5m` | Bounded wait escape hatch for sparse pending queues that have not reached `ACD_INTENT_MIN_PENDING`. |
| `ACD_INTENT_RECENT_COMMITS` | `5` | Recent branch/path commits included as compact context. |
| `ACD_INTENT_DEFER_LIMIT` | `2` | Deferrals allowed before the oldest overdue capture is forced into a one-capture window. |
| `ACD_AI_DIFF_EGRESS` | unset | Truthy (`1`/`true`/`yes`) opts in to sending reconstructed diffs. Off by default; metadata-only payload otherwise. Has no effect for `deterministic`. |
| `ACD_AI_PROMPT_TRACE` | unset | Truthy (`1`/`true`/`yes`) writes local AI prompt diagnostics to `<gitDir>/acd/prompt-trace/` when a non-deterministic provider sends a request. Treat as sensitive: records are post-redaction/truncation but may still contain source text. |

Unrecognized `ACD_AI_PROVIDER` values degrade to `deterministic` with a warning log; the daemon never silently disables commit-message generation.

---

## Intent commit strategy

`ACD_COMMIT_STRATEGY=event` is the compatibility default. Replay drains pending
captures in FIFO order and keeps the current one-event commit behavior.

`ACD_COMMIT_STRATEGY=intent` changes only replay grouping. ACD offers a bounded
pending window to the configured AI provider as structured `capture_intent_plan`
input. `ACD_INTENT_WINDOW` is the maximum offered, `ACD_INTENT_MIN_PENDING` is
the preferred normal trigger, and `ACD_INTENT_MAX_PENDING_AGE` bounds how long a
sparse queue waits before planning anyway. The planner must classify every
offered seq as selected or deferred. It may select exactly one capture, select
any larger non-empty subset, or defer unrelated captures. ACD validates the plan
before touching git, applies selected captures in seq order through the same
scratch-index path, writes one commit, and marks all selected events with the
same `commit_oid`.

If a capture is deferred repeatedly, ACD eventually sends a forced-aging window
containing only that overdue capture. That keeps intent grouping from starving
small or hard-to-name edits.

Planner contract: every entry in `deferred_reasons` must reference a seq that
appears in `deferred_seqs`. Reasons attached to a selected seq, or to a seq
outside the offered window, are invalid by contract. The `openai-compat` and
`subprocess` providers normalize their own response before returning: if a
planner emits a spurious `deferred_reasons` entry, the provider drops that
entry, logs a single warning naming the affected seqs, and hands the cleaned
plan to the validator. A plan that still fails validation after normalization
is surfaced as `intent_planner_error` and replay falls back to deterministic
one-capture commits as before.

Batching behavior is deliberately bounded:

- `ACD_INTENT_WINDOW` is a ceiling, not a target. A normal pass offers at most
  that many visible pending captures.
- `ACD_INTENT_MIN_PENDING` is the preferred count trigger. Until the visible
  queue reaches it, intent replay usually waits for a better grouping window.
- `ACD_INTENT_MAX_PENDING_AGE` is the age escape hatch. Sparse repos publish
  when the oldest visible pending capture reaches that age even if the count
  trigger was not met.
- `acd wake` and other explicit flush requests bypass only the batch wait. They
  do not bypass planner validation, ordering checks, terminal replay barriers,
  or the forced-aging rules.
- Forced aging can still select a one-capture window after repeated deferrals;
  if an earlier related-path capture must land first, ACD preserves that order.

Setup choices:

~~~bash
# Safe default for CI, shared branches, and compatibility-sensitive repos.
export ACD_COMMIT_STRATEGY=event
~~~

~~~bash
# Reviewer-friendly local work, metadata only.
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
~~~

~~~bash
# Private/self-hosted endpoint with explicit diff egress.
export ACD_COMMIT_STRATEGY=intent
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
export ACD_AI_BASE_URL=https://ai.example.internal/v1
export ACD_AI_DIFF_EGRESS=1
~~~

Troubleshooting:

- `acd status` and `acd diagnose --json` show active strategy, deferred count,
  forced-aging readiness, and the last planner error.
- When commits are waiting for a larger batch, `acd status`, `acd diagnose`,
  and `acd doctor` show visible pending count, `min_pending`, oldest pending
  age, `max_pending_age`, and the age-trigger countdown.
- `acd events --watch` shows grouped seqs, deferrals, forced-aging decisions,
  and planner validation failures from the decision ledger.
- If the AI provider fails or returns an invalid plan, ACD records the planner
  error and falls back to a deterministic one-capture plan rather than
  corrupting queue order.
- Recovery remains the same: inspect with `acd diagnose --json`, preview with
  `acd fix --dry-run --json`, then apply with `acd fix --yes` only when the
  daemon is stopped.

---

## Prompt tracing and `acd prompt`

Prompt tracing is off by default. Enable it only while debugging AI behavior:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=event acd start
acd prompt --last
acd prompt --seq 42 --json
~~~

In `event` mode, the trace shows the commit-message request for one captured
event: strategy, provider, model, seq, redaction/truncation metadata, system
prompt, user prompt, request envelope, response, and any fallback reason.
The default deterministic provider does not send an AI request, so it emits no
prompt trace.

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=intent acd start
acd prompt --last
acd prompt --seq 42
~~~

In `intent` mode, `--seq` matches either the event seq or any offered seq in an
intent planner window. The trace shows `offered_seqs`, selected/deferred seqs,
grouping reason, validation errors, and deterministic fallback records when the
planner output cannot be trusted.

Trace files are stored locally as JSONL under `<gitDir>/acd/prompt-trace/`.
They are captured after ACD redacts and truncates outbound payloads, but they
may still contain source code, private paths, prompt text, tool schemas, request
envelopes, provider responses, and fallback metadata. Do not enable
`ACD_AI_PROMPT_TRACE` on sensitive repos unless the local diagnostic value is
worth that risk. Files are grouped by UTC day and are not pruned automatically;
the async writer buffers up to 256 pending records and drops the oldest buffered
record if it falls behind. Remove the prompt-trace directory after
sharing-safe evidence has been collected.

---

## Plugin protocol

Source of truth: `internal/ai/plugin_subprocess.go` package comment (§10.3).

Subprocess plugins are external binaries on `$PATH` named `acd-provider-<name>`. Set `ACD_AI_PROVIDER=subprocess:<name>` to activate one.

### Wire format

One JSON object per line in both directions (JSONL). The `version` field exists for future negotiation without breaking older plugins.

**Commit-message request (daemon → plugin, one line per commit event):**

```json
{
  "version": 1,
  "path": "src/auth.go",
  "op": "modify",
  "old_path": "",
  "diff": "@@ -10,6 +10,7 @@\n ...",
  "repo_root": "/abs/path/to/repo",
  "branch": "refs/heads/main",
  "multi_op": [
    {"path": "src/auth.go", "op": "modify", "old_path": ""}
  ],
  "now": "2026-04-28T12:00:00Z"
}
```

`op` values: `create` | `modify` | `delete` | `rename` | `mode` | `symlink`.  
`multi_op` is present when one daemon event covers more than one file.  
`diff` is empty unless **both** the selected provider declares `NeedsDiff=true` and the operator has set `ACD_AI_DIFF_EGRESS=1`. When both signals are set, it is a unified diff built from captured `before_oid`/`after_oid` blobs stored in SQLite — not from the live worktree — so it accurately reflects the change at capture time even if the file has been modified since. Secret-like values are redacted before the diff is capped at 4000 bytes (`DiffCap` in `internal/ai/prompt.go`).

**Commit-message response (plugin → daemon, one line per request):**

```json
{
  "version": 1,
  "subject": "Update auth token expiry check",
  "body": "- modify src/auth.go\n- Snapshot seq: 142 tool: acd",
  "error": ""
}
```

`subject` must be non-empty for a successful response. `body` may be empty. Set `error` to a non-empty string to signal a soft error (see lifecycle below).

**Intent-planner request (daemon → plugin, one line per planning window):**

When `ACD_COMMIT_STRATEGY=intent` is active, the same subprocess can also
receive planning requests. The envelope has `request_type: "intent_plan"` and a
`planner_request` payload. The payload includes the offered captures, recent
commit context, branch/repo metadata, and whether the window is forced aging.

```json
{
  "version": 1,
  "request_type": "intent_plan",
  "planner_request": {
    "repo_root": "/abs/path/to/repo",
    "branch": "refs/heads/main",
    "forced_aging": false,
    "offered_captures": [
      {"seq": 101, "path": "src/auth.go", "op": "modify"}
    ],
    "recent_commits": [
      {"oid": "abc123", "subject": "Update auth flow"}
    ]
  }
}
```

**Intent-planner response (plugin → daemon):**

```json
{
  "version": 1,
  "selected_seqs": [101],
  "deferred_seqs": [],
  "subject": "Update auth flow",
  "body": "",
  "grouping_reason": "single focused auth change",
  "deferred_reasons": [],
  "error": ""
}
```

Every offered seq must appear in either `selected_seqs` or `deferred_seqs`.
`selected_seqs` must be non-empty. Add one `{ "seq": <id>, "reason": "..." }`
entry to `deferred_reasons` for every deferred seq. Invalid responses are
recorded as planner errors and fall back to safe one-capture planning.

### Lifecycle

- The daemon spawns the plugin binary **once per daemon lifetime** and multiplexes all commit-message requests over the single stdin/stdout pair. The plugin protocol is single-threaded by contract; the daemon serializes requests on its side too.
- **Per-request timeout** defaults to `30s` (controlled by `ACD_AI_TIMEOUT`). On timeout the plugin process is killed; the next `Generate` call respawns it from scratch.
- **Soft errors**: a response with a non-empty `error` field keeps the plugin process alive. Only the current request fails, allowing `Compose` to fall back to `deterministic`.
- **Hard errors** (timeout, unexpected EOF, I/O failure, exit): the plugin is killed and marked crashed. The next `Generate` call respawns the binary transparently.
- **Shutdown**: `Close()` sends EOF on stdin and waits up to 5 seconds for a clean exit before escalating to SIGKILL. The daemon calls `Close()` at shutdown so plugins are always reaped.
- **Stderr**: plugin stderr is captured for diagnostics. By default it is appended to `~/.local/state/acd/plugin-<name>.log`; tests or embedders can override this with `SubprocessOptions.Stderr`.

---

## Example plugin: bash skeleton

The following script is a minimal commit-message provider. It requires `jq` for
JSON parsing. It does not implement `request_type: "intent_plan"`; if you use it
with `ACD_COMMIT_STRATEGY=intent`, ACD falls back to deterministic intent
planning while still using the plugin for commit-message requests.

```bash
#!/usr/bin/env bash
# acd-provider-mine: a minimal commit message provider.
# Reads JSONL requests on stdin, writes JSONL responses on stdout.

set -euo pipefail

while IFS= read -r line; do
  # Parse the request fields we need.
  path=$(printf '%s' "$line" | jq -r '.path')
  op=$(printf '%s' "$line"   | jq -r '.op')

  subject="$op $(basename "$path")"

  # Write one JSONL response line.
  printf '%s\n' "$(jq -n --arg s "$subject" '{version:1, subject:$s, body:"", error:""}')"
done
```

`jq` is used here for convenience only; a real plugin can use Python, Go, Rust, `awk`, or any tool that can parse and emit JSON. A Python equivalent of the same logic:

```python
#!/usr/bin/env python3
import json, sys, os

for line in sys.stdin:
    req = json.loads(line)
    if req.get("request_type") == "intent_plan":
        offered = req["planner_request"]["offered_captures"]
        first = offered[0]
        sys.stdout.write(json.dumps({
            "version": 1,
            "selected_seqs": [first["seq"]],
            "deferred_seqs": [c["seq"] for c in offered[1:]],
            "subject": f"{first['op']} {os.path.basename(first['path'])}",
            "body": "",
            "grouping_reason": "choose the first focused capture",
            "deferred_reasons": [
                {"seq": c["seq"], "reason": "separate follow-up capture"}
                for c in offered[1:]
            ],
            "error": "",
        }) + "\n")
        sys.stdout.flush()
        continue
    path = req.get("path", "")
    op   = req.get("op", "modify")
    subject = f"{op} {os.path.basename(path)}"
    sys.stdout.write(json.dumps({"version": 1, "subject": subject, "body": "", "error": ""}) + "\n")
    sys.stdout.flush()
```

### Installation

1. Save the script as `acd-provider-mine` anywhere on `$PATH`.
2. Make it executable: `chmod +x /usr/local/bin/acd-provider-mine`
3. Activate it: `export ACD_AI_PROVIDER=subprocess:mine`

The `acd-provider-` prefix is mandatory; the part after the prefix must match the `<name>` in `subprocess:<name>`.

---

## Fallback semantics

Every provider selection resolves to a `Compose(primary, deterministic)` chain. `Result.Source` records which provider actually answered.

| Scenario | Effective provider | `Result.Source` |
|---|---|---|
| `ACD_AI_PROVIDER` unset or `deterministic` | deterministic | `deterministic` |
| `openai-compat`, any error (5xx, network, parse, timeout, missing key) | deterministic fallback | `deterministic` |
| `openai-compat`, success | openai-compat | `openai-compat` |
| `subprocess:<name>`, soft error (`error` field non-empty) | deterministic fallback | `deterministic` |
| `subprocess:<name>`, hard error (timeout / crash / EOF) | deterministic fallback | `deterministic` |
| `subprocess:<name>`, success | plugin | `subprocess:<name>` |

The `deterministic` provider never fails. It always produces a message and is the terminal backstop for every error path.

---

## Security note

> **Read this section before enabling a subprocess plugin or pointing openai-compat at an external endpoint.**

### Subprocess plugins

- Plugins run as **subprocesses of the daemon** and inherit its full process privileges: file-system access, network access, environment variables (including secrets), the operator's Git credentials, and the ability to invoke `git` commands, including `git push`.
- The daemon reads from your repository and writes commits. A malicious or compromised plugin can read and exfiltrate your source code or push tampered commits.
- **Vetting plugins is entirely the operator's responsibility.** Treat every third-party `acd-provider-*` binary exactly as you would any unsandboxed binary on your `$PATH`: pin versions, review source, audit network calls, and prefer running the daemon under a restricted system user.

### Diffs can leave your machine

- The daemon sends an empty `diff` while the deterministic provider is selected.
- Selecting a network `ACD_AI_PROVIDER` (`openai-compat` or `subprocess:<name>`) is **not** sufficient to enable diff egress. The operator must additionally set `ACD_AI_DIFF_EGRESS=1` (or `true`/`yes`). Without that opt-in the daemon emits a one-shot startup warn and continues with metadata only.
- With both signals set, the openai-compat provider sends redacted file diffs (truncated to 4000 bytes) to `ACD_AI_BASE_URL/chat/completions`. When `ACD_AI_BASE_URL` points to the public OpenAI API those diffs are transmitted to OpenAI's infrastructure.
- With both signals set, subprocess plugins receive the same redacted, truncated diff over stdin.
- Redaction is best-effort and pattern-based. It is a backstop, not a guarantee that arbitrary secrets or proprietary code cannot be transmitted.
- **Do not enable `ACD_AI_DIFF_EGRESS` on private or sensitive repositories without explicit consent and a verified endpoint or plugin.** If you run a local proxy or self-hosted model, set `ACD_AI_BASE_URL` to that endpoint and verify it does not forward requests upstream.
- `ACD_AI_BASE_URL` must be an absolute `https://` URL. Plain HTTP and relative URLs are rejected before the OpenAI-compatible provider is built.
- The default HTTP client refuses 3xx redirects to prevent the bearer token from being steered to a different host by a hostile network.

### Prompt traces stay local but can contain code

- `ACD_AI_PROMPT_TRACE=1` persists prompt request and response diagnostics under
  `<gitDir>/acd/prompt-trace/` when a non-deterministic provider sends a
  request; there is no automatic upload.
- Records are written after ACD redaction/truncation, but the prompt may still
  include source code, private paths, diffs allowed by `ACD_AI_DIFF_EGRESS`, and
  provider responses.
- Files are daily JSONL logs with no automatic pruning. ACD buffers 256 pending
  prompt-trace records in memory and drops the oldest buffered record under
  sustained writer backpressure.
- Treat prompt traces like raw logs from a private repository. Review or delete
  them before sharing a repo archive, support bundle, or debug artifact.

---

## Verifying your plugin

Smoke-test a plugin before wiring it into the daemon:

```sh
echo '{"version":1,"path":"foo.go","op":"modify","old_path":"","diff":"","repo_root":".","branch":"refs/heads/main","multi_op":[],"now":"2026-04-28T00:00:00Z"}' \
  | acd-provider-mine
```

Expected output: one JSON line containing a non-empty `subject` field and an empty `error` field, for example:

```json
{"version":1,"subject":"modify foo.go","body":"","error":""}
```

If the plugin exits immediately without writing a response, check that it handles `multi_op` being an empty array (the field is present when provided; plugins should treat it as optional).
