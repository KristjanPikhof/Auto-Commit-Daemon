# AI Providers

`acd` generates commit messages through a `Provider` interface (§10.1). Three implementations ship in v1: `deterministic` (rule-based, always available), `openai-compat` (HTTP to any OpenAI-compatible endpoint), and `subprocess` (JSONL protocol to an external binary). The default is `deterministic`; opt into the others via environment variables. Providers are composed so that any error in the primary falls back to `deterministic` automatically.

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
# Optional overrides:
# export ACD_AI_BASE_URL=https://api.openai.com/v1
# export ACD_AI_MODEL=gpt-4o-mini
```

**Subprocess plugin:**

```sh
export ACD_AI_PROVIDER=subprocess:my-provider
export PATH=$PATH:/path/to/plugin/dir
# acd will exec acd-provider-my-provider from $PATH
```

---

## Environment variables

Source of truth: `internal/ai/config.go`.

| Variable | Default | Notes |
|---|---|---|
| `ACD_AI_PROVIDER` | `deterministic` | `deterministic` \| `openai-compat` \| `subprocess:<name>` |
| `ACD_AI_BASE_URL` | `https://api.openai.com/v1` | openai-compat only |
| `ACD_AI_API_KEY` | (none) | openai-compat only; missing key degrades to deterministic with a warning |
| `ACD_AI_MODEL` | `gpt-4o-mini` | openai-compat only |
| `ACD_AI_TIMEOUT` | `30s` | per-request hard timeout; applies to subprocess and openai-compat; accepts Go duration (`30s`) or plain seconds (`30`) |

Unrecognized `ACD_AI_PROVIDER` values degrade to `deterministic` with a warning log; the daemon never silently disables commit-message generation.

---

## Plugin protocol

Source of truth: `internal/ai/plugin_subprocess.go` package comment (§10.3).

Subprocess plugins are external binaries on `$PATH` named `acd-provider-<name>`. Set `ACD_AI_PROVIDER=subprocess:<name>` to activate one.

### Wire format

One JSON object per line in both directions (JSONL). The `version` field exists for future negotiation without breaking older plugins.

**Request (daemon → plugin, one line per commit event):**

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
`diff` is a unified diff capped at 4000 bytes before transmission (see `DiffCap` in `internal/ai/prompt.go`).

**Response (plugin → daemon, one line per request):**

```json
{
  "version": 1,
  "subject": "Update auth token expiry check",
  "body": "- modify src/auth.go\n- Snapshot seq: 142 tool: acd",
  "error": ""
}
```

`subject` must be non-empty for a successful response. `body` may be empty. Set `error` to a non-empty string to signal a soft error (see lifecycle below).

### Lifecycle

- The daemon spawns the plugin binary **once per daemon lifetime** and multiplexes all commit-message requests over the single stdin/stdout pair. The plugin protocol is single-threaded by contract; the daemon serializes requests on its side too.
- **Per-request timeout** defaults to `30s` (controlled by `ACD_AI_TIMEOUT`). On timeout the plugin process is killed; the next `Generate` call respawns it from scratch.
- **Soft errors**: a response with a non-empty `error` field keeps the plugin process alive. Only the current request fails, allowing `Compose` to fall back to `deterministic`.
- **Hard errors** (timeout, unexpected EOF, I/O failure, exit): the plugin is killed and marked crashed. The next `Generate` call respawns the binary transparently.
- **Shutdown**: `Close()` sends EOF on stdin and waits up to 5 seconds for a clean exit before escalating to SIGKILL. The daemon calls `Close()` at shutdown so plugins are always reaped.

---

## Example plugin: bash skeleton

The following script is a minimal but runnable subprocess provider. It requires `jq` for JSON parsing; see the note below if `jq` is not available.

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

- Plugins run as **subprocesses of the daemon** and inherit its full process privileges: file-system access, network access, environment variables (including secrets), and the ability to invoke `git` commands.
- The daemon reads from your repository and writes commits. A malicious or compromised plugin can read and exfiltrate your source code or push tampered commits.
- **Vetting plugins is entirely the operator's responsibility.** Treat every third-party `acd-provider-*` binary exactly as you would any unsandboxed binary on your `$PATH`: pin versions, review source, audit network calls, and prefer running the daemon under a restricted system user.

### openai-compat diffs leave your machine

- The openai-compat provider sends file diffs (truncated to 4000 bytes) to `ACD_AI_BASE_URL/chat/completions`. When `ACD_AI_BASE_URL` points to the public OpenAI API those diffs are transmitted to OpenAI's infrastructure.
- **Do not enable `ACD_AI_PROVIDER=openai-compat` on private or sensitive repositories without explicit consent and a fully verified `ACD_AI_BASE_URL`.** If you run a local proxy or self-hosted model, set `ACD_AI_BASE_URL` to that endpoint and verify it does not forward requests upstream.
- The default HTTP client refuses 3xx redirects to prevent the bearer token from being steered to a different host by a hostile network.

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
