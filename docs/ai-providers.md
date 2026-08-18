# AI providers

AI is optional. Fresh setup uses the deterministic provider and requires no
credential, network request, or source egress.

| Provider | Credential | Source diff |
|---|---|---|
| `deterministic` | None | Never |
| `openai-compat` | Protected credential store or environment | Only with explicit diff-egress approval and provider declaration |
| `subprocess:<name>` | Provider-specific | Local process receives only its approved input contract |

Configure through the advanced namespace:

~~~bash
acd config set ai.provider deterministic
acd config credentials
acd config edit
~~~

## Privacy contract

Network content is redacted and bounded. A network provider receives diffs
only when it declares `NeedsDiff` and diff egress is explicitly enabled.
Credentials, raw provider failures, and unredacted source never enter state,
logs, status, diagnostics, traces, plan fingerprints, or test output.

Strict provider tests use fixed synthetic content and no repository source.
First setup tests the selected provider after review and before any write. Its
scratch self-test does not call a network provider.

## OpenAI-compatible endpoints

The provider supports an explicit base URL, model, timeout, CA file, and API
key. Endpoint credentials are stored in the existing protected credential
file. Environment credentials win without being persisted.

Use a custom endpoint only after reviewing its data handling. A saved endpoint
does not itself grant diff egress.

HTTPS is the safe default. HTTP requires a separate warning approval because
the bearer token and request content can be read or changed in transit. ACD
refuses redirects and endpoint URLs with embedded credentials, query strings,
fragments, control characters, or unsupported schemes. A custom CA certificate
is available under the optional connection settings.

## Subprocess providers

Subprocess providers are unsandboxed local programs and inherit worker
privileges. Pin and review them like any executable on `PATH`. Protocol v1 is
adapted for compatibility and cannot claim native Intent readiness.

## Failure behavior

Provider failures do not affect completed checkpoints or `protected=true`.
Malformed plans are repaired locally, partially replanned, or replaced with a
verified evidence partition. This applies to Fast, Balanced, and Quality.

Only connection, timeout, protocol transport, and unavailable-service failures
open the provider circuit. It uses 30-second, 2-minute, then 10-minute
cooldowns and permits one half-open probe. A rejected semantic plan does not
change transport health. Cancellation releases a probe without changing
provider health.

Rejected plans are written to the exact worktree Git directory at
`<gitDir>/acd/planner-rejects.jsonl`. Linked worktrees therefore keep separate
reject logs. By default each row omits the raw response and records its byte
count, SHA-256 digest, typed failure, and a small parsed-plan summary. Explicit
raw-retention opt-in stores the response as well. The current file rotates at
5 MiB and keeps one `.1` file. A reject-log write failure never blocks capture
or publication fallback.
