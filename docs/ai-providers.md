# AI providers

Fresh interactive setup recommends AI semantic commits. You choose the provider
and approve its access. The explicit deterministic option works without
credentials, network requests, or source sharing. Upgrades preserve saved choices.

| Provider | Credential | Source diff |
|---|---|---|
| `deterministic` | None | Never |
| `openai-compat` | Protected credential store or environment | Only with explicit diff-egress approval and provider declaration |
| `subprocess:<name>` | Provider-specific | Local process receives only its approved input contract |

Open `acd config`, choose global or repository scope, and edit the provider,
model, endpoint, or API key. Save reviews the permissions and tests the connection
before storing a replacement key. A global save also queues inherited changes
for enabled repositories. The standalone credential command remains available:

~~~bash
acd config
acd config credentials set
~~~

`acd config credentials set` stores the key securely and tests it with synthetic
content for the current enabled OpenAI-compatible repository. Use `--repo PATH`
to select one.
After a successful test, ACD queues the repository's existing settings again so
work waiting on corrected credentials can resume. Saved provider choices and
source-sharing approvals stay intact. Other running repositories are unchanged.
An environment key still takes precedence over the stored key.

## Privacy contract

Network content is redacted and bounded. A network provider receives diffs
only when it declares `NeedsDiff` and diff egress is explicitly enabled.
Binary contents never enter planner requests. Native Intent input includes
filenames, operations, blob sizes, file kind, and the reason a diff was omitted.
Text diffs keep the existing redaction, size bounds, and egress permission.
Native metadata marks text diffs shortened by the size limit.
Older provider protocols keep their existing filename and operation fields.
Credentials never enter state, logs, status, diagnostics, traces, plan
fingerprints, or test output. Full provider payloads stay out of ordinary
diagnostics. `ACD_AI_PROMPT_TRACE` is an explicit local opt-in that records
redacted and truncated provider requests which may still contain source text.
Rejected-plan logs omit the raw response by default.
`ACD_INTENT_REJECTS_RAW` is a separate opt-in that stores that response in the
worktree-local reject log described below.

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

Each provider probe gets an `ai.timeout` budget, five minutes by default.
Connection failures and timeouts leave the planning run retryable. Capture and
checkpoint protection continue while publication waits. A failed connection
does not consume a semantic correction attempt or permanently cache a local
message as the completed plan.

The retry schedule is five minutes after the first failure, ten minutes after
the next, then one hour after subsequent failures. The next probe time survives
a worker restart. Only one probe runs at a time; file events still wake capture
immediately. Status and list show the retry countdown, then `provider-retry-due`
when the scheduled time has arrived.

A rejected semantic plan is a separate failure. ACD keeps valid groups and
corrects the unresolved part within the bounded planning session. An unchanged
unresolved goal gets a scheduled review instead of repeated calls every poll.
Incorrect credentials or missing required configuration still require a
settings correction.

Local recovery messages must describe an outcome supported by the captured
changes. Filename messages, raw code symbols, clipped subjects, and generic
phrases such as `Update export code changes` do not pass Intent quality checks.
When ACD cannot write a useful message, it keeps the work protected for later
planning. An outage never grants permission to publish an incomplete goal.

Rejected plans are written to the exact worktree Git directory at
`<gitDir>/acd/planner-rejects.jsonl`. Linked worktrees therefore keep separate
reject logs. By default each row omits the raw response and records its byte
count, SHA-256 digest, typed failure, and a small parsed-plan summary. Explicit
raw-retention opt-in stores the response as well. The current file rotates at
5 MiB and keeps one `.1` file. A reject-log write failure never blocks capture
or publication fallback.
