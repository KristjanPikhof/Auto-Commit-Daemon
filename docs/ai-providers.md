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
Setup and its scratch self-test never call a network provider.

## OpenAI-compatible endpoints

The provider supports an explicit base URL, model, timeout, CA file, and API
key. Endpoint credentials are stored in the existing protected credential
file. Environment credentials win without being persisted.

Use a custom endpoint only after reviewing its data handling. A saved endpoint
does not itself grant diff egress.

## Subprocess providers

Subprocess providers are unsandboxed local programs and inherit worker
privileges. Pin and review them like any executable on `PATH`. Protocol v1 is
adapted for compatibility and cannot claim native Intent readiness.

## Failure behavior

Transport, validation, safety, timeout, and provider circuit failures delay
publication only. Completed checkpoints and `protected=true` remain intact.
Fast and Balanced presets use bounded deterministic fallback where their
existing gates allow it. Quality may keep publication waiting with an exact
next action.

The provider circuit opens immediately on transport failure or after three
validation/safety failures, uses 30-second, 2-minute, then 10-minute cooldowns,
and permits one half-open probe. Cancellation releases a probe without
changing provider health.
