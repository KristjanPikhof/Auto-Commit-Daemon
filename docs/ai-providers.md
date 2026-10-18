# AI providers

Local finish is offline. It builds no provider, reads no credentials, launches
no provider process, and sends no source content. Intent requires an explicitly
configured and approved AI provider.

| Mode or provider | Execution | Input |
|---|---|---|
| Local (`deterministic`) | Inside the worker | No provider request |
| Intent (`openai-compat`) | Reviewed HTTP endpoint | Bounded metadata and approved redacted text |
| Intent (`subprocess:<name>`) | Approved local executable | The same bounded planning contract |

Selecting Intent with `deterministic` is invalid. Choose Local for offline work
or configure an AI transport in `acd config`.

## Credentials and configuration

~~~bash
acd config
acd config credentials status
acd config credentials set
~~~

The protected credential store is
`${XDG_CONFIG_HOME:-$HOME/.config}/acd/credentials.json`, under an owner-only
directory with a regular `0600` file. An environment API key takes precedence
over the stored key and is not persisted.

Saving credentials or applying reviewed settings makes no provider request.
Validation checks configuration and permissions locally. An explicitly invoked
connection test through the settings API sends one fixed synthetic request,
uses no repository source, and has a maximum 30-second budget. It does not retry
or become a background readiness check. The command-line editor does not
require a successful connection test to Save.

Provider credential values stay out of repository state, plan fingerprints,
ordinary logs, JSON, diagnostics, and support output. Errors use redacted summaries. When
sharing older opt-in trace files, inspect them first: redacted diagnostic
content can still contain source text. New Finish does not use the retired
planner's raw rejection log or automatic prompt-trace writer.

## Privacy contract

Diff egress is off by default. Intent metadata includes paths, recorded
operations and versions, object identities, sizes, and file kind. Supplied
purpose and message can also enter the provider request; ACD redacts their
provider copies. The exact valid user message remains local for publication.
Approving text diffs permits their redacted content to be sent to the selected
provider. A local subprocess is also a recipient; being local does not grant
it implicit permission.

Without shared text diffs, a new Intent finish requires completion context or
an exact batch message. Describe the problem, approach, and constraints in the
quoted purpose. ACD does not enable diff sharing automatically. Metadata-only
previews say that code diffs were not shared. When recorded intermediate versions
are used, approved diffs describe each exact transition, not repeated final diffs.

Binary or invalid-text blobs are metadata only. Their complete objects remain
available locally for exact tree verification. Limits are independent of
checkpoint storage:

| Input | Maximum |
|---|---|
| One textual path diff | 128 KiB |
| Aggregate finish diffs | 512 KiB |
| Aggregate history diffs | 512 KiB |
| Serialized planning request, including a correction | 512 KiB per exchange |

ACD measures complete input before sending. It fails oversized evidence with
`provider_input_too_large` and guidance to choose Local or a smaller explicit
history range. It does not clip text or silently omit eligible changes.

Saved disclosure identifies the attempted provider input and redaction. Encoded
request byte counts measure attempted exchanges; they do not prove the peer
received a payload after a network failure.

## OpenAI-compatible endpoints

Configure the base URL, model, timeout, and optional CA certificate in
`acd config`. Custom endpoint credential disclosure, unencrypted HTTP, and
diff egress have separate approvals. HTTPS is the default. HTTP can expose the
bearer token and content in transit.

ACD rejects redirects, embedded URL credentials, query strings, fragments,
control characters, and unsupported schemes. Each exchange performs one HTTP
request with bounded response size and cancellation.

## Subprocess providers

Subprocess providers are unsandboxed programs with worker privileges. Review
the executable and its protocol before approving it. The retained transport
accepts a Finish planning exchange; old event, candidate, and repair protocols
are not planning alternatives. Cancellation and Close terminate the owned
process group and join its readers within bounded cleanup.

## Failure behavior

Intent allows one planning exchange and at most one combined correction under
a single persisted deadline. `ai.timeout` defaults to five minutes. Transport
failure, missing credentials, invalid output, or deadline exhaustion fails the
request. There is no automatic retry timer or Local fallback.

Messages-only history planning follows the same rule. Invalid response framing,
including an unsupported protocol version, requires correction or fails the
request even when the enclosed goals are valid.

Completed checkpoints remain protected. Correct the cause and explicitly retry
with `acd finish --retry REQUEST_ID`, or choose Local in reviewed settings and
start a new finish. Retry keeps the frozen target; it does not include later
edits. Status, list, and doctor never contact a provider.
