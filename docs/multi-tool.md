# Running ACD next to another auto-committer

ACD can share a repo with another tool that also creates commits. If the other
tool lands the same captured state first, ACD settles its queued event at
`HEAD` instead of making a duplicate commit.

## What happens

~~~mermaid
flowchart TB
  A["File changes"] --> B["ACD captures<br/>pending event"]
  A --> C["Other committer<br/>creates commit"]
  C --> D["HEAD advances"]
  B --> E["ACD replay tick"]
  D --> E
  E --> F{"HEAD already has<br/>captured after-state?"}
  F -->|yes| G["Mark event published<br/>at external HEAD"]
  F -->|no| H["Continue replay<br/>or block on mismatch"]

  classDef event fill:#243447,stroke:#7aa2f7,color:#e6edf3
  classDef decision fill:#3d2f1f,stroke:#f6c177,color:#fff4d6
  classDef ok fill:#203a31,stroke:#9ece6a,color:#eaffdf
  classDef block fill:#402b2b,stroke:#f7768e,color:#ffe8ee
  class A,B,C,D,E event
  class F decision
  class G ok
  class H block
~~~

ACD checks the current `HEAD` tree against the captured after-state for every
op. For deletes, the path must already be absent. It also checks ancestry so a
coincidental tree match does not hide a real branch divergence.

## Expected decisions

| Decision | Meaning |
|---|---|
| `already_published_by_external_committer` | Replay would have blocked, but `HEAD` already matches the captured final state. |
| `already_published_no_op_tree` | The op set produced no tree change. |
| `already_published_after_cas_exhaustion` | `update-ref` retries failed, then `HEAD` was found to contain the captured state. |
| `handled_external_after_block` | A previously blocked row self-healed after an external commit landed the captured state. |

User-facing commands show these as `handled_external`,
`handled_external_after_block`, or `superseded_external`.

## Common setups

| Setup | Result |
|---|---|
| ACD is the only committer | Simplest. Replay writes the commits. |
| Another hook commits first | ACD usually settles its matching event at the external commit. |
| Both tools race on different content | ACD may produce `blocked_conflict`. |
| Codex ACD hook plus another auto-commit plugin | Same settle behavior; watch `acd events`. |

Codex `Stop` calls `acd touch`, not logical flush. Claude Code, OpenCode, and Pi
snippets use `acd flush --logical` at their natural idle or stop boundary.

If repo autodiscovery is disabled, hook-driven starts manage only repos that
were registered with `acd repo init`.

## Edge cases that still block

| Case | Why it blocks |
|---|---|
| Mode-only mismatch | Blob matches, but the captured file mode does not. |
| Rename source mismatch | The source path state no longer matches the captured rename op. |
| Symlink target mismatch | Symlink blobs encode target strings, so a different target is different content. |
| Ancestry divergence | Current `HEAD` does not descend from the replay parent. |
| Partial external commit | Some, but not all, captured ops are present at `HEAD`. |

Recovery:

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
acd diagnose --repo .
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
~~~

Normal `acd fix --yes` proves a chain at stable `HEAD` when possible and archives
it under `refs/acd/recovery/*` otherwise. Use `--force` only when you explicitly
want archive-only recovery without attempting the publish proof. Neither path
discards captured changes.

## Recommended configurations

| Option | When to use | Commands |
|---|---|---|
| Let ACD be sole committer | You want fewer moving parts. | Disable other auto-commit hooks, then run `acd setup <harness>`. |
| Accept external settle | You need another hook too. | Watch `acd events --watch` and keep `blocked_conflicts` at `0`. |
| Debug settle decisions | You need internal proof. | Start with `ACD_TRACE=1`, then inspect `.git/acd/trace/*.jsonl`. |

Trace filter:

~~~bash
grep already_published .git/acd/trace/*.jsonl | python3 -c \
  'import sys,json; [print(json.loads(line).get("decision"), json.loads(line).get("input")) for line in sys.stdin]'
~~~

## See also

| Doc | Use |
|---|---|
| [user-workflows.md](user-workflows.md) | Daily status, explain, fix, and support flows. |
| [capture-replay.md](capture-replay.md) | Scratch-index replay and conflict probes. |
| [intent-commit-flow.md](intent-commit-flow.md) | Intent batch waits and logical flush. |
