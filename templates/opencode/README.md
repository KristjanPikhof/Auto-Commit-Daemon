# acd adapter: opencode

For [`KristjanPikhof/OpenCode-Hooks`](https://github.com/KristjanPikhof/OpenCode-Hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup opencode`
3. Merge the printed YAML into `~/.config/opencode/hooks.yaml`
4. Restart OpenCode

OpenCode exposes `OPENCODE_SESSION_ID` natively; no jq required.

If you previously installed a snippet that predates the v2026-05-08 release,
re-run `acd setup opencode` and replace the acd block in
`~/.config/opencode/hooks.yaml` so the new self-heal hooks take effect. Run
`acd doctor` to check whether your installed snippet is current; it warns when
active hooks are missing `acd start` or `acd wake` and shows the remediation
command.

Tool hooks run idempotent `acd start` before `acd wake`, so later tool activity
can recover if you manually ran `acd stop` while the OpenCode session stayed
open. `session.deleted` still deregisters the session with
`acd stop --session-id`.

## Verify

- Open OpenCode in any git repo
- From another shell, run `acd status`
- One client with `harness=opencode` should appear

## Uninstall

See [uninstall.md](uninstall.md).
