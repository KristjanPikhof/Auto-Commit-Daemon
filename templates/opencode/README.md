# acd adapter: opencode

For [`KristjanPikhof/OpenCode-Hooks`](https://github.com/KristjanPikhof/OpenCode-Hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd init opencode`
3. Merge the printed YAML into your OpenCode-Hooks `hooks.yaml`
4. Restart OpenCode

OpenCode exposes `OPENCODE_SESSION_ID` natively; no jq required.

## Verify

- Open OpenCode in any git repo
- From another shell, run `acd status`
- One client with `harness=opencode` should appear

## Uninstall

See [uninstall.md](uninstall.md).
