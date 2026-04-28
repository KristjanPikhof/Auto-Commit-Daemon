# acd adapter: pi

For [`KristjanPikhof/pi-yaml-hooks`](https://github.com/KristjanPikhof/pi-yaml-hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd init pi`
3. Merge the printed YAML into your `.pi/hook/hooks.yaml`
4. Restart Pi

## Verify

- Open Pi in any git repo
- From another shell, run `acd status`
- One client with `harness=pi` should appear

## Uninstall

See [uninstall.md](uninstall.md).
