# acd adapter: pi

For [`KristjanPikhof/pi-yaml-hooks`](https://github.com/KristjanPikhof/pi-yaml-hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup pi`
3. Merge the printed YAML into `~/.pi/hook/hooks.yaml`
4. Restart Pi

If you previously installed a snippet that predates the v2026-05-08 release,
re-run `acd setup pi` and replace the acd block in `~/.pi/hook/hooks.yaml` so
the new self-heal hooks take effect. Run `acd doctor` to check whether your
installed snippet is current; it warns when active hooks are missing `acd start`
or `acd wake` and shows the remediation command.

Tool hooks run idempotent `acd start` before `acd wake`, so later tool activity
can recover if you manually ran `acd stop` while the Pi session stayed open.
`session.deleted` still deregisters the session with `acd stop --session-id`.

## Verify

- Open Pi in any git repo
- From another shell, run `acd status`
- One client with `harness=pi` should appear

## Uninstall

See [uninstall.md](uninstall.md).
