# acd adapter: pi

For [`KristjanPikhof/Pi-YAML-Hooks`](https://github.com/KristjanPikhof/Pi-YAML-Hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup pi`
3. Merge the printed YAML into `~/.pi/agent/hook/hooks.yaml`

   **Overwrite warning:** if you redirect `acd setup pi --raw` straight to
   `~/.pi/agent/hook/hooks.yaml` with `>`, the entire file is replaced. If you have
   other hooks or settings in that file, back it up first and merge the acd YAML
   block in manually rather than using `>`.

4. Restart Pi

Run `acd doctor` after setup. It compares the installed hook with the current
template and tells you when to regenerate the ACD block with `acd setup pi`.

If the hook file exists but `acd doctor` does not recognize it, check its first
line for `# acd-managed: true`. Add the marker or run `acd setup pi` and merge
the printed YAML into the existing file. Do not redirect raw output over a file
that contains custom hooks.

Tool hooks run idempotent `acd start` before `acd wake`, so later tool activity
can recover if you manually ran `acd stop` while the Pi session stayed open.
`session.deleted` still deregisters the session with `acd stop --session-id`.

`acd wake` refreshes the heartbeat and nudges capture/replay, but it does not
bypass `ACD_INTENT_MIN_PENDING` or `ACD_INTENT_MAX_PENDING_AGE`. The
`session.idle` hook uses `acd flush --logical` for the prompt-end commit
boundary. `acd doctor` reports template drift when that wiring is missing.

Repo autodiscovery is enabled by default. If you disable it with
`repo_lifecycle.autodiscovery` in `~/.config/acd/config.json` or with
`ACD_REPO_AUTODISCOVERY=disabled`, Pi hooks in unregistered repos skip without
creating `.git/acd`. Run `acd repo init` in each repo you want Pi to manage, or
temporarily set `ACD_REPO_AUTODISCOVERY=enabled` before starting the session.

## Verify

- Open Pi in any git repo
- From another shell, run `acd status`
- One client with `harness=pi` should appear

## Uninstall

See [uninstall.md](uninstall.md).
