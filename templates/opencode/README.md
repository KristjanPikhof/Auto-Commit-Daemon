# acd adapter: opencode

For [`KristjanPikhof/OpenCode-Hooks`](https://github.com/KristjanPikhof/OpenCode-Hooks).

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup opencode`
3. Merge the printed YAML into `~/.config/opencode/hook/hooks.yaml`

   **Overwrite warning:** if you redirect `acd setup opencode --raw` straight
   to `~/.config/opencode/hook/hooks.yaml` with `>`, the entire file is replaced. If
   you have other hooks or settings in that file, back it up first and merge the
   acd YAML block in manually rather than using `>`.

4. Restart OpenCode

OpenCode exposes `OPENCODE_SESSION_ID` natively; no jq required.

If you previously installed a snippet that predates the v2026-05-08 release,
re-run `acd setup opencode` and replace the acd block in
`~/.config/opencode/hook/hooks.yaml` so the new self-heal hooks take effect. Run
`acd doctor` to check whether your installed snippet is current; it warns when
active hooks are missing `acd start` or `acd wake` and shows the remediation
command.

**Hook file detected but `acd doctor` still reports detection as `no`?**
The hook file exists but lacks the `# acd-managed: true` marker on its first
line, so `acd doctor` cannot recognise it as an acd-managed file. To fix this,
either:

- **Prepend the marker manually** — open `~/.config/opencode/hook/hooks.yaml` in
  an editor and add `# acd-managed: true` as the very first line.
- **Re-run setup and merge** — run `acd setup opencode` (no `>`), copy the
  printed YAML, and merge the acd block into your existing file.

  **Do not use `>` to redirect** when you have custom hooks — `acd setup opencode
  --raw > ~/.config/opencode/hook/hooks.yaml` overwrites the entire file and
  destroys any existing entries.

Tool hooks run idempotent `acd start` before `acd wake`, so later tool activity
can recover if you manually ran `acd stop` while the OpenCode session stayed
open. `session.deleted` still deregisters the session with
`acd stop --session-id`.

`acd wake` refreshes the heartbeat and nudges capture/replay, but it does not
bypass `ACD_INTENT_MIN_PENDING` or `ACD_INTENT_MAX_PENDING_AGE`. The
`session.idle` hook uses `acd flush --logical` for the prompt-end commit
boundary; re-run `acd setup opencode` if your installed snippet predates that
hook.

Repo autodiscovery is enabled by default. If you disable it with
`repo_lifecycle.autodiscovery` in `~/.config/acd/config.json` or with
`ACD_REPO_AUTODISCOVERY=disabled`, OpenCode hooks in unregistered repos skip
without creating `.git/acd`. Run `acd repo init` in each repo you want OpenCode
to manage, or temporarily set `ACD_REPO_AUTODISCOVERY=enabled` before starting
the session.

## Verify

- Open OpenCode in any git repo
- From another shell, run `acd status`
- One client with `harness=opencode` should appear

## Uninstall

See [uninstall.md](uninstall.md).
