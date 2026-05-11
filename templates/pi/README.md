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

If you previously installed a snippet that predates the v2026-05-08 release,
re-run `acd setup pi` and replace the acd block in `~/.pi/agent/hook/hooks.yaml` so
the new self-heal hooks take effect. Run `acd doctor` to check whether your
installed snippet is current; it warns when active hooks are missing `acd start`
or `acd wake` and shows the remediation command.

**Hook file detected but `acd doctor` still reports detection as `no`?**
The hook file exists but lacks the `# acd-managed: true` marker on its first
line, so `acd doctor` cannot recognise it as an acd-managed file. To fix this,
either:

- **Prepend the marker manually** — open `~/.pi/agent/hook/hooks.yaml` in an
  editor and add `# acd-managed: true` as the very first line.
- **Re-run setup and merge** — run `acd setup pi` (no `>`), copy the printed
  YAML, and merge the acd block into your existing file.

  **Do not use `>` to redirect** when you have custom hooks — `acd setup pi --raw
  > ~/.pi/agent/hook/hooks.yaml` overwrites the entire file and destroys any
  existing entries.

Tool hooks run idempotent `acd start` before `acd wake`, so later tool activity
can recover if you manually ran `acd stop` while the Pi session stayed open.
`session.deleted` still deregisters the session with `acd stop --session-id`.

## Verify

- Open Pi in any git repo
- From another shell, run `acd status`
- One client with `harness=pi` should appear

## Uninstall

See [uninstall.md](uninstall.md).
