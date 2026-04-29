# acd adapter: codex

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Install jq (required): `brew install jq` or `apt install jq`
3. Generate snippet: `acd init codex`
4. Append the printed TOML block to your Codex config (e.g. `~/.codex/config.toml`)
5. Restart Codex

## Verify

- Open Codex in any git repo
- From another shell, run `acd status`
- One client with `harness=codex` should appear
- If a hook fails, inspect `~/.local/state/acd/codex-hook.log`

The snippet uses `CODEX_PROJECT_DIR` for the repo path and falls back to the
hook process working directory when the environment variable is unavailable.

## Uninstall

See [uninstall.md](uninstall.md).
