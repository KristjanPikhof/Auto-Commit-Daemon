# acd adapter: claude-code

## Install

1. Install acd: `curl -fsSL https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh | sh`
2. Generate snippet: `acd setup claude-code`
3. Merge the printed JSON into `~/.claude/settings.json` under the `hooks` key

   **Overwrite warning:** if you redirect `acd setup claude-code --raw` straight
   to `~/.claude/settings.json` with `>`, the entire file is replaced. If you
   have other settings or non-acd hooks in that file, back it up first and merge
   the acd JSON block in manually rather than using `>`.

4. Restart Claude Code

Run `acd doctor` after setup. It compares the installed hook with the current
template and tells you when to regenerate the ACD block with
`acd setup claude-code`.

## Verify

- Open Claude Code in any git repo
- From another shell, run `acd status`
- The output should show one client with `harness=claude-code`

`PreToolUse` and `PostToolUse` run idempotent `acd start` before `acd wake`,
so later tool activity can recover if you manually ran `acd stop` while the
Claude Code session stayed open. `SessionEnd` still deregisters the session
with `acd stop --session-id`.

`acd wake` refreshes the heartbeat and nudges capture/replay, but it does not
bypass `ACD_INTENT_MIN_PENDING` or `ACD_INTENT_MAX_PENDING_AGE`. The `Stop`
hook uses `acd flush --logical` for the prompt-end commit boundary. `acd doctor`
reports template drift when that wiring is missing.

Repo autodiscovery is enabled by default. If you disable it with
`repo_lifecycle.autodiscovery` in `~/.config/acd/config.json` or with
`ACD_REPO_AUTODISCOVERY=disabled`, Claude Code hooks in unregistered repos skip
without creating `.git/acd`. Run `acd repo init` in each repo you want Claude
Code to manage, or temporarily set `ACD_REPO_AUTODISCOVERY=enabled` before
starting the session.

## Uninstall

See [uninstall.md](uninstall.md).
