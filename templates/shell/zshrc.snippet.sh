# acd-managed: true
# Add to ~/.zshrc to auto-register every shell session inside a git repo.
acd_auto_start() {
    if command -v acd >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
        local repo=$(git rev-parse --show-toplevel)
        export ACD_SESSION_ID=${ACD_SESSION_ID:-$(uuidgen)}
        acd start --session-id "$ACD_SESSION_ID" --harness shell \
                  --watch-pid "$$" --repo "$repo" >/dev/null 2>&1
    fi
}
# Hook into chpwd so every cd into a repo triggers registration.
autoload -U add-zsh-hook
add-zsh-hook chpwd acd_auto_start
acd_auto_start
