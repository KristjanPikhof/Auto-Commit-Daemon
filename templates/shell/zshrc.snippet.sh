# acd-managed: true
# Add to ~/.zshrc to wake protection in registered, enabled repositories.
acd_wake_protection() {
    if command -v acd >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
        local repo=$(git rev-parse --show-toplevel)
        acd internal hint --repo "$repo" >/dev/null 2>&1 || true
    fi
}
# Changing directory never enables a repository or publishes work.
autoload -U add-zsh-hook
add-zsh-hook chpwd acd_wake_protection
acd_wake_protection
