# acd adapter: shell (universal fallback)

For tools without a hook system: rely on the shell to register/deregister.

## direnv

Append `direnv.envrc.snippet` to your repo's `.envrc`. `direnv allow` once.

## zsh

Append `zshrc.snippet.sh` to `~/.zshrc`. The `chpwd` hook registers acd whenever you `cd` into a git repo.

Either approach uses `--harness shell`. PID-based liveness keeps the daemon alive only while a registered shell is running.
