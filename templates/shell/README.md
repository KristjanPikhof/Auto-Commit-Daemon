# acd adapter: shell

Use the shell adapter when your coding tool does not have a native hook system.
Choose either direnv for one repo or zsh for every repo you enter.

## Install with direnv

Run `acd setup shell`, copy the direnv snippet into the repository's `.envrc`,
then approve it once:

~~~bash
direnv allow
~~~

The snippet registers the current shell when direnv loads the repo and
deregisters it when direnv unloads the environment.

## Install with zsh

Run `acd setup shell` and append the zsh snippet to `~/.zshrc`. Load the change
in the current terminal:

~~~bash
source ~/.zshrc
~~~

The `chpwd` hook registers ACD whenever you enter a Git worktree. The shell PID
keeps the session alive, so ACD removes the client after that shell exits.

## Verify

Enter a Git repository and run:

~~~bash
acd status
~~~

The client list should contain `harness=shell`. Run `acd doctor` if the daemon
does not start or the client is missing.

## Uninstall

Remove the ACD block from `.envrc` or `~/.zshrc`, depending on which form you
installed. Restart the shell, or stop the current repo immediately:

~~~bash
acd stop
~~~

Stopping the daemon does not delete `.git/acd` state.
