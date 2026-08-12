package cli

const rootHelpTemplate = `{{if eq .CommandPath "acd"}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}Usage:
  acd [flags]
  acd <command> [flags]

Commands:
  setup       Safely install or upgrade ACD
  status      Show protection, Git publication, and the next step
  on          Start protecting this repository
  off         Save a final checkpoint and stop protection
  list        Show protection across all enabled repositories
  commit-all  Protect and publish all current changes
  history     Show checkpoints and whether Git contains them
  restore     Preview or bring back a protected checkpoint
  doctor      Explain ACD problems and show how to fix them
  uninstall   Remove ACD and keep protected data by default

Common tasks:
  Preview publishing all:    acd commit-all --dry-run
  Rewrite commit messages:   acd history rewrite --help
  Change settings:           acd config edit
  Manage repositories:       acd repo --help
  Diagnose a problem:        acd doctor
  Advanced recovery tools:   acd support --help

Flags:
{{.Flags.FlagUsages | trimTrailingWhitespaces}}

Run "acd <command> --help" for command details.
{{else}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}{{end}}`
