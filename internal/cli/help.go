package cli

const rootHelpTemplate = `{{if eq .CommandPath "acd"}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}Usage:
  acd [flags]
  acd <command> [flags]

Commands:
  setup       Install or upgrade ACD transactionally
  status      Show whether this repository is protected and published
  on          Enable protection for this repository
  off         Complete a final checkpoint and disable protection
  commit-all  Checkpoint and publish all current changes
  history     Show checkpoints and their Git publication state
  restore     Preview or restore a checkpoint into the working tree
  doctor      Explain installation and protection problems
  uninstall   Remove ACD while preserving protected data by default

Flags:
{{.Flags.FlagUsages | trimTrailingWhitespaces}}

Run "acd <command> --help" for command details.
{{else}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}{{end}}`
