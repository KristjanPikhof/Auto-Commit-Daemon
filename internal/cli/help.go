package cli

const rootHelpTemplate = `{{if eq .CommandPath "acd"}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}Usage:
  acd <command> [flags]

Common workflow:
  acd start        Register this session and ensure the repo daemon is running
  acd status       Show daemon, branch, and client state for the current repo
  acd list         List known repo daemons
  acd list --watch Watch known repo daemons live
  acd logs         Tail the current repo daemon log
  acd wake         Refresh heartbeat and nudge replay
  acd stop         Stop the repo daemon or deregister a session

Diagnostics and recovery:
  acd diagnose     Inspect replay blockers and branch anchors
  acd doctor       Run diagnostics and optionally bundle a support zip
  acd recover      Retarget stale replay state after branch incidents
  acd pause        Pause capture and replay
  acd resume       Resume capture and replay
  acd purge-events Delete non-published queued events

Setup:
  acd init         Print harness install snippets
  acd version      Print version and build info

Advanced:
  acd stats        Show aggregate commits, events, and bytes
  acd gc           Prune dead or missing repo registry entries
  acd touch        Refresh heartbeat without waking replay

Flags:
{{.Flags.FlagUsages | trimTrailingWhitespaces}}

Use "acd <command> --help" for command details.
{{else}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}{{end}}`
