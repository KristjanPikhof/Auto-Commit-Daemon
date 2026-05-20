package cli

const rootHelpTemplate = `{{if eq .CommandPath "acd"}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}Usage:
  acd <command> [flags]

Common workflow:
  acd start                         Register this session and ensure the repo daemon is running
  acd status                        Show daemon, branch, and client state for the current repo
  acd events                        Show recent product decisions for the current repo
  acd events --watch                Stream appended product decisions
  acd prompt --last                 Inspect the last recorded AI prompt request
  acd explain --path FILE           Explain why ACD did or did not commit a path
  acd fix --dry-run                 Preview safe remediation for a stuck repo (use --force dry-run for barrier purge plans)
  acd repo init                     Explicitly initialize ACD state for this repo
  acd repo list                     List all registry rows for lifecycle management
  acd repo remove --dry-run          Preview registry removal and state preservation
  acd list                          List known repo daemons
  acd list --watch --interval 5s     Watch known repo daemons live
  acd logs --lines 200              Tail the current repo daemon log as raw JSONL
  acd logs --follow                 Stream appended raw JSONL daemon log lines
  acd wake                          Refresh heartbeat and nudge replay
  acd commit-all                    One-shot: commit every uncommitted file (daemon must be off)
  acd rewrite-commits --from 5 --plan-only  Generate an AI-gated linear rewrite plan without prompts
  acd rewrite-commits --range 5-12 --review  Review/edit proposed messages before applying
  acd rewrite-commits --apply <plan-id> --dry-run  Validate a saved plan before apply
  acd stop                          Stop the repo daemon or deregister a session

Diagnostics and recovery:
  acd diagnose     Inspect replay blockers, waiting queues, and branch anchors
  acd doctor       Run diagnostics and optionally bundle a support zip
  acd pause        Pause capture and replay
  acd resume       Resume capture and replay

Setup:
  acd setup        Print harness install snippets
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
