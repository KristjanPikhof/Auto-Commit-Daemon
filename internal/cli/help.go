package cli

const rootHelpTemplate = `{{if eq .CommandPath "acd"}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}Usage:
  acd [flags]
  acd <command> [flags]

Primary controls:
  acd              Show one read-only health classification and next action
  acd on           Enable this repo and ensure its daemon is running
  acd off          Durably disable this repo while preserving state
  acd status       Show the detailed current-repo snapshot
  acd setup        Print the one-time harness install snippet

Observe:
  acd list         Show all active repos
  acd events       Follow capture, grouping, publish, and block decisions
  acd explain      Explain one path or commit decision

Support and recovery:
  acd diagnose     Inspect replay blockers and branch anchors read-only
  acd doctor       Inspect installation/runtime health or create a support bundle
  acd fix          Preview or apply advanced recovery actions

Advanced:
  acd repo         Manage explicit registration and lifecycle details
  acd logs         Read raw daemon logs
  acd pause        Pause capture and replay for repository surgery
  acd resume       Resume a manually paused repo
  acd commit-all   Capture a dirty worktree while the daemon is off
  acd rewrite-commits  Plan or apply an explicit local history rewrite
  acd stats / gc / prompt  Inspect aggregate or internal state

Hook protocol (normally managed by acd setup):
  acd start / stop / wake / touch / flush

Flags:
{{.Flags.FlagUsages | trimTrailingWhitespaces}}

Use "acd <command> --help" for command details.
{{else}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}{{end}}`
