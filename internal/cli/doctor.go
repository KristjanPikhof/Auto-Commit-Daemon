package cli

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	daemonpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

type doctorTargetContextKey struct{}

func withDoctorTarget(ctx context.Context, repo string) context.Context {
	return context.WithValue(ctx, doctorTargetContextKey{}, strings.TrimSpace(repo))
}

// doctorRepoReport is the per-repo block inside the doctor report.
type doctorRepoReport struct {
	Path                     string                    `json:"path"`
	RepoHash                 string                    `json:"repo_hash"`
	StateDB                  string                    `json:"state_db"`
	StateDBReadable          bool                      `json:"state_db_readable"`
	DaemonPID                int                       `json:"daemon_pid"`
	DaemonAlive              bool                      `json:"daemon_alive"`
	DaemonProcessCount       int                       `json:"daemon_process_count,omitempty"`
	DaemonProcessPIDs        []int                     `json:"daemon_process_pids,omitempty"`
	DaemonMode               string                    `json:"daemon_mode"`
	HeartbeatTS              int64                     `json:"heartbeat_ts,omitempty"`
	HeartbeatAgeS            int64                     `json:"heartbeat_age_seconds,omitempty"`
	HeartbeatStale           bool                      `json:"heartbeat_stale"`
	Clients                  int                       `json:"client_count"`
	Harnesses                []string                  `json:"harnesses,omitempty"`
	LogPath                  string                    `json:"log_path"`
	LogLines                 []string                  `json:"log_tail,omitempty"`
	FsnotifyMode             string                    `json:"fsnotify_mode,omitempty"`
	FsnotifyWatches          int                       `json:"fsnotify_watches,omitempty"`
	FsnotifyDropped          int                       `json:"fsnotify_dropped,omitempty"`
	FsnotifyFallbackReason   string                    `json:"fsnotify_fallback_reason,omitempty"`
	LastCaptureError         string                    `json:"last_capture_error,omitempty"`
	PendingEvents            int                       `json:"pending_events"`
	BlockedConflicts         int                       `json:"blocked_conflicts"`
	FailedEvents             int                       `json:"failed_events"`
	FailedBlockingPending    int                       `json:"failed_blocking_pending"`
	IntentStrategy           intentStrategyReport      `json:"intent_strategy"`
	Configuration            configReadinessReport     `json:"configuration"`
	Replay                   replayObservabilityReport `json:"replay"`
	IntentV2                 intentV2Report            `json:"intent_v2"`
	SelfPublication          selfPublicationReport     `json:"self_publication"`
	LastReplayConflictTS     int64                     `json:"last_replay_conflict_ts,omitempty"`
	LastReplayConflictPath   string                    `json:"last_replay_conflict_path,omitempty"`
	LastReplayConflictErr    string                    `json:"last_replay_conflict_error,omitempty"`
	LastReplayFailureTS      int64                     `json:"last_replay_failure_ts,omitempty"`
	LastReplayFailurePath    string                    `json:"last_replay_failure_path,omitempty"`
	LastReplayFailureErr     string                    `json:"last_replay_failure_error,omitempty"`
	Notes                    []string                  `json:"notes,omitempty"`
	FlushSessionID           string                    `json:"-"`
	Busy                     bool                      `json:"busy"`
	OperationalState         string                    `json:"operational_state"`
	WorktreeClean            bool                      `json:"worktree_clean"`
	AllChangesCommittedInGit bool                      `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                      `json:"checkpoint_published_by_acd"`
	PublicationDrain         publicationDrainReport    `json:"publication_drain"`
}

type doctorHarnessReport struct {
	Name           string `json:"name"`
	ConfigPath     string `json:"config_path"`
	ConfigPresent  bool   `json:"config_present"`
	ConfigReadable bool   `json:"config_readable"`
	MarkerFound    bool   `json:"marker_found"`
	Installed      bool   `json:"installed"`
	// MatchedPath is the candidate path that actually carried the acd
	// marker on disk, when it differs from ConfigPath (i.e., the marker
	// lives on a legacy fallback path rather than the canonical primary).
	// Empty when the marker is on the canonical ConfigPath or no marker
	// was found. JSON consumers use this to learn which file the user
	// must edit; the text renderer surfaces it as a "marker found at"
	// line so users know remediation will be a merge-only nudge toward
	// the canonical layout.
	MatchedPath string `json:"matched_path,omitempty"`
	// ConfigReadError carries the os.ReadFile error string when the
	// primary config exists (ConfigPresent=true) but we could not read
	// it (EACCES / EIO / etc). When non-empty, ConfigReadable is false
	// AND MarkerFound is false purely because the body was unreadable —
	// not because the marker is genuinely absent. JSON consumers must
	// use this to disambiguate "marker missing" from "fell back to
	// alternate-path detection because the primary was unreadable".
	ConfigReadError string   `json:"config_read_error,omitempty"`
	Notes           []string `json:"notes,omitempty"`
}

type doctorAIReport struct {
	Provider             string   `json:"provider"`
	APIKeySet            bool     `json:"api_key_set,omitempty"`
	ProviderCommand      string   `json:"provider_command,omitempty"`
	ProviderCommandFound bool     `json:"provider_command_found,omitempty"`
	ProviderCommandPath  string   `json:"provider_command_path,omitempty"`
	Notes                []string `json:"notes,omitempty"`
}

// doctorReport is the full report rendered by `acd doctor` and embedded in
// `manifest.json` of the doctor bundle.
type doctorReport struct {
	GeneratedAt          string                `json:"generated_at"`
	ACDVersion           string                `json:"acd_version"`
	GitVersion           string                `json:"git_version,omitempty"`
	GitPath              string                `json:"git_path,omitempty"`
	Uname                string                `json:"uname,omitempty"`
	GoVersion            string                `json:"go_version"`
	GoOS                 string                `json:"go_os"`
	GoArch               string                `json:"go_arch"`
	UlimitNoFile         int64                 `json:"ulimit_nofile,omitempty"`
	InotifyMaxUserWatch  int64                 `json:"inotify_max_user_watches,omitempty"`
	RegistryPath         string                `json:"registry_path"`
	RegistryRepoCount    int                   `json:"registry_repo_count"`
	SensitiveGlobsEnv    string                `json:"sensitive_globs_env"`
	SensitiveGlobsActive []string              `json:"sensitive_globs_active"`
	SafeIgnoreEnv        string                `json:"safe_ignore_env"`
	SafeIgnoreExtraEnv   string                `json:"safe_ignore_extra_env"`
	SafeIgnoreActive     []string              `json:"safe_ignore_active"`
	Harnesses            []doctorHarnessReport `json:"harnesses"`
	AI                   doctorAIReport        `json:"ai"`
	Repos                []doctorRepoReport    `json:"repos"`
}

func newDoctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Collect install/runtime diagnostics and support bundles",
		Long: `Collect broad install and runtime diagnostics across acd state.

Doctor checks the central registry, daemon liveness, harness installs, AI provider settings, safe-ignore and sensitive-glob configuration, fsnotify status, and recent daemon log tails. Use --bundle to write a zip with sanitized diagnostic files for sharing.

Doctor is the support/health command, not the daily "why" view. Use acd status for the current repo snapshot, acd events to follow product decisions, acd explain for path or commit answers, and acd diagnose for focused replay/branch blockers.`,
		Example: `  acd doctor
  acd doctor --json
  acd doctor --bundle
  acd doctor --bundle --output /tmp
  acd explain --path internal/state/schema.go
  acd events --watch`,
		RunE: func(c *cobra.Command, args []string) error {
			jsonOut, _ := c.Flags().GetBool("json")
			bundle, _ := c.Flags().GetBool("bundle")
			outputDir, _ := c.Flags().GetString("output")
			return runDoctor(c.Context(), c.OutOrStdout(), bundle, outputDir, jsonOut)
		},
	}
	cmd.Flags().Bool("bundle", false, "Write a doctor zip to ~/Downloads (or --output)")
	cmd.Flags().String("output", "", "Override the directory for --bundle (default ~/Downloads)")
	return cmd
}

func runDoctor(ctx context.Context, out io.Writer, bundle bool, outputDir string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rep, err := collectDoctorReport(ctx)
	if err != nil {
		return fmt.Errorf("acd doctor: collect: %w", err)
	}

	if bundle {
		bres, err := writeDoctorBundle(ctx, rep, outputDir)
		if err != nil {
			return fmt.Errorf("acd doctor: bundle: %w", err)
		}
		if jsonOut {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(bres)
		}
		fmt.Fprintf(out, "acd doctor: wrote bundle %s (%d files, %d bytes)\n",
			homeShort(bres.Path), bres.FilesCount, bres.SizeBytes)
		return nil
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	return renderDoctorHuman(out, rep)
}

// collectDoctorReport gathers every diagnostic field per §7.10 + §13.3.
func collectDoctorReport(ctx context.Context) (doctorReport, error) {
	rep := doctorReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ACDVersion:  version.String(),
		GoVersion:   runtime.Version(),
		GoOS:        runtime.GOOS,
		GoArch:      runtime.GOARCH,
	}

	if path, err := exec.LookPath("git"); err == nil {
		rep.GitPath = path
		if out, err := exec.CommandContext(ctx, "git", "--version").Output(); err == nil {
			rep.GitVersion = strings.TrimSpace(string(out))
		}
	}

	if out, err := exec.CommandContext(ctx, "uname", "-a").Output(); err == nil {
		rep.Uname = strings.TrimSpace(string(out))
	}

	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil {
		rep.UlimitNoFile = int64(rlim.Cur)
	}

	if runtime.GOOS == "linux" {
		if body, err := os.ReadFile("/proc/sys/fs/inotify/max_user_watches"); err == nil {
			if n, perr := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64); perr == nil {
				rep.InotifyMaxUserWatch = n
			}
		}
	}

	rep.SensitiveGlobsEnv = os.Getenv(state.EnvSensitiveGlobs)
	rep.SensitiveGlobsActive = state.SensitivePatterns()
	rep.SafeIgnoreEnv = os.Getenv(state.EnvSafeIgnore)
	rep.SafeIgnoreExtraEnv = os.Getenv(state.EnvSafeIgnoreExtra)
	rep.SafeIgnoreActive = state.SafeIgnorePatterns()
	rep.Harnesses = collectDoctorHarnesses()

	roots, err := paths.Resolve()
	if err != nil {
		return rep, fmt.Errorf("resolve paths: %w", err)
	}
	target, _ := ctx.Value(doctorTargetContextKey{}).(string)
	if target != "" {
		if wt, resolveErr := gitpkg.ResolveWorktree(ctx, target); resolveErr == nil {
			target = wt.Root
		}
	}
	if target != "" {
		rep.AI = collectDoctorAIForRepo(ctx, roots, target)
	} else {
		rep.AI = collectDoctorAI()
	}
	rep.RegistryPath = roots.RegistryPath()
	reg, err := central.Load(roots)
	if err != nil {
		return rep, fmt.Errorf("load registry: %w", err)
	}
	for _, rec := range reg.Repos {
		if target != "" && filepath.Clean(rec.Path) != filepath.Clean(target) {
			continue
		}
		rr := doctorRepoReport{
			Path:      rec.Path,
			RepoHash:  fallback(rec.RepositoryID, rec.RepoHash),
			StateDB:   rec.StateDB,
			Harnesses: append([]string{}, rec.Harnesses...),
			LogPath:   roots.RepoLogPath(fallback(rec.RepositoryID, rec.RepoHash)),
		}
		rr.DaemonProcessPIDs = findDaemonProcesses(ctx, rec.Path)
		rr.DaemonProcessCount = len(rr.DaemonProcessPIDs)
		if rr.DaemonProcessCount > 1 {
			rr.Notes = append(rr.Notes, fmt.Sprintf("multiple acd daemon processes for repo: %v", rr.DaemonProcessPIDs))
		}
		// Read state.db (best-effort).
		if fileExists(rec.StateDB) {
			rr.StateDBReadable = readRepoState(ctx, &rr, rec.Path, rec.StateDB)
			if status, statusErr := buildStatusReport(ctx, rec, time.Now()); statusErr == nil {
				rr.Busy = status.Busy
				rr.OperationalState = status.OperationalState
				rr.WorktreeClean = status.WorktreeClean
				rr.AllChangesCommittedInGit = status.AllChangesCommittedInGit
				rr.CheckpointPublishedByACD = status.CheckpointPublishedByACD
				rr.PublicationDrain = status.PublicationDrain
			} else {
				rr.Notes = append(rr.Notes, "status truth failed: "+statusErr.Error())
			}
		} else {
			rr.Notes = append(rr.Notes, "state.db missing")
		}
		// Tail the log (best-effort).
		if fileExists(rr.LogPath) {
			rr.LogLines = tailLogLines(rr.LogPath, 100)
		}
		rep.Repos = append(rep.Repos, rr)
	}
	rep.RegistryRepoCount = len(rep.Repos)

	return rep, nil
}

func collectDoctorAIForRepo(ctx context.Context, roots paths.Roots, repo string) doctorAIReport {
	service, err := settings.NewValidationService(ctx, settings.Options{Roots: roots, RepoPath: repo})
	if err != nil {
		report := collectDoctorAI()
		report.Notes = append(report.Notes, "effective repository settings unavailable: "+err.Error())
		return report
	}
	cfg, err := service.AuthoringProviderConfig()
	if err != nil {
		report := collectDoctorAI()
		report.Notes = append(report.Notes, "effective repository provider unavailable: "+err.Error())
		return report
	}
	provider := cfg.Mode
	if provider == "" {
		provider = "deterministic"
	}
	report := doctorAIReport{Provider: provider, APIKeySet: strings.TrimSpace(cfg.APIKey) != ""}
	if provider == "openai-compat" && !report.APIKeySet {
		report.Notes = append(report.Notes, "effective provider requires a credential; checkpoint protection remains active")
	}
	if strings.HasPrefix(provider, "subprocess:") {
		name := strings.TrimSpace(strings.TrimPrefix(provider, "subprocess:"))
		report.ProviderCommand = "acd-provider-" + name
		if path, lookupErr := exec.LookPath(report.ProviderCommand); lookupErr == nil {
			report.ProviderCommandFound = true
			report.ProviderCommandPath = path
		} else {
			report.Notes = append(report.Notes, report.ProviderCommand+" not found on PATH")
		}
	}
	return report
}

func collectDoctorHarnesses() []doctorHarnessReport {
	detected := map[string]bool{}
	matched := map[string]string{}
	for _, h := range adapter.DetectInstalled() {
		detected[h.Name()] = true
		if mp, ok := h.MatchedPath(); ok {
			matched[h.Name()] = mp
		}
	}

	reports := make([]doctorHarnessReport, 0, len(supportedHarnesses))
	for _, name := range supportedHarnesses {
		h, ok := adapter.Lookup(name)
		if !ok {
			continue
		}
		path := h.ConfigPath()
		hr := doctorHarnessReport{
			Name:       name,
			ConfigPath: path,
		}
		// If the marker lives on a candidate path that differs from
		// ConfigPath (e.g., the legacy fallback), surface it so the user
		// learns which file to edit. Doctor scans drift against the file
		// that actually carries the marker.
		matchedPath := matched[name]
		if matchedPath != "" && matchedPath != path {
			hr.MatchedPath = matchedPath
		}
		var matchedBody []byte
		matchedBodyPath := ""
		body, err := os.ReadFile(path)
		switch {
		case err == nil:
			hr.ConfigPresent = true
			hr.ConfigReadable = true
			hr.MarkerFound = adapter.PrimaryPathMatchesMarker(name, body)
			hr.Installed = hr.MarkerFound
			if !hr.Installed && detected[name] {
				hr.Notes = append(hr.Notes, "ACD managed install detected in an alternate config path")
				hr.Installed = true
			}
			if hr.Installed {
				driftBody := body
				if hr.MatchedPath != "" {
					if mb, rerr := os.ReadFile(hr.MatchedPath); rerr == nil {
						driftBody = mb
						matchedBody = mb
						matchedBodyPath = hr.MatchedPath
					}
				}
				if note := scanHookBodyDriftAt(name, driftBody, hr.MatchedPath); note != "" {
					hr.Notes = append(hr.Notes, note)
				}
			}
		case errors.Is(err, os.ErrNotExist):
			if detected[name] {
				hr.Notes = append(hr.Notes, "ACD managed install detected in an alternate config path")
				hr.Installed = true
				if hr.MatchedPath != "" {
					if mb, rerr := os.ReadFile(hr.MatchedPath); rerr == nil {
						matchedBody = mb
						matchedBodyPath = hr.MatchedPath
						if note := scanHookBodyDriftAt(name, mb, hr.MatchedPath); note != "" {
							hr.Notes = append(hr.Notes, note)
						}
					}
				}
			}
		default:
			// Permission-denied / EIO / other read errors must not silently
			// drop the harness from the report. Treat the primary config as
			// "present but unreadable" and fall back to alternate-path
			// detection just like ENOENT does. ConfigReadable + MarkerFound
			// stay false (we genuinely could not read the body), and the
			// error string is surfaced via ConfigReadError so JSON consumers
			// can distinguish this state from "config is readable, marker is
			// just missing".
			hr.ConfigPresent = true
			hr.ConfigReadError = err.Error()
			hr.Notes = append(hr.Notes, "primary-path read failed: "+err.Error()+"; using alternate-path detection")
			if detected[name] {
				hr.Installed = true
				if hr.MatchedPath != "" {
					if mb, rerr := os.ReadFile(hr.MatchedPath); rerr == nil {
						matchedBody = mb
						matchedBodyPath = hr.MatchedPath
						if note := scanHookBodyDriftAt(name, mb, hr.MatchedPath); note != "" {
							hr.Notes = append(hr.Notes, note)
						}
					}
				}
			}
		}

		if name == "codex" {
			legacyPath := path
			legacyBody := body
			legacyReadable := hr.ConfigReadable
			if matchedBodyPath != "" {
				legacyPath = matchedBodyPath
				legacyBody = matchedBody
				legacyReadable = true
			}
			if legacyReadable && adapter.HasLegacyJSONManagedKey(legacyBody) {
				hr.Notes = append(hr.Notes, codexLegacyJSONManagedKeyNote(legacyPath))
			}
			jsonOK, legacyTOMLOK := adapter.CodexInstalls()
			if jsonOK && legacyTOMLOK {
				hr.Notes = append(hr.Notes, "both ~/.codex/hooks.json and a legacy Codex config.toml contain ACD hook installs; Codex merges all hook sources and will fire each event twice (doubled acd start/wake/touch). Remove the # acd-managed: true block from config.toml")
			}
			if note := tailCodexHookLog(); note != "" {
				hr.Notes = append(hr.Notes, note)
			}
		}
		if name == "cursor" && hr.Installed {
			if note := tailCursorHookLog(); note != "" {
				hr.Notes = append(hr.Notes, note)
			}
		}
		reports = append(reports, hr)
	}
	return reports
}

func codexLegacyJSONManagedKeyNote(path string) string {
	if path == "" {
		path = "~/.codex/hooks.json"
	}
	pathArg := path
	if !strings.HasPrefix(pathArg, "~") {
		pathArg = strconv.Quote(pathArg)
	}
	return fmt.Sprintf("legacy top-level _acd_managed in %s is incompatible with Codex 0.141+ hooks schemas because current Codex rejects unknown top-level fields. Regenerate schema-clean hooks with `acd setup codex --raw > %s`; if the file also has custom hooks, remove only the top-level `_acd_managed` key manually and keep the `hooks` object.", path, pathArg)
}

// driftRemediationCommands maps each supported harness to the recommended
// remediation hint shown when an active hook is missing the canonical
// `acd start` + `acd wake` body. The hint shows the merge-first (non-
// destructive) form first, then the full-overwrite form as an alternative so
// users with custom hooks are not surprised by silent config loss.
var driftRemediationCommands = map[string]string{
	"claude-code": "acd setup --integrations=claude-code",
	"codex":       "acd setup --integrations=codex",
	"cursor":      "acd setup --integrations=cursor",
	"opencode":    "acd setup --integrations=opencode",
	"pi":          "acd setup --integrations=pi",
}

// scanHookBodyDrift inspects the installed config body for the named harness
// and returns a non-empty note when one or more lifecycle command bodies are
// missing their canonical command. Active hooks are:
//
//   - claude-code, codex : PreToolUse + PostToolUse entries inside nested JSON
//     "hooks" map (PascalCase event keys)
//   - cursor             : sessionStart/postToolUse/afterFileEdit/stop/sessionEnd
//     flat JSON entries (camelCase keys, top-level `command` per hook)
//   - opencode, pi       : YAML hook items whose `event:` is `tool.before.*`
//     or `tool.after.*`
//
// Returns "" when no drift is detected, when the harness has no
// active-hook concept (shell), or when the config cannot be parsed at all
// (we leave silent rather than scream — drift detection is opportunistic).
func scanHookBodyDrift(name string, body []byte) string {
	return scanHookBodyDriftAt(name, body, "")
}

// scanHookBodyDriftAt is the legacy-aware variant of scanHookBodyDrift. When
// matchedPath is non-empty (the marker lives on a path other than the
// canonical primary), the returned note names the matched file in a merge-
// only remediation so we never recommend a destructive overwrite of a user-
// authored canonical file. When matchedPath is empty, behaves identically to
// the canonical-path remediation.
func scanHookBodyDriftAt(name string, body []byte, matchedPath string) string {
	var stale int
	switch name {
	case "claude-code", "codex":
		stale = countJSONActiveHookDrift(name, body)
	case "cursor":
		stale = countCursorStaleLifecycleCommands(body)
	default:
		bodies := extractActiveHookBodies(name, body)
		if len(bodies) == 0 {
			return ""
		}
		for _, b := range bodies {
			if !activeHookBodyHasStartWake(name, b) {
				stale++
			}
		}
	}
	if stale == 0 {
		return ""
	}
	if matchedPath != "" {
		// Marker lives on a non-canonical (legacy) path. Recommend a merge
		// into the matched file only — never an overwrite that could blow
		// away a user's canonical config they have not migrated yet.
		return fmt.Sprintf("installed snippet drift: %d lifecycle hook(s) missing the canonical command at %s; reinstall via acd setup %s and merge output into %s", stale, matchedPath, name, matchedPath)
	}
	cmd, ok := driftRemediationCommands[name]
	if !ok {
		return fmt.Sprintf("installed snippet drift: %d lifecycle hook(s) missing the canonical command; reinstall via acd setup %s --raw", stale, name)
	}
	return fmt.Sprintf("installed snippet drift: %d lifecycle hook(s) missing the canonical command; reinstall via %s", stale, cmd)
}

// cursorLifecycleSubcommands maps wired Cursor hook events to the acd command
// each inline hook string must invoke.
var cursorLifecycleSubcommands = map[string]string{
	"sessionStart":  "start",
	"postToolUse":   "wake",
	"afterFileEdit": "wake",
	"stop":          "flush",
	"sessionEnd":    "stop",
}

// activeHookBodyHasStartWake reports whether an installed active-hook command
// body still carries the canonical acd start+wake behavior.
func activeHookBodyHasStartWake(harness, body string) bool {
	if strings.Contains(body, "acd start") && strings.Contains(body, "acd wake") {
		return true
	}
	if strings.Contains(body, "acd internal session open") &&
		strings.Contains(body, "acd internal hint --kind wake") {
		return true
	}
	if harness == "cursor" {
		return false
	}
	return false
}

func countJSONActiveHookDrift(name string, body []byte) int {
	byEvent, ok := extractJSONHookCommandsByEvent(body)
	if !ok {
		return 0
	}
	stale := 0
	for _, event := range []string{"PreToolUse", "PostToolUse"} {
		cmds := byEvent[event]
		if len(cmds) == 0 {
			stale++
			continue
		}
		for _, cmd := range cmds {
			if !activeHookBodyHasStartWake(name, cmd) {
				stale++
			}
		}
	}
	if name == "codex" {
		cmds := byEvent["Stop"]
		if len(cmds) == 0 {
			stale++
		}
		for _, cmd := range cmds {
			if !codexStopHasSoftBoundary(cmd) ||
				strings.Contains(cmd, "acd flush --logical") {
				stale++
			}
		}
	}
	return stale
}

var codexTouchInvocation = regexp.MustCompile(
	`(?:^|[;&|({][[:space:]]*)acd[[:space:]]+touch(?:[[:space:]]+[^;&|\n]*)?`,
)

func codexStopHasSoftBoundary(command string) bool {
	if strings.Contains(command, "acd internal hint --kind soft_boundary") {
		return true
	}
	for _, invocation := range codexTouchInvocation.FindAllString(command, -1) {
		for _, field := range strings.Fields(invocation) {
			field = strings.Trim(field, `'"(){}[]`)
			if field == "--soft-boundary" {
				return true
			}
		}
	}
	return false
}

func countCursorStaleLifecycleCommands(body []byte) int {
	byEvent, ok := extractCursorHookCommandsByEvent(body)
	if !ok {
		return 0
	}
	stale := 0
	for event, want := range cursorLifecycleSubcommands {
		cmds := byEvent[event]
		if len(cmds) == 0 {
			stale++
			continue
		}
		for _, cmd := range cmds {
			if !cursorLifecycleCommandOK(want, cmd) {
				stale++
			}
		}
	}
	return stale
}

func cursorLifecycleCommandOK(wantSubcmd, command string) bool {
	switch wantSubcmd {
	case "wake":
		return (strings.Contains(command, "acd start") && strings.Contains(command, "acd wake")) ||
			(strings.Contains(command, "acd internal session open") &&
				strings.Contains(command, "acd internal hint --kind wake"))
	case "start":
		return strings.Contains(command, "acd start") || strings.Contains(command, "acd internal session open")
	case "flush":
		return strings.Contains(command, "acd flush --logical") || strings.Contains(command, "acd internal hint --kind logical_boundary")
	case "stop":
		return strings.Contains(command, "acd stop") || strings.Contains(command, "acd internal session close")
	}
	return false
}

// extractCursorHookCommandsByEvent parses Cursor hooks.json and returns the
// command string(s) for each event key present in the file.
func extractCursorHookCommandsByEvent(body []byte) (map[string][]string, bool) {
	var top struct {
		Hooks map[string][]struct {
			Command string `json:"command"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(body, &top); err != nil || top.Hooks == nil {
		return nil, false
	}
	out := make(map[string][]string)
	for ev, entries := range top.Hooks {
		for _, e := range entries {
			if c := strings.TrimSpace(e.Command); c != "" {
				out[ev] = append(out[ev], c)
			}
		}
	}
	return out, true
}

// extractActiveHookBodies returns the command-string bodies of the active
// hooks for harness `name`. JSON harnesses (claude-code, codex) parse the
// nested "hooks" map and return the "command" string of every entry under
// PreToolUse + PostToolUse. Cursor uses the flat version-1 schema (camelCase
// event keys with a top-level `command` per hook). YAML harnesses (opencode,
// pi) text-scan for hook items whose event matches tool.before.* /
// tool.after.* and concatenate the `bash: |` block bodies.
func extractActiveHookBodies(name string, body []byte) []string {
	switch name {
	case "claude-code", "codex":
		return extractJSONHookBodies(body, []string{"PreToolUse", "PostToolUse"})
	case "cursor":
		return extractCursorFlatHookBodies(body, []string{"postToolUse", "afterFileEdit"})
	case "opencode", "pi":
		return extractYAMLHookBodies(body, []string{"tool.before.", "tool.after."})
	default:
		return nil
	}
}

// extractJSONHookBodies parses a Claude-Code-style settings.json / Codex
// hooks.json and returns the `command` string of every hook entry under any
// of the requested top-level event keys (e.g. PreToolUse, PostToolUse).
//
// Schema (per templates/claude-code/settings.snippet.json and
// templates/codex/hooks.json):
//
//	{
//	  "hooks": {
//	    "<event>": [
//	      { "matcher": "...", "hooks": [ { "command": "..." }, ... ] },
//	      ...
//	    ]
//	  }
//	}
func extractJSONHookBodies(body []byte, events []string) []string {
	byEvent, ok := extractJSONHookCommandsByEvent(body)
	if !ok {
		return nil
	}
	var out []string
	for _, ev := range events {
		out = append(out, byEvent[ev]...)
	}
	return out
}

func extractJSONHookCommandsByEvent(body []byte) (map[string][]string, bool) {
	var top struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, false
	}
	if len(top.Hooks) == 0 {
		return map[string][]string{}, true
	}
	out := make(map[string][]string)
	for ev, entries := range top.Hooks {
		for _, e := range entries {
			for _, h := range e.Hooks {
				if strings.TrimSpace(h.Command) != "" {
					out[ev] = append(out[ev], h.Command)
				}
			}
		}
	}
	return out, true
}

// extractCursorFlatHookBodies parses Cursor hooks.json (version 1 flat schema)
// and returns the `command` string of every hook entry under the requested
// camelCase event keys (e.g. postToolUse, afterFileEdit).
//
// Schema (per templates/cursor/hooks.json):
//
//	{
//	  "version": 1,
//	  "hooks": {
//	    "postToolUse": [ { "command": "...", "timeout": 15 }, ... ],
//	    ...
//	  }
//	}
func extractCursorFlatHookBodies(body []byte, events []string) []string {
	var top struct {
		Hooks map[string][]struct {
			Command string `json:"command"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(body, &top); err != nil {
		return nil
	}
	if len(top.Hooks) == 0 {
		return nil
	}
	var out []string
	for _, ev := range events {
		entries, ok := top.Hooks[ev]
		if !ok {
			continue
		}
		for _, e := range entries {
			if strings.TrimSpace(e.Command) != "" {
				out = append(out, e.Command)
			}
		}
	}
	return out
}

// extractYAMLHookBodies text-scans the OpenCode / Pi hooks.yaml for hook
// items keyed by `event:` matching any of the supplied prefixes, and
// concatenates the body of each `bash: |` block under those items.
//
// We do not pull a YAML parser in for this — the snippet shape is stable
// and indentation-anchored. A real YAML parser would be a heavier dep for
// what amounts to substring inspection of installed-snippet command bodies.
//
// The OpenCode/Pi snippet shape nests action items under each top-level
// hook. We treat the FIRST `- ` encountered as the canonical top-level item
// indent; deeper `- ` lines (nested action lists like
// `actions: - bash: |`) are consumed as content of the current parent
// item, not as new hook items. Without this guard we would allocate
// orphan hookItems for every `- bash: |` under `actions:` and silently
// drop the parent event association — drift detection would never fire
// for a real OpenCode/Pi config.
func extractYAMLHookBodies(body []byte, eventPrefixes []string) []string {
	lines := strings.Split(string(body), "\n")
	type hookItem struct {
		eventLine string
		bashBody  strings.Builder
	}
	var items []hookItem
	var cur *hookItem
	inBash := false
	bashIndent := 0
	itemIndent := -1
	topItemIndent := -1
	for _, raw := range lines {
		stripped := strings.TrimRight(raw, "\r")
		trimmed := strings.TrimSpace(stripped)
		indent := len(stripped) - len(strings.TrimLeft(stripped, " "))
		// Detect a list-item line like "  - id: acd-..." or
		// "    - bash: |". Only treat it as a NEW top-level hook item
		// when its indent matches the canonical top-level indent
		// established by the first `- ` we ever see; deeper dashes are
		// nested action items and must remain part of the current
		// parent's bash body / metadata.
		if strings.HasPrefix(trimmed, "- ") || trimmed == "-" {
			if topItemIndent < 0 {
				topItemIndent = indent
			}
			if indent == topItemIndent {
				items = append(items, hookItem{})
				cur = &items[len(items)-1]
				itemIndent = indent
				inBash = false
				rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
				if strings.HasPrefix(rest, "event:") {
					cur.eventLine = strings.TrimSpace(strings.TrimPrefix(rest, "event:"))
				}
				continue
			}
			// Nested action item (e.g. `- bash: |` under `actions:`).
			// Fall through so the existing bash-block detector below
			// can pick up the literal block opener under the parent.
			if cur == nil {
				continue
			}
			// Strip the leading `- ` so a line like `- bash: |` is
			// handled the same as the bare `bash: |` form below.
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
		}
		if cur == nil {
			continue
		}
		// End the bash block when indentation falls back to or above the
		// item indent.
		if inBash && indent <= bashIndent && trimmed != "" {
			inBash = false
		}
		if inBash {
			// Preserve original line content (without the indent prefix
			// that prefixes the literal block) for `acd start`/`acd wake`
			// substring search; we do not need the exact whitespace.
			cur.bashBody.WriteString(trimmed)
			cur.bashBody.WriteByte('\n')
			continue
		}
		// Capture event: lines that appear as item children.
		if strings.HasPrefix(trimmed, "event:") && itemIndent >= 0 && indent > itemIndent {
			cur.eventLine = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			continue
		}
		// Detect literal `bash: |` block under the current item.
		if strings.HasPrefix(trimmed, "bash:") {
			rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "bash:"))
			if rest == "|" || rest == "|+" || rest == "|-" {
				inBash = true
				bashIndent = indent
				continue
			}
			// Inline `bash: <cmd>` form (rare; we don't emit it but accept it).
			if rest != "" {
				cur.bashBody.WriteString(rest)
				cur.bashBody.WriteByte('\n')
			}
		}
	}
	var out []string
	for _, it := range items {
		ev := it.eventLine
		if ev == "" {
			continue
		}
		matched := false
		for _, p := range eventPrefixes {
			if strings.HasPrefix(ev, p) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		bash := it.bashBody.String()
		if strings.TrimSpace(bash) == "" {
			continue
		}
		out = append(out, bash)
	}
	return out
}

// harnessHookLogPath returns <XDG_STATE_HOME>/acd/<harness>-hook.log.
func harnessHookLogPath(harness string) string {
	if v := os.Getenv("XDG_STATE_HOME"); v != "" && filepath.IsAbs(v) {
		return filepath.Join(v, "acd", harness+"-hook.log")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".local", "state", "acd", harness+"-hook.log")
}

// codexHookLogPath returns the canonical location of codex-hook.log under
// XDG_STATE_HOME (defaulting to $HOME/.local/state). Mirrors the path used
// by templates/codex/hooks.json.
func codexHookLogPath() string {
	return harnessHookLogPath("codex")
}

func cursorHookLogPath() string {
	return harnessHookLogPath("cursor")
}

// codexHookLogRecentWindow controls how far back tailCodexHookLog looks for
// "recent" errors; events outside the window are still reported when they
// fall within the last 50 lines but do not count toward the recent total.
var codexHookLogRecentWindow = 5 * time.Minute

// tailCodexHookLog returns a Note describing recent codex-hook.log entries
// that look like errors (stderr-style), or "" when the file does not exist
// or contains no error-like lines.
func tailCodexHookLog() string {
	return tailHarnessHookLog(codexHookLogPath(), "codex-hook.log")
}

// tailCursorHookLog mirrors tailCodexHookLog for cursor-hook.log.
func tailCursorHookLog() string {
	return tailHarnessHookLog(cursorHookLogPath(), "cursor-hook.log")
}

// tailHarnessHookLog reads the trailing 8 KiB of path and inspects up to the
// last 50 non-empty lines for hook-wrapper failures.
func tailHarnessHookLog(path, displayName string) string {
	if path == "" {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return ""
	}
	const tailBytes = 8 * 1024
	var buf []byte
	if st.Size() > tailBytes {
		if _, err := f.Seek(-tailBytes, io.SeekEnd); err != nil {
			return ""
		}
		buf = make([]byte, tailBytes)
		n, err := io.ReadFull(f, buf)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return ""
		}
		buf = buf[:n]
		// Drop the first (potentially partial) line.
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			buf = buf[i+1:]
		}
	} else {
		buf, err = io.ReadAll(f)
		if err != nil {
			return ""
		}
	}
	lines := strings.Split(string(buf), "\n")
	// Take last 50 non-empty lines.
	const maxLines = 50
	tail := make([]string, 0, maxLines)
	for i := len(lines) - 1; i >= 0 && len(tail) < maxLines; i-- {
		ln := strings.TrimRight(lines[i], "\r")
		if strings.TrimSpace(ln) == "" {
			continue
		}
		tail = append([]string{ln}, tail...)
	}
	if len(tail) == 0 {
		return ""
	}
	cutoff := time.Now().Add(-codexHookLogRecentWindow)
	var firstErr string
	recentCount := 0
	totalErr := 0
	for _, ln := range tail {
		if !looksLikeHookError(ln) {
			continue
		}
		totalErr++
		if firstErr == "" {
			firstErr = ln
		}
		if ts, ok := parseLogTimestamp(ln); ok {
			if ts.After(cutoff) {
				recentCount++
			}
		} else {
			// No parseable timestamp — count it as recent so operators do
			// not miss errors when the log line lacks a timestamp prefix.
			recentCount++
		}
	}
	if totalErr == 0 {
		return ""
	}
	first := firstErr
	if len(first) > 240 {
		first = first[:240] + "…"
	}
	if recentCount > 0 {
		return fmt.Sprintf("%s shows %d recent error(s) within the last %s (first: %s); see %s",
			displayName, recentCount, formatDurationCompact(codexHookLogRecentWindow), first, homeShort(path))
	}
	return fmt.Sprintf("%s shows %d error(s) in the last %d line(s) (first: %s); see %s",
		displayName, totalErr, len(tail), first, homeShort(path))
}

// looksLikeHookError returns true when ln looks like a real failure
// emitted by the codex hook bash wrapper or a structured ACD error log
// line. Two narrow shapes count:
//
//  1. Wrapper printf shape: a leading bracketed timestamp followed by
//     `... failed exit=N` OR a `cmd=acd-` marker. The wrappers in
//     templates/codex/hooks.json (and the OpenCode/Pi YAML snippets) use
//     this exact form. Example:
//     `[2026-05-08T12:34:56+0300] active hook failed exit=1 cmd=acd-start-wake`
//
//  2. JSONL with an explicit failure level: lines that look like JSON
//     with `"level":"error"` or `"level":"fatal"`. Other JSONL fields
//     (e.g. `failed_blocking_pending=0`) do NOT count — they are status
//     fields on info-level lines and were the dominant false positive.
//
// Free-text "error" / "failed" substring matches are intentionally
// dropped: tail noise like `failed_blocking_pending=0` and the
// wrapper's own `active-hook failed` prose used to round-trip through
// the broad matcher and inflate the recent-error count.
func looksLikeHookError(ln string) bool {
	if isWrapperFailureLine(ln) {
		return true
	}
	if level, ok := jsonlLevel(ln); ok {
		switch strings.ToLower(level) {
		case "error", "fatal":
			return true
		}
	}
	return false
}

// isWrapperFailureLine reports whether ln looks like the bash wrapper's
// `printf '[%s] ... failed exit=%d cmd=acd-%s\n' ...` output.
func isWrapperFailureLine(ln string) bool {
	if !strings.HasPrefix(ln, "[") {
		return false
	}
	close := strings.IndexByte(ln, ']')
	if close <= 0 {
		return false
	}
	// Require a parseable timestamp inside the brackets so we do not
	// accidentally flag arbitrary `[anything]` prefixes.
	if _, ok := parseLogTimestamp(ln); !ok {
		return false
	}
	rest := ln[close+1:]
	if strings.Contains(rest, "failed exit=") {
		return true
	}
	if strings.Contains(rest, "cmd=acd-") {
		return true
	}
	return false
}

// jsonlLevel best-effort extracts the value of a `"level":"<x>"` field
// from a JSONL-shaped line. Returns ok=false when the line does not
// look like JSON or when no level field is present.
func jsonlLevel(ln string) (string, bool) {
	trimmed := strings.TrimSpace(ln)
	if !strings.HasPrefix(trimmed, "{") {
		return "", false
	}
	idx := strings.Index(trimmed, `"level":"`)
	if idx < 0 {
		return "", false
	}
	rest := trimmed[idx+len(`"level":"`):]
	end := strings.IndexByte(rest, '"')
	if end <= 0 {
		return "", false
	}
	return rest[:end], true
}

// parseLogTimestamp tries to extract a timestamp from the start of ln. It
// understands the JSONL `"ts":` field used by acd's structured logger,
// the wrapper printf shape `[2026-05-08T12:34:56+0300] message` emitted
// by templates/codex/hooks.json (and the OpenCode/Pi YAML snippets), and
// the bare ISO-8601 prefix "YYYY-MM-DDTHH:MM:SS" common to bash-redirected
// stderr. Returns ok=false when no timestamp is found.
//
// The wrapper printf is built from `date +%FT%T%z` — that is, ISO-8601
// with a numeric timezone offset and NO colon between hh:mm of the zone
// (e.g. `+0300`, not `+03:00`). We must accept both the full bracketed
// form (with zone) and the bare 19-char ISO prefix; otherwise every
// hook-failure line falls through to the no-timestamp branch and is
// counted as recent regardless of when it was written, defeating the
// 5-minute window in tailCodexHookLog.
func parseLogTimestamp(ln string) (time.Time, bool) {
	// JSONL: {"ts":"2026-05-08T...","level":"error",...}
	if idx := strings.Index(ln, `"ts":"`); idx >= 0 {
		rest := ln[idx+len(`"ts":"`):]
		if end := strings.IndexByte(rest, '"'); end > 0 {
			if t, err := time.Parse(time.RFC3339Nano, rest[:end]); err == nil {
				return t, true
			}
			if t, err := time.Parse(time.RFC3339, rest[:end]); err == nil {
				return t, true
			}
		}
	}
	// Strip an optional leading "[" so the wrapper printf shape
	// `[2026-05-08T12:34:56+0300] active hook failed exit=1 ...` parses.
	head := strings.TrimPrefix(ln, "[")
	// Bracketed form with numeric zone (date +%FT%T%z): 24-char prefix.
	if len(head) >= 24 {
		if t, err := time.Parse("2006-01-02T15:04:05-0700", head[:24]); err == nil {
			return t, true
		}
	}
	// Bare ISO-8601 prefix "2026-05-08T12:34:56" (no zone).
	if len(head) >= 19 {
		if t, err := time.Parse("2006-01-02T15:04:05", head[:19]); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func collectDoctorAI() doctorAIReport {
	cfg := ai.LoadProviderConfigFromEnv()
	provider := cfg.Mode
	if provider == "" {
		provider = "deterministic"
	}
	rep := doctorAIReport{
		Provider: provider,
	}
	switch {
	case provider == "openai-compat":
		rep.APIKeySet = strings.TrimSpace(os.Getenv(ai.EnvAPIKey)) != ""
		if !rep.APIKeySet {
			rep.Notes = append(rep.Notes, "ACD_AI_PROVIDER=openai-compat but ACD_AI_API_KEY is not set")
		}
	case strings.HasPrefix(provider, "subprocess:"):
		name := strings.TrimSpace(strings.TrimPrefix(provider, "subprocess:"))
		if name == "" {
			rep.Notes = append(rep.Notes, "ACD_AI_PROVIDER=subprocess: is missing a provider name")
			break
		}
		rep.ProviderCommand = "acd-provider-" + name
		if path, err := exec.LookPath(rep.ProviderCommand); err == nil {
			rep.ProviderCommandFound = true
			rep.ProviderCommandPath = path
		} else {
			rep.Notes = append(rep.Notes, rep.ProviderCommand+" not found on PATH")
		}
	}
	return rep
}

var doctorProcessList = defaultDoctorProcessList
var doctorRepoIdentityResolver = doctorRepoIdentity

type doctorProcess struct {
	PID     int
	Command string
}

func defaultDoctorProcessList(ctx context.Context) ([]doctorProcess, error) {
	psPath := "ps"
	switch runtime.GOOS {
	case "darwin":
		psPath = "/bin/ps"
	case "linux":
		psPath = "/usr/bin/ps"
	}
	out, err := exec.CommandContext(ctx, psPath, "-axo", "pid=,command=").Output()
	if err != nil {
		return nil, err
	}
	var processes []doctorProcess
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		processes = append(processes, doctorProcess{
			PID:     pid,
			Command: strings.TrimSpace(strings.TrimPrefix(line, fields[0])),
		})
	}
	return processes, scanner.Err()
}

func findDaemonProcesses(ctx context.Context, repo string) []int {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	processes, err := doctorProcessList(probeCtx)
	if err != nil {
		return nil
	}
	target, err := doctorRepoIdentityResolver(probeCtx, repo)
	if err != nil {
		return nil
	}
	identities := map[string]string{repo: target}
	var pids []int
	for _, proc := range processes {
		repoArg, ok := daemonRepoArg(proc.Command)
		if !ok {
			continue
		}
		identity, cached := identities[repoArg]
		if !cached {
			identity, err = doctorRepoIdentityResolver(probeCtx, repoArg)
			if err != nil {
				identities[repoArg] = ""
				continue
			}
			identities[repoArg] = identity
		}
		if identity == target {
			pids = append(pids, proc.PID)
		}
	}
	return pids
}

// daemonRepoArg delegates to the daemon's direct production-command parser so
// ownership fencing and diagnostics cannot drift.
func daemonRepoArg(command string) (string, bool) {
	return daemonpkg.ParseDaemonRunRepoArg(command)
}

func doctorRepoIdentity(ctx context.Context, repo string) (string, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return "", err
	}
	commonDir := wt.GitDir
	body, err := os.ReadFile(filepath.Join(wt.GitDir, "commondir"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err == nil {
		value := strings.TrimSpace(string(body))
		if value == "" {
			return "", errors.New("empty Git commondir")
		}
		if filepath.IsAbs(value) {
			commonDir = filepath.Clean(value)
		} else {
			commonDir = filepath.Clean(filepath.Join(wt.GitDir, value))
		}
	}
	if real, err := filepath.EvalSymlinks(commonDir); err == nil {
		commonDir = filepath.Clean(real)
	}
	return commonDir, nil
}

// readRepoState opens the per-repo DB read-only and fills the report fields
// we can derive from daemon_state, daemon_clients and daemon_meta.
func readRepoState(ctx context.Context, rr *doctorRepoReport, repoPath, dbPath string) bool {
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		rr.Notes = append(rr.Notes, "state.db open failed: "+err.Error())
		return false
	}
	defer conn.Close()
	ttl := clientTTLForRepo(repoPath)

	var pid int
	var mode string
	var heartbeatTS sql.NullFloat64
	var branchRef sql.NullString
	var branchGeneration sql.NullInt64
	row := conn.QueryRowContext(ctx, `SELECT pid, mode, heartbeat_ts, branch_ref, branch_generation FROM daemon_state WHERE id = 1`)
	if err := row.Scan(&pid, &mode, &heartbeatTS, &branchRef, &branchGeneration); err != nil && !errors.Is(err, sql.ErrNoRows) {
		rr.Notes = append(rr.Notes, "daemon_state read failed: "+err.Error())
		return true
	} else if err == nil {
		rr.DaemonPID = pid
		rr.DaemonMode = mode
		rr.DaemonAlive = pid > 0 && identity.Alive(pid)
		if heartbeatTS.Valid && heartbeatTS.Float64 > 0 {
			rr.HeartbeatTS = int64(heartbeatTS.Float64)
			age := time.Since(time.Unix(int64(heartbeatTS.Float64), 0))
			rr.HeartbeatAgeS = int64(age.Seconds())
			if age > ttl {
				rr.HeartbeatStale = true
			}
		}
	}
	if n, err := countDoctorClients(ctx, conn); err == nil {
		rr.Clients = n
		if n > 0 {
			if sessionID, err := latestDoctorClientSession(ctx, conn); err == nil {
				rr.FlushSessionID = sessionID
			}
		}
	}

	// fsnotify diagnostics — defensive: missing keys mean "not yet
	// recorded by the fsnotify lane". We do not invent values.
	if v, ok, _ := metaLookup(ctx, conn, "fsnotify.mode"); ok {
		rr.FsnotifyMode = v
	}
	if v, ok, _ := metaLookup(ctx, conn, "fsnotify.watch_count"); ok {
		if n, perr := strconv.Atoi(v); perr == nil {
			rr.FsnotifyWatches = n
		}
	}
	if v, ok, _ := metaLookup(ctx, conn, "fsnotify.dropped_events"); ok {
		if n, perr := strconv.Atoi(v); perr == nil {
			rr.FsnotifyDropped = n
		}
	}
	if v, ok, _ := metaLookup(ctx, conn, "fsnotify.fallback_reason"); ok && v != "" {
		rr.FsnotifyFallbackReason = v
	}
	if v, ok, _ := metaLookup(ctx, conn, "last_capture_error"); ok && v != "" {
		rr.LastCaptureError = v
	}

	// Pending FIFO depth + terminal blocked-conflict count.
	// Best-effort: a missing capture_events table (older schema) yields a
	// note rather than failing the whole doctor run.
	if n, err := countEventsByStateSQL(ctx, conn, state.EventStatePending); err == nil {
		rr.PendingEvents = n
	} else {
		rr.Notes = append(rr.Notes, "pending events count failed: "+err.Error())
	}
	if n, err := countEventsByStateSQL(ctx, conn, state.EventStateBlockedConflict); err == nil {
		rr.BlockedConflicts = n
	} else {
		rr.Notes = append(rr.Notes, "blocked conflicts count failed: "+err.Error())
	}
	if n, err := countEventsByStateSQL(ctx, conn, state.EventStateFailed); err == nil {
		rr.FailedEvents = n
	} else {
		rr.Notes = append(rr.Notes, "failed events count failed: "+err.Error())
	}
	if n, err := countBlockingTerminalEvents(ctx, conn, state.EventStateFailed); err == nil {
		rr.FailedBlockingPending = n
	} else {
		rr.Notes = append(rr.Notes, "failed blocking pending count failed: "+err.Error())
	}
	if intentStrategy, err := loadIntentStrategyReportForPair(
		ctx, conn, branchRef.String, branchGeneration.Int64); err == nil {
		rr.IntentStrategy = intentStrategy
		rr.IntentStrategy.RejectLogPath = plannerRejectLogPath(dbPath)
		rr.Notes = append(rr.Notes, doctorIntentStrategyNotes(rr.IntentStrategy, repoPath, rr.FlushSessionID)...)
		if rr.IntentStrategy.LastPlannerError != "" && rr.IntentStrategy.RejectLogPath != "" {
			rr.Notes = append(rr.Notes,
				"planner rejects for this worktree: "+rr.IntentStrategy.RejectLogPath)
		}
	} else {
		rr.Notes = append(rr.Notes, "intent planner summary failed: "+err.Error())
	}
	if intentV2, err := loadIntentV2Report(ctx, conn); err == nil {
		rr.IntentV2 = intentV2
		if intentV2.NeedsAttention != "" {
			rr.Notes = append(rr.Notes,
				"Intent v2 replay stopped; capture remains durable; run acd config edit")
		}
	} else {
		rr.Notes = append(rr.Notes, "Intent v2 summary failed: "+err.Error())
	}
	if replay, err := loadReplayObservabilityReport(ctx, conn); err == nil {
		rr.Replay = replay
		if replay.State == "needs_attention" {
			rr.Notes = append(rr.Notes,
				"replay is repeatedly failing; inspect acd diagnose")
		}
	} else {
		rr.Notes = append(rr.Notes,
			"replay observability failed: "+err.Error())
	}
	if publication, err := loadSelfPublicationReport(
		ctx, conn, dbPath, time.Now(), rr.DaemonProcessCount,
	); err == nil {
		rr.SelfPublication = publication
		if publication.Remediation != "" {
			rr.Notes = append(rr.Notes, publication.Remediation)
		}
	} else {
		rr.Notes = append(rr.Notes,
			"self-publication observability failed: "+err.Error())
	}
	if readiness, err := loadConfigReadinessReport(ctx, conn, time.Now()); err == nil {
		rr.Configuration = readiness
		if readiness.Configuration == "needs_attention" {
			rr.Notes = append(rr.Notes,
				"configuration validation needs attention; run acd config edit")
		}
	} else {
		rr.Notes = append(rr.Notes,
			"configuration readiness failed: "+err.Error())
	}

	// Most recent terminal blocked_conflict event — gives the operator a
	// concrete path + timestamp to investigate without rummaging the DB.
	if rr.BlockedConflicts > 0 {
		row := conn.QueryRowContext(ctx,
			`SELECT path, published_ts, error FROM capture_events
			 WHERE state = ?
			 ORDER BY seq DESC LIMIT 1`, state.EventStateBlockedConflict)
		var path string
		var ts sql.NullFloat64
		var errMsg sql.NullString
		if err := row.Scan(&path, &ts, &errMsg); err == nil {
			rr.LastReplayConflictPath = path
			if ts.Valid {
				rr.LastReplayConflictTS = int64(ts.Float64)
			}
			if errMsg.Valid {
				rr.LastReplayConflictErr = errMsg.String
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			rr.Notes = append(rr.Notes, "last replay conflict lookup failed: "+err.Error())
		}
	}
	if rr.FailedEvents > 0 {
		row := conn.QueryRowContext(ctx,
			`SELECT path, published_ts, error FROM capture_events
			 WHERE state = ?
			 ORDER BY seq DESC LIMIT 1`, state.EventStateFailed)
		var path string
		var ts sql.NullFloat64
		var errMsg sql.NullString
		if err := row.Scan(&path, &ts, &errMsg); err == nil {
			rr.LastReplayFailurePath = path
			if ts.Valid {
				rr.LastReplayFailureTS = int64(ts.Float64)
			}
			if errMsg.Valid {
				rr.LastReplayFailureErr = errMsg.String
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			rr.Notes = append(rr.Notes, "last replay failure lookup failed: "+err.Error())
		}
	}

	return true
}

func doctorIntentStrategyNotes(r intentStrategyReport, repoPath, sessionID string) []string {
	if !r.BatchWaitActive {
		return nil
	}
	if r.BatchWaitReason == "skipped_due_intent_settle_window" {
		wait := formatDurationCompact(time.Duration(r.SettleTriggerInSeconds) * time.Second)
		sessionHint := sessionID
		if sessionHint == "" {
			sessionHint = "<active-session-id>"
		}
		return []string{
			fmt.Sprintf("intent replay reached min pending and is waiting for the latest capture to stay quiet for %s (about %s remaining)", formatDurationCompact(time.Duration(r.SettleWindowSeconds)*time.Second), wait),
			fmt.Sprintf("to publish now, run acd flush --logical --repo %s --session-id %s; explicit flushes bypass intent batch wait and require a registered active session id", repoPath, sessionHint),
			"to shorten burst collection, lower ACD_INTENT_SETTLE_WINDOW and restart acd",
			"to disable batching, set ACD_COMMIT_STRATEGY=event and restart acd",
		}
	}
	wait := formatDurationCompact(time.Duration(r.AgeTriggerInSeconds) * time.Second)
	need := r.MinPending - r.VisiblePendingEvents
	if need < 0 {
		need = 0
	}
	sessionHint := sessionID
	if sessionHint == "" {
		sessionHint = "<active-session-id>"
	}
	return []string{
		fmt.Sprintf("intent replay is waiting for %d more pending capture(s) or the oldest pending capture to reach %s (about %s remaining)", need, formatDurationCompact(time.Duration(r.MaxPendingAgeSeconds)*time.Second), wait),
		fmt.Sprintf("to publish now, run acd flush --logical --repo %s --session-id %s; explicit flushes bypass intent batch wait and require a registered active session id", repoPath, sessionHint),
		"for sparse repos, lower ACD_INTENT_MIN_PENDING or ACD_INTENT_MAX_PENDING_AGE and restart acd",
		"to disable batching, set ACD_COMMIT_STRATEGY=event and restart acd",
	}
}

func countDoctorClients(ctx context.Context, conn *sql.DB) (int, error) {
	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM daemon_clients`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func latestDoctorClientSession(ctx context.Context, conn *sql.DB) (string, error) {
	var sessionID string
	if err := conn.QueryRowContext(ctx, `SELECT session_id FROM daemon_clients ORDER BY last_seen_ts DESC LIMIT 1`).Scan(&sessionID); err != nil {
		return "", err
	}
	return sessionID, nil
}

func countEventsByStateSQL(ctx context.Context, conn *sql.DB, stateName string) (int, error) {
	var n int
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`, stateName).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// tailLogLines returns the last n lines of path. Best-effort: errors yield
// nil. We read the entire file (logs are bounded by rotation at 10 MB so
// this is cheap).
func tailLogLines(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	lines := make([]string, 0, n)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return lines
}

// renderDoctorHuman writes a human-readable report.
func renderDoctorHuman(out io.Writer, r doctorReport) error {
	fmt.Fprintf(out, "acd doctor: %s\n", r.GeneratedAt)
	fmt.Fprintf(out, "  acd version : %s\n", r.ACDVersion)
	fmt.Fprintf(out, "  go          : %s (%s/%s)\n", r.GoVersion, r.GoOS, r.GoArch)
	if r.GitVersion != "" {
		fmt.Fprintf(out, "  git         : %s @ %s\n", r.GitVersion, r.GitPath)
	} else {
		fmt.Fprintln(out, "  git         : NOT FOUND on PATH")
	}
	if r.Uname != "" {
		fmt.Fprintf(out, "  uname       : %s\n", r.Uname)
	}
	if r.UlimitNoFile > 0 {
		fmt.Fprintf(out, "  ulimit -n   : %d\n", r.UlimitNoFile)
	}
	if r.InotifyMaxUserWatch > 0 {
		fmt.Fprintf(out, "  inotify max : %d\n", r.InotifyMaxUserWatch)
	}
	fmt.Fprintf(out, "\nRegistry (%s)\n", homeShort(r.RegistryPath))
	fmt.Fprintf(out, "  repos: %d\n", r.RegistryRepoCount)

	fmt.Fprintf(out, "\nSensitive globs (env=%q, %d active)\n",
		r.SensitiveGlobsEnv, len(r.SensitiveGlobsActive))
	fmt.Fprintf(out, "Safe-ignore patterns (env=%q extra=%q, %d active)\n",
		r.SafeIgnoreEnv, r.SafeIgnoreExtraEnv, len(r.SafeIgnoreActive))

	fmt.Fprintf(out, "\nInstall\n")
	fmt.Fprintf(out, "  hooks:\n")
	for _, h := range r.Harnesses {
		installed := "no"
		if h.Installed {
			installed = "yes"
		}
		fmt.Fprintf(out, "    %-11s : %s (%s)\n", h.Name, installed, homeShort(h.ConfigPath))
		if h.MatchedPath != "" && h.MatchedPath != h.ConfigPath {
			fmt.Fprintf(out, "                  marker found at: %s\n", homeShort(h.MatchedPath))
		}
		if len(h.Notes) > 0 {
			fmt.Fprintf(out, "                  notes: %s\n", strings.Join(h.Notes, "; "))
		}
	}
	fmt.Fprintf(out, "  ai provider : %s\n", r.AI.Provider)
	if r.AI.Provider == "openai-compat" {
		fmt.Fprintf(out, "                api key set=%v\n", r.AI.APIKeySet)
	}
	if r.AI.ProviderCommand != "" {
		fmt.Fprintf(out, "                command=%s found=%v", r.AI.ProviderCommand, r.AI.ProviderCommandFound)
		if r.AI.ProviderCommandPath != "" {
			fmt.Fprintf(out, " path=%s", r.AI.ProviderCommandPath)
		}
		fmt.Fprintln(out)
	}
	if len(r.AI.Notes) > 0 {
		fmt.Fprintf(out, "                notes: %s\n", strings.Join(r.AI.Notes, "; "))
	}

	fmt.Fprintf(out, "\nRepos (%d):\n", len(r.Repos))
	for _, rr := range r.Repos {
		mode := rr.DaemonMode
		if mode == "" {
			mode = "stopped"
		}
		if rr.HeartbeatStale {
			mode = "stale"
		}
		fmt.Fprintf(out, "  - %s\n", homeShort(rr.Path))
		fmt.Fprintf(out, "      hash       : %s\n", rr.RepoHash)
		fmt.Fprintf(out, "      daemon     : %s (pid %d, alive=%v)\n", mode, rr.DaemonPID, rr.DaemonAlive)
		if rr.DaemonProcessCount > 0 {
			fmt.Fprintf(out, "      processes  : %d %v\n", rr.DaemonProcessCount, rr.DaemonProcessPIDs)
		}
		fmt.Fprintf(out, "      clients    : %d\n", rr.Clients)
		fmt.Fprintf(out, "      pending    : %d\n", rr.PendingEvents)
		if rr.PublicationDrain.ID != "" {
			fmt.Fprintf(out,
				"      drain      : %s phase=%s remaining=%d/%d commits=%d\n",
				rr.PublicationDrain.ID, rr.PublicationDrain.Phase,
				rr.PublicationDrain.RemainingEvents,
				rr.PublicationDrain.TargetEvents,
				rr.PublicationDrain.CommitCount)
		}
		if rr.BlockedConflicts > 0 {
			fmt.Fprintf(out, "      blocked    : %d\n", rr.BlockedConflicts)
			if rr.LastReplayConflictPath != "" {
				bits := []string{rr.LastReplayConflictPath}
				if rr.LastReplayConflictTS > 0 {
					age := time.Since(time.Unix(rr.LastReplayConflictTS, 0))
					bits = append(bits, formatDurationCompact(age)+" ago")
				}
				if rr.LastReplayConflictErr != "" {
					bits = append(bits, fmt.Sprintf("%q", rr.LastReplayConflictErr))
				}
				fmt.Fprintf(out, "      last conflict : %s\n", strings.Join(bits, " "))
			}
		}
		if rr.FailedEvents > 0 {
			fmt.Fprintf(out, "      failed     : %d\n", rr.FailedEvents)
			if rr.FailedBlockingPending > 0 {
				fmt.Fprintf(out, "      failed blockers : %d pending successors; run acd fix --dry-run\n", rr.FailedBlockingPending)
			}
			if rr.LastReplayFailurePath != "" {
				bits := []string{rr.LastReplayFailurePath}
				if rr.LastReplayFailureTS > 0 {
					age := time.Since(time.Unix(rr.LastReplayFailureTS, 0))
					bits = append(bits, formatDurationCompact(age)+" ago")
				}
				if rr.LastReplayFailureErr != "" {
					bits = append(bits, fmt.Sprintf("%q", rr.LastReplayFailureErr))
				}
				fmt.Fprintf(out, "      last failure : %s\n", strings.Join(bits, " "))
			}
		}
		if rr.IntentStrategy.Active || rr.IntentStrategy.DeferredEvents > 0 || rr.IntentStrategy.LastPlannerError != "" {
			fmt.Fprintf(out, "      strategy   : %s active=%v deferred=%d forced_ready=%d\n",
				valueOrUnset(rr.IntentStrategy.Strategy), rr.IntentStrategy.Active, rr.IntentStrategy.DeferredEvents, rr.IntentStrategy.ForcedAgingReady)
			if rr.IntentStrategy.BatchWaitActive {
				if rr.IntentStrategy.BatchWaitReason == "skipped_due_intent_settle_window" {
					fmt.Fprintf(out, "      settle wait: pending=%d newest_age=%s settle=%s trigger_in=%s\n",
						rr.IntentStrategy.VisiblePendingEvents,
						formatDurationCompact(time.Duration(rr.IntentStrategy.NewestPendingAgeSeconds)*time.Second),
						formatDurationCompact(time.Duration(rr.IntentStrategy.SettleWindowSeconds)*time.Second),
						formatDurationCompact(time.Duration(rr.IntentStrategy.SettleTriggerInSeconds)*time.Second))
				} else {
					fmt.Fprintf(out, "      batch wait : pending=%d min_pending=%d oldest_age=%s max_age=%s trigger_in=%s\n",
						rr.IntentStrategy.VisiblePendingEvents,
						rr.IntentStrategy.MinPending,
						formatDurationCompact(time.Duration(rr.IntentStrategy.OldestPendingAgeSeconds)*time.Second),
						formatDurationCompact(time.Duration(rr.IntentStrategy.MaxPendingAgeSeconds)*time.Second),
						formatDurationCompact(time.Duration(rr.IntentStrategy.AgeTriggerInSeconds)*time.Second))
				}
			}
			if rr.IntentStrategy.LastPlannerError != "" {
				fmt.Fprintf(out, "      planner err: seq %d %s\n",
					rr.IntentStrategy.LastPlannerErrorEventSeq, rr.IntentStrategy.LastPlannerError)
			}
		}
		if rr.IntentV2.Available {
			renderIntentV2Human(out, rr.IntentV2)
		}
		if rr.Configuration.Available {
			renderConfigReadinessHuman(out, rr.Configuration)
		}
		renderReplayObservabilityHuman(out, rr.Replay)
		renderSelfPublicationHuman(out, rr.SelfPublication, "      ")
		if rr.FsnotifyMode != "" {
			fmt.Fprintf(out, "      watcher    : mode=%s watches=%d dropped=%d",
				rr.FsnotifyMode, rr.FsnotifyWatches, rr.FsnotifyDropped)
			if rr.FsnotifyFallbackReason != "" {
				fmt.Fprintf(out, " fallback=%s", rr.FsnotifyFallbackReason)
			}
			fmt.Fprintln(out)
		}
		if rr.LastCaptureError != "" {
			fmt.Fprintf(out, "      last error : %s\n", rr.LastCaptureError)
		}
		if len(rr.Notes) > 0 {
			fmt.Fprintf(out, "      notes      : %s\n", strings.Join(rr.Notes, "; "))
		}
	}
	return nil
}

// bundleResult is the JSON payload returned by `acd doctor --bundle --json`.
type bundleResult struct {
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
	FilesCount int    `json:"files_count"`
}

// writeDoctorBundle writes the §13.3 zip layout to outputDir (or
// ~/Downloads when outputDir is empty).
//
// Layout (per §13.3):
//
//	manifest.json
//	acd-version.txt
//	git-version.txt
//	uname.txt
//	ulimit.txt
//	inotify-watches.txt        (linux only)
//	fseventsd.txt              (darwin only)
//	registry.json              (sanitized — home prefix replaced)
//	repos/<repo-hash>/state-schema.txt
//	repos/<repo-hash>/daemon-state.json
//	repos/<repo-hash>/daemon-clients.json
//	repos/<repo-hash>/daemon-meta.json
//	repos/<repo-hash>/daemon-tail.log
//	repos/<repo-hash>/sensitive-globs.txt
//	repos/<repo-hash>/safe-ignore-patterns.txt
//	repos/<repo-hash>/fsnotify-stats.json
func writeDoctorBundle(ctx context.Context, rep doctorReport, outputDir string) (bundleResult, error) {
	if outputDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return bundleResult{}, fmt.Errorf("home dir: %w", err)
		}
		outputDir = filepath.Join(home, "Downloads")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return bundleResult{}, fmt.Errorf("mkdir output: %w", err)
	}
	stamp := time.Now().UTC().Format("20060102-150405")
	zipPath := filepath.Join(outputDir, "acd-doctor-"+stamp+".zip")
	f, err := os.OpenFile(zipPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return bundleResult{}, fmt.Errorf("create zip: %w", err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	files := 0
	add := func(name string, body []byte) error {
		w, err := zw.Create(name)
		if err != nil {
			return err
		}
		if _, err := w.Write(body); err != nil {
			return err
		}
		files++
		return nil
	}

	manifest, err := json.MarshalIndent(sanitizeReport(rep), "", "  ")
	if err != nil {
		return bundleResult{}, fmt.Errorf("marshal manifest: %w", err)
	}
	if err := add("manifest.json", manifest); err != nil {
		return bundleResult{}, fmt.Errorf("write manifest: %w", err)
	}
	if err := add("acd-version.txt", []byte(rep.ACDVersion+"\n")); err != nil {
		return bundleResult{}, err
	}
	if err := add("git-version.txt", []byte(rep.GitVersion+"\n")); err != nil {
		return bundleResult{}, err
	}
	if err := add("uname.txt", []byte(rep.Uname+"\n")); err != nil {
		return bundleResult{}, err
	}
	if err := add("ulimit.txt", []byte(fmt.Sprintf("%d\n", rep.UlimitNoFile))); err != nil {
		return bundleResult{}, err
	}
	if runtime.GOOS == "linux" {
		if err := add("inotify-watches.txt", []byte(fmt.Sprintf("%d\n", rep.InotifyMaxUserWatch))); err != nil {
			return bundleResult{}, err
		}
	}
	if runtime.GOOS == "darwin" {
		// fseventsd.txt is a Mac-only diagnostic; we don't talk to the
		// daemon directly, just record the platform note.
		body := "platform: darwin\nfseventsd is system-managed; no per-tool counters available\n"
		if err := add("fseventsd.txt", []byte(body)); err != nil {
			return bundleResult{}, err
		}
	}

	// registry.json mirrors the report scope so a repo-targeted bundle cannot
	// disclose unrelated registered repository paths.
	roots, err := paths.Resolve()
	if err != nil {
		return bundleResult{}, err
	}
	registry, err := central.Load(roots)
	if err != nil {
		return bundleResult{}, fmt.Errorf("load registry: %w", err)
	}
	included := make(map[string]struct{}, len(rep.Repos))
	for _, repo := range rep.Repos {
		included[filepath.Clean(repo.Path)] = struct{}{}
	}
	filtered := make([]central.RepoRecord, 0, len(registry.Repos))
	for _, repo := range registry.Repos {
		if _, ok := included[filepath.Clean(repo.Path)]; ok {
			filtered = append(filtered, repo)
		}
	}
	registry.Repos = filtered
	regBody, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return bundleResult{}, fmt.Errorf("marshal registry: %w", err)
	}
	regBody = sanitizeBytes(regBody)
	if err := add("registry.json", regBody); err != nil {
		return bundleResult{}, err
	}

	// Per-repo files.
	for _, rr := range rep.Repos {
		base := "repos/" + rr.RepoHash + "/"
		// state-schema.txt + daemon-{state,clients,meta}.json: read the
		// per-repo DB if accessible.
		if fileExists(rr.StateDB) {
			if err := writeRepoBundleFiles(ctx, zw, base, rr.StateDB, &files); err != nil {
				// best-effort: record the failure as a note in a
				// _bundle-error.txt file.
				_ = add(base+"_bundle-error.txt", []byte(err.Error()+"\n"))
			}
		}
		// daemon-tail.log
		if len(rr.LogLines) > 0 {
			body := strings.Join(rr.LogLines, "\n") + "\n"
			body = string(sanitizeBytes([]byte(body)))
			if err := add(base+"daemon-tail.log", []byte(body)); err != nil {
				return bundleResult{}, err
			}
		}
		// sensitive-globs.txt — same active list per repo.
		globs := strings.Join(rep.SensitiveGlobsActive, "\n") + "\n"
		if err := add(base+"sensitive-globs.txt", []byte(globs)); err != nil {
			return bundleResult{}, err
		}
		safeIgnore := strings.Join(rep.SafeIgnoreActive, "\n") + "\n"
		if err := add(base+"safe-ignore-patterns.txt", []byte(safeIgnore)); err != nil {
			return bundleResult{}, err
		}
		// fsnotify-stats.json
		fsstats := map[string]any{
			"mode":            rr.FsnotifyMode,
			"watch_count":     rr.FsnotifyWatches,
			"dropped_events":  rr.FsnotifyDropped,
			"fallback_reason": rr.FsnotifyFallbackReason,
		}
		fb, _ := json.MarshalIndent(fsstats, "", "  ")
		if err := add(base+"fsnotify-stats.json", fb); err != nil {
			return bundleResult{}, err
		}
	}

	if err := zw.Close(); err != nil {
		return bundleResult{}, fmt.Errorf("close zip: %w", err)
	}
	if err := f.Close(); err != nil {
		return bundleResult{}, fmt.Errorf("close file: %w", err)
	}
	st, err := os.Stat(zipPath)
	if err != nil {
		return bundleResult{}, err
	}
	return bundleResult{Path: zipPath, SizeBytes: st.Size(), FilesCount: files}, nil
}

// writeRepoBundleFiles dumps state-schema.txt + daemon-{state,clients,meta}
// from the per-repo state.db into the zip under base.
func writeRepoBundleFiles(ctx context.Context, zw *zip.Writer, base, dbPath string, files *int) error {
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer conn.Close()

	add := func(name string, body []byte) error {
		w, err := zw.Create(name)
		if err != nil {
			return err
		}
		if _, err := w.Write(body); err != nil {
			return err
		}
		*files++
		return nil
	}

	// state-schema.txt
	uv := 0
	_ = conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&uv)
	rows, err := conn.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	tables := []string{}
	if err == nil {
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err == nil {
				tables = append(tables, n)
			}
		}
		rows.Close()
	}
	schema := fmt.Sprintf("user_version=%d\ntables:\n", uv)
	for _, t := range tables {
		schema += "  - " + t + "\n"
	}
	if err := add(base+"state-schema.txt", []byte(schema)); err != nil {
		return err
	}

	// daemon-state.json
	row := conn.QueryRowContext(ctx, `SELECT pid, mode, heartbeat_ts, updated_ts FROM daemon_state WHERE id = 1`)
	var pid int
	var mode string
	var heartbeatTS, updatedTS sql.NullFloat64
	if err := row.Scan(&pid, &mode, &heartbeatTS, &updatedTS); err == nil {
		dsObj := map[string]any{
			"pid":          pid,
			"mode":         mode,
			"heartbeat_ts": nullableFloat64(heartbeatTS),
			"updated_ts":   nullableFloat64(updatedTS),
		}
		body, _ := json.MarshalIndent(dsObj, "", "  ")
		if err := add(base+"daemon-state.json", body); err != nil {
			return err
		}
	}

	// daemon-clients.json
	crows, err := conn.QueryContext(ctx,
		`SELECT session_id, harness, watch_pid, registered_ts, last_seen_ts
		 FROM daemon_clients ORDER BY last_seen_ts DESC`)
	if err == nil {
		entries := []map[string]any{}
		for crows.Next() {
			var sessionID, harness string
			var watchPID sql.NullInt64
			var registeredTS, lastSeenTS float64
			if err := crows.Scan(&sessionID, &harness, &watchPID, &registeredTS, &lastSeenTS); err != nil {
				continue
			}
			entries = append(entries, map[string]any{
				"session_id":    sessionID,
				"harness":       harness,
				"watch_pid":     nullableInt64(watchPID),
				"registered_ts": registeredTS,
				"last_seen_ts":  lastSeenTS,
			})
		}
		crows.Close()
		body, _ := json.MarshalIndent(entries, "", "  ")
		if err := add(base+"daemon-clients.json", body); err != nil {
			return err
		}
	}

	// daemon-meta.json
	mrows, err := conn.QueryContext(ctx, `SELECT key, value FROM daemon_meta ORDER BY key`)
	if err == nil {
		meta := map[string]string{}
		for mrows.Next() {
			var k, v string
			if err := mrows.Scan(&k, &v); err == nil {
				meta[k] = v
			}
		}
		mrows.Close()
		body, _ := json.MarshalIndent(meta, "", "  ")
		if err := add(base+"daemon-meta.json", body); err != nil {
			return err
		}
	}
	return nil
}

func nullableFloat64(v sql.NullFloat64) any {
	if !v.Valid {
		return nil
	}
	return v.Float64
}

func nullableInt64(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

// sanitizeReport replaces $HOME prefixes inside the report's path strings
// with "~" so the bundle is safe to attach to a public issue.
func sanitizeReport(r doctorReport) doctorReport {
	out := r
	out.RegistryPath = homeShort(r.RegistryPath)
	harnesses := make([]doctorHarnessReport, 0, len(r.Harnesses))
	for _, h := range r.Harnesses {
		c := h
		c.ConfigPath = homeShort(h.ConfigPath)
		if h.MatchedPath != "" {
			c.MatchedPath = homeShort(h.MatchedPath)
		}
		harnesses = append(harnesses, c)
	}
	out.Harnesses = harnesses
	repos := make([]doctorRepoReport, 0, len(r.Repos))
	for _, rr := range r.Repos {
		c := rr
		c.Path = homeShort(rr.Path)
		c.StateDB = homeShort(rr.StateDB)
		c.LogPath = homeShort(rr.LogPath)
		// log lines may contain absolute paths — sanitize too.
		if len(rr.LogLines) > 0 {
			lines := make([]string, len(rr.LogLines))
			for i, ln := range rr.LogLines {
				lines[i] = string(sanitizeBytes([]byte(ln)))
			}
			c.LogLines = lines
		}
		if rr.LastCaptureError != "" {
			c.LastCaptureError = string(sanitizeBytes([]byte(rr.LastCaptureError)))
		}
		if rr.Replay.LastError != "" {
			c.Replay.LastError = string(
				sanitizeBytes([]byte(rr.Replay.LastError)))
		}
		repos = append(repos, c)
	}
	out.Repos = repos
	return out
}

// sanitizeBytes replaces $HOME (when set + absolute) with "~" verbatim. We
// only do a literal byte replacement — no smart quoting. Good enough to
// strip the operator's username from a manifest before they share it.
func sanitizeBytes(b []byte) []byte {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return b
	}
	return []byte(strings.ReplaceAll(string(b), home, "~"))
}
