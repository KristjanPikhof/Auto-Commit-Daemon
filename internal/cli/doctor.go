package cli

import (
	"archive/zip"
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

// doctorRepoReport is the per-repo block inside the doctor report.
type doctorRepoReport struct {
	Path                   string   `json:"path"`
	RepoHash               string   `json:"repo_hash"`
	StateDB                string   `json:"state_db"`
	StateDBReadable        bool     `json:"state_db_readable"`
	DaemonPID              int      `json:"daemon_pid"`
	DaemonAlive            bool     `json:"daemon_alive"`
	DaemonProcessCount     int      `json:"daemon_process_count,omitempty"`
	DaemonProcessPIDs      []int    `json:"daemon_process_pids,omitempty"`
	DaemonMode             string   `json:"daemon_mode"`
	HeartbeatTS            int64    `json:"heartbeat_ts,omitempty"`
	HeartbeatAgeS          int64    `json:"heartbeat_age_seconds,omitempty"`
	HeartbeatStale         bool     `json:"heartbeat_stale"`
	Clients                int      `json:"client_count"`
	Harnesses              []string `json:"harnesses,omitempty"`
	LogPath                string   `json:"log_path"`
	LogLines               []string `json:"log_tail,omitempty"`
	FsnotifyMode           string   `json:"fsnotify_mode,omitempty"`
	FsnotifyWatches        int      `json:"fsnotify_watches,omitempty"`
	FsnotifyDropped        int      `json:"fsnotify_dropped,omitempty"`
	FsnotifyFallbackReason string   `json:"fsnotify_fallback_reason,omitempty"`
	LastCaptureError       string   `json:"last_capture_error,omitempty"`
	PendingEvents          int      `json:"pending_events"`
	BlockedConflicts       int      `json:"blocked_conflicts"`
	LastReplayConflictTS   int64    `json:"last_replay_conflict_ts,omitempty"`
	LastReplayConflictPath string   `json:"last_replay_conflict_path,omitempty"`
	LastReplayConflictErr  string   `json:"last_replay_conflict_error,omitempty"`
	Notes                  []string `json:"notes,omitempty"`
}

type doctorHarnessReport struct {
	Name           string   `json:"name"`
	ConfigPath     string   `json:"config_path"`
	ConfigPresent  bool     `json:"config_present"`
	ConfigReadable bool     `json:"config_readable"`
	MarkerFound    bool     `json:"marker_found"`
	Installed      bool     `json:"installed"`
	Notes          []string `json:"notes,omitempty"`
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
		Short: "Run install + runtime diagnostics; optionally bundle as zip",
		Long: `Run broad install and runtime diagnostics across acd state.

Doctor checks the central registry, daemon liveness, harness installs, AI provider settings, safe-ignore and sensitive-glob configuration, fsnotify status, and recent daemon log tails. Use --bundle to write a zip with sanitized diagnostic files for sharing.

Use acd diagnose for focused replay/branch blockers in the current repo.`,
		Example: `  acd doctor
  acd doctor --json
  acd doctor --bundle
  acd doctor --bundle --output /tmp`,
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
	rep.AI = collectDoctorAI()

	roots, err := paths.Resolve()
	if err != nil {
		return rep, fmt.Errorf("resolve paths: %w", err)
	}
	rep.RegistryPath = roots.RegistryPath()
	reg, err := central.Load(roots)
	if err != nil {
		return rep, fmt.Errorf("load registry: %w", err)
	}
	rep.RegistryRepoCount = len(reg.Repos)

	for _, rec := range reg.Repos {
		rr := doctorRepoReport{
			Path:      rec.Path,
			RepoHash:  rec.RepoHash,
			StateDB:   rec.StateDB,
			Harnesses: append([]string{}, rec.Harnesses...),
			LogPath:   roots.RepoLogPath(rec.RepoHash),
		}
		rr.DaemonProcessPIDs = findDaemonProcesses(ctx, rec.Path)
		rr.DaemonProcessCount = len(rr.DaemonProcessPIDs)
		if rr.DaemonProcessCount > 1 {
			rr.Notes = append(rr.Notes, fmt.Sprintf("multiple acd daemon processes for repo: %v", rr.DaemonProcessPIDs))
		}
		// Read state.db (best-effort).
		if fileExists(rec.StateDB) {
			rr.StateDBReadable = readRepoState(ctx, &rr, rec.Path, rec.StateDB)
		} else {
			rr.Notes = append(rr.Notes, "state.db missing")
		}
		// Tail the log (best-effort).
		if fileExists(rr.LogPath) {
			rr.LogLines = tailLogLines(rr.LogPath, 100)
		}
		rep.Repos = append(rep.Repos, rr)
	}

	return rep, nil
}

func collectDoctorHarnesses() []doctorHarnessReport {
	detected := map[string]bool{}
	for _, h := range adapter.DetectInstalled() {
		detected[h.Name()] = true
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
		body, err := os.ReadFile(path)
		switch {
		case err == nil:
			hr.ConfigPresent = true
			hr.ConfigReadable = true
			hr.MarkerFound = configHasACDMarker(body)
			hr.Installed = hr.MarkerFound
		case errors.Is(err, os.ErrNotExist):
			if detected[name] {
				hr.Notes = append(hr.Notes, "acd-managed marker detected in an alternate config path")
				hr.Installed = true
			}
		default:
			hr.ConfigPresent = true
			hr.Notes = append(hr.Notes, "config read failed: "+err.Error())
		}

		if name == "codex" {
			home, _ := os.UserHomeDir()
			legacyPath := filepath.Join(home, ".codex", "hooks.json")
			if fileExists(legacyPath) {
				hr.Notes = append(hr.Notes, "legacy ~/.codex/hooks.json exists; Codex also loads ~/.codex/config.toml, remove stale hooks.json after installing the toml snippet")
			}
		}
		reports = append(reports, hr)
	}
	return reports
}

func configHasACDMarker(body []byte) bool {
	text := string(body)
	return strings.Contains(text, `"_acd_managed": true`) ||
		strings.Contains(text, `"_acd_managed":true`) ||
		strings.Contains(text, "acd-managed: true")
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

type doctorProcess struct {
	PID     int
	Command string
}

func defaultDoctorProcessList(ctx context.Context) ([]doctorProcess, error) {
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,command=").Output()
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
	processes, err := doctorProcessList(ctx)
	if err != nil {
		return nil
	}
	var pids []int
	for _, proc := range processes {
		cmd := proc.Command
		if strings.Contains(cmd, "acd daemon run") &&
			strings.Contains(cmd, "--repo") &&
			strings.Contains(cmd, repo) {
			pids = append(pids, proc.PID)
		}
	}
	return pids
}

// readRepoState opens the per-repo DB read-only and fills the report fields
// we can derive from daemon_state, daemon_clients and daemon_meta.
func readRepoState(ctx context.Context, rr *doctorRepoReport, repoPath, dbPath string) bool {
	d, err := state.Open(ctx, dbPath)
	if err != nil {
		rr.Notes = append(rr.Notes, "state.db open failed: "+err.Error())
		return false
	}
	defer d.Close()

	st, _, err := state.LoadDaemonState(ctx, d)
	if err != nil {
		rr.Notes = append(rr.Notes, "daemon_state read failed: "+err.Error())
		return true
	}
	rr.DaemonPID = st.PID
	rr.DaemonMode = st.Mode
	rr.DaemonAlive = st.PID > 0 && identity.Alive(st.PID)
	if st.HeartbeatTS > 0 {
		rr.HeartbeatTS = int64(st.HeartbeatTS)
		age := time.Since(time.Unix(int64(st.HeartbeatTS), 0))
		rr.HeartbeatAgeS = int64(age.Seconds())
		if age > clientTTL() {
			rr.HeartbeatStale = true
		}
	}
	if n, err := state.CountClients(ctx, d); err == nil {
		rr.Clients = n
	}

	// fsnotify diagnostics — defensive: missing keys mean "not yet
	// recorded by the fsnotify lane". We do not invent values.
	if v, ok, _ := state.MetaGet(ctx, d, "fsnotify.mode"); ok {
		rr.FsnotifyMode = v
	}
	if v, ok, _ := state.MetaGet(ctx, d, "fsnotify.watch_count"); ok {
		if n, perr := strconv.Atoi(v); perr == nil {
			rr.FsnotifyWatches = n
		}
	}
	if v, ok, _ := state.MetaGet(ctx, d, "fsnotify.dropped_events"); ok {
		if n, perr := strconv.Atoi(v); perr == nil {
			rr.FsnotifyDropped = n
		}
	}
	if v, ok, _ := state.MetaGet(ctx, d, "fsnotify.fallback_reason"); ok && v != "" {
		rr.FsnotifyFallbackReason = v
	}
	if v, ok, _ := state.MetaGet(ctx, d, "last_capture_error"); ok && v != "" {
		rr.LastCaptureError = v
	}
	if head, err := git.RevParse(ctx, repoPath, "HEAD"); err == nil {
		if plan, err := daemon.PlanPublishedLiveIndexRepair(ctx, repoPath, d, head, daemon.DefaultLiveIndexRepairLimit); err == nil {
			if plan.Candidates > 0 || len(plan.Skipped) > 0 {
				rr.Notes = append(rr.Notes, fmt.Sprintf("live-index repair candidates=%d skipped=%d; run acd recover --repo %s --auto --dry-run",
					plan.Candidates, len(plan.Skipped), repoPath))
			}
		}
	}

	// Pending FIFO depth + terminal blocked-conflict count.
	// Best-effort: a missing capture_events table (older schema) yields a
	// note rather than failing the whole doctor run.
	if n, err := state.CountEventsByState(ctx, d, state.EventStatePending); err == nil {
		rr.PendingEvents = n
	} else {
		rr.Notes = append(rr.Notes, "pending events count failed: "+err.Error())
	}
	if n, err := state.CountEventsByState(ctx, d, state.EventStateBlockedConflict); err == nil {
		rr.BlockedConflicts = n
	} else {
		rr.Notes = append(rr.Notes, "blocked conflicts count failed: "+err.Error())
	}

	// Most recent terminal blocked_conflict event — gives the operator a
	// concrete path + timestamp to investigate without rummaging the DB.
	if rr.BlockedConflicts > 0 {
		row := d.SQL().QueryRowContext(ctx,
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

	return true
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
	fmt.Fprintf(out, "acd doctor — %s\n", r.GeneratedAt)
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

	// registry.json — sanitize home dir prefix.
	roots, err := paths.Resolve()
	if err != nil {
		return bundleResult{}, err
	}
	regBody, err := os.ReadFile(roots.RegistryPath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return bundleResult{}, fmt.Errorf("read registry: %w", err)
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
	d, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer d.Close()

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
	uv, _ := d.UserVersion(ctx)
	rows, err := d.SQL().QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
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
	st, _, err := state.LoadDaemonState(ctx, d)
	if err == nil {
		dsObj := map[string]any{
			"pid":          st.PID,
			"mode":         st.Mode,
			"heartbeat_ts": st.HeartbeatTS,
			"updated_ts":   st.UpdatedTS,
		}
		body, _ := json.MarshalIndent(dsObj, "", "  ")
		if err := add(base+"daemon-state.json", body); err != nil {
			return err
		}
	}

	// daemon-clients.json
	clients, err := state.ListClients(ctx, d)
	if err == nil {
		entries := make([]map[string]any, 0, len(clients))
		for _, c := range clients {
			entries = append(entries, map[string]any{
				"session_id":    c.SessionID,
				"harness":       c.Harness,
				"watch_pid":     c.WatchPID.Int64,
				"registered_ts": c.RegisteredTS,
				"last_seen_ts":  c.LastSeenTS,
			})
		}
		body, _ := json.MarshalIndent(entries, "", "  ")
		if err := add(base+"daemon-clients.json", body); err != nil {
			return err
		}
	}

	// daemon-meta.json
	mrows, err := d.SQL().QueryContext(ctx, `SELECT key, value FROM daemon_meta ORDER BY key`)
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

// sanitizeReport replaces $HOME prefixes inside the report's path strings
// with "~" so the bundle is safe to attach to a public issue.
func sanitizeReport(r doctorReport) doctorReport {
	out := r
	out.RegistryPath = homeShort(r.RegistryPath)
	harnesses := make([]doctorHarnessReport, 0, len(r.Harnesses))
	for _, h := range r.Harnesses {
		c := h
		c.ConfigPath = homeShort(h.ConfigPath)
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
