package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type integrationRepoState string

const (
	integrationRepoInactiveNoGit        integrationRepoState = "inactive_no_git"
	integrationRepoInactiveUnregistered integrationRepoState = "inactive_unregistered"
	integrationRepoInactiveDisabled     integrationRepoState = "inactive_disabled"
	integrationRepoInactiveUnactivated  integrationRepoState = "inactive_unactivated"
	integrationRepoActive               integrationRepoState = "active"
	integrationRepoIndeterminate        integrationRepoState = "indeterminate"
)

type integrationRepoDecision struct {
	State    integrationRepoState
	Root     string
	Worktree git.Worktree
	Record   central.RepoRecord
	Roots    paths.Roots
	Err      error
}

type integrationEvent struct {
	Harness   string
	Kind      string
	Repo      string
	SessionID string
	WatchPID  int
}

var supportedIntegrationHarnesses = map[string]struct{}{
	"claude-code": {},
	"codex":       {},
	"cursor":      {},
	"opencode":    {},
	"pi":          {},
}

var supportedIntegrationEvents = map[string]struct{}{
	"session_open":     {},
	"activity":         {},
	"soft_boundary":    {},
	"logical_boundary": {},
	"session_close":    {},
}

func newInternalIntegrationEventCmd() *cobra.Command {
	var event integrationEvent
	cmd := &cobra.Command{
		Use:    "event",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			event.Repo, _ = cmd.Flags().GetString("repo")
			return runIntegrationEvent(cmd.Context(), cmd.InOrStdin(), event)
		},
	}
	cmd.Flags().StringVar(&event.Harness, "harness", "", "Integration name")
	cmd.Flags().StringVar(&event.Kind, "event", "", "Normalized integration event")
	cmd.Flags().StringVar(&event.SessionID, "session-id", "", "Optional integration session identity")
	cmd.Flags().IntVar(&event.WatchPID, "watch-pid", 0, "Optional integration process id")
	_ = cmd.MarkFlagRequired("harness")
	_ = cmd.MarkFlagRequired("event")
	return cmd
}

func runIntegrationEvent(ctx context.Context, in io.Reader, event integrationEvent) error {
	return runIntegrationEventWithSender(ctx, in, event, sendInternalHint)
}

type integrationHintSender func(context.Context, string, string, bool, string, string, string, int) error

func runIntegrationEventWithSender(ctx context.Context, in io.Reader, event integrationEvent, send integrationHintSender) error {
	if _, ok := supportedIntegrationHarnesses[event.Harness]; !ok {
		return fmt.Errorf("acd internal integration event: unsupported harness %q", event.Harness)
	}
	if _, ok := supportedIntegrationEvents[event.Kind]; !ok {
		return fmt.Errorf("acd internal integration event: unsupported event %q", event.Kind)
	}

	normalized, err := normalizeIntegrationEvent(in, event)
	if err != nil {
		logIntegrationSessionOpenFailure(event, err)
		return nil
	}

	kind, sessionAction := integrationEventProtocol(normalized.Kind)
	err = send(ctx, normalized.Repo, kind, false, sessionAction,
		normalized.SessionID, normalized.Harness, normalized.WatchPID)
	if err != nil {
		logIntegrationSessionOpenFailure(normalized, err)
	}
	return nil
}

func normalizeIntegrationEvent(in io.Reader, event integrationEvent) (integrationEvent, error) {
	event.Harness = strings.TrimSpace(event.Harness)
	event.Kind = strings.TrimSpace(event.Kind)
	event.Repo = strings.TrimSpace(event.Repo)
	event.SessionID = strings.TrimSpace(event.SessionID)

	needsPayload := event.SessionID == "" ||
		(event.Repo == "" && (event.Harness == "codex" || event.Harness == "cursor"))
	if needsPayload && (event.Harness == "claude-code" || event.Harness == "codex" || event.Harness == "cursor") {
		payload, err := decodeHookStdinPayload(in, "acd internal integration event")
		if err != nil {
			return event, err
		}
		if event.Harness == "cursor" {
			if event.SessionID == "" {
				event.SessionID, err = hookJSONRequiredScalar(payload, "conversation_id", "acd internal integration event")
				if err != nil {
					return event, err
				}
			}
			if event.Repo == "" {
				event.Repo, err = resolveCursorIntegrationRepo(payload)
				if err != nil {
					return event, err
				}
			}
		} else {
			if event.SessionID == "" {
				event.SessionID, err = hookJSONRequiredScalar(payload, "session_id", "acd internal integration event")
				if err != nil {
					return event, err
				}
			}
			if event.Harness == "codex" && event.Repo == "" {
				event.Repo, err = hookJSONOptionalScalar(payload, "cwd", "acd internal integration event")
				if err != nil {
					return event, err
				}
			}
		}
	}

	if event.SessionID == "" && event.Harness == "pi" && event.WatchPID > 0 {
		event.SessionID = "pi-" + strconv.Itoa(event.WatchPID)
	}
	if event.SessionID == "" {
		return event, errors.New("acd internal integration event: session identity is required")
	}
	if event.Repo == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return event, fmt.Errorf("acd internal integration event: get working directory: %w", err)
		}
		event.Repo = cwd
	}
	return event, nil
}

func resolveCursorIntegrationRepo(payload map[string]any) (string, error) {
	if roots, ok := payload["workspace_roots"].([]any); ok {
		for _, item := range roots {
			root, ok := item.(string)
			if !ok || strings.TrimSpace(root) == "" {
				continue
			}
			candidate, err := nearestGitMarkerRoot(root)
			if err == nil && candidate != "" {
				return candidate, nil
			}
		}
	}
	if cwd, err := hookJSONOptionalScalar(payload, "cwd", "acd internal integration event"); err != nil {
		return "", err
	} else if cwd != "" {
		return canonicalHookPath(cwd), nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("acd internal integration event: get working directory: %w", err)
	}
	return canonicalHookPath(wd), nil
}

func integrationEventProtocol(event string) (kind, sessionAction string) {
	switch event {
	case "session_open", "activity":
		return "wake", "open"
	case "soft_boundary":
		return "soft_boundary", ""
	case "logical_boundary":
		return "logical_boundary", ""
	case "session_close":
		return "wake", "close"
	default:
		return "", ""
	}
}

func evaluateIntegrationRepo(ctx context.Context, repo string) integrationRepoDecision {
	root, err := nearestGitMarkerRoot(repo)
	if err != nil {
		return integrationRepoDecision{State: integrationRepoIndeterminate, Err: err}
	}
	if root == "" {
		return integrationRepoDecision{State: integrationRepoInactiveNoGit}
	}

	roots, err := paths.Resolve()
	if err != nil {
		return integrationRepoDecision{State: integrationRepoIndeterminate, Root: root, Err: err}
	}
	reg, err := central.Load(roots)
	if err != nil {
		return integrationRepoDecision{State: integrationRepoIndeterminate, Root: root, Roots: roots, Err: err}
	}
	record, ok := reg.FindRepo(root)
	if !ok {
		return integrationRepoDecision{State: integrationRepoInactiveUnregistered, Root: root, Roots: roots}
	}
	if record.LifecycleDisabled() {
		return integrationRepoDecision{State: integrationRepoInactiveDisabled, Root: root, Record: record, Roots: roots}
	}
	if !registryRecordHasCanonicalIdentity(record) {
		return integrationRepoDecision{State: integrationRepoInactiveUnactivated, Root: root, Record: record, Roots: roots}
	}

	worktree, err := git.ResolveWorktree(ctx, root)
	if err != nil {
		return integrationRepoDecision{State: integrationRepoIndeterminate, Root: root, Record: record, Roots: roots, Err: err}
	}
	if !registryRecordActivatedForWorktree(record, worktree) {
		return integrationRepoDecision{
			State: integrationRepoIndeterminate, Root: root, Worktree: worktree,
			Record: record, Roots: roots,
			Err: errors.New("acd internal integration event: registered repository identity does not match the worktree"),
		}
	}
	return integrationRepoDecision{
		State: integrationRepoActive, Root: root, Worktree: worktree,
		Record: record, Roots: roots,
	}
}

func nearestGitMarkerRoot(repo string) (string, error) {
	if strings.TrimSpace(repo) == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("acd integration gate: get working directory: %w", err)
		}
		repo = cwd
	}
	abs, err := filepath.Abs(repo)
	if err != nil {
		return "", fmt.Errorf("acd integration gate: resolve %q: %w", repo, err)
	}
	abs = filepath.Clean(abs)
	if real, resolveErr := filepath.EvalSymlinks(abs); resolveErr == nil {
		abs = filepath.Clean(real)
	}
	info, err := os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("acd integration gate: inspect %s: %w", abs, err)
	}
	if !info.IsDir() {
		return "", nil
	}

	for dir := abs; ; dir = filepath.Dir(dir) {
		_, err := os.Lstat(filepath.Join(dir, ".git"))
		if err == nil {
			return dir, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("acd integration gate: inspect %s: %w", filepath.Join(dir, ".git"), err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", nil
		}
	}
}

func logIntegrationSessionOpenFailure(event integrationEvent, cause error) {
	if event.Kind != "session_open" || cause == nil {
		return
	}
	path := harnessHookLogPath(event.Harness)
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = fmt.Fprintf(file, "[%s] integration event failed harness=%s event=%s repo=%q cause=%q\n",
		time.Now().Format(time.RFC3339), event.Harness, event.Kind, event.Repo, cause.Error())
}
