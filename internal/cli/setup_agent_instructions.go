package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const agentInstructionsStart = "<!-- acd:agents:start -->"
const agentInstructionsEnd = "<!-- acd:agents:end -->"

type agentInstructionsPlan struct {
	Path          string `json:"path,omitempty"`
	Target        string `json:"target,omitempty"`
	Status        string `json:"status"`
	Changed       bool   `json:"changed"`
	before, after []byte
	mode          os.FileMode
	existed       bool
}

// Resolve existing symlinks while allowing missing final directories. Retain
// the requested path separately so apply can detect a changed symlink target.
func agentInstructionsTarget(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if info, statErr := os.Lstat(path); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("instruction symlink has no target: %s", path)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return "", err
	}
	resolvedParent, err := agentInstructionsTarget(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(path)), nil
}

func planAgentInstructions(path, guidance string) (agentInstructionsPlan, error) {
	plan := agentInstructionsPlan{Status: "copy only", mode: 0o644}
	if path == "" {
		return plan, nil
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return plan, err
		}
		path = filepath.Join(home, path[2:])
	}
	var err error
	plan.Path, err = filepath.Abs(path)
	if err != nil {
		return plan, err
	}
	plan.Target, err = agentInstructionsTarget(plan.Path)
	if err != nil {
		return plan, err
	}
	info, err := os.Stat(plan.Target)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return plan, err
	}
	if err == nil {
		if !info.Mode().IsRegular() {
			return plan, fmt.Errorf("instruction file is not a regular file: %s", plan.Target)
		}
		plan.existed, plan.mode = true, info.Mode().Perm()
		plan.before, err = os.ReadFile(plan.Target)
		if err != nil {
			return plan, err
		}
	}
	block := agentInstructionsStart + "\n" + strings.TrimSpace(guidance) + "\n" + agentInstructionsEnd
	before := string(plan.before)
	start, end := strings.Index(before, agentInstructionsStart), strings.Index(before, agentInstructionsEnd)
	switch {
	case start >= 0 || end >= 0:
		if start < 0 || end < start || strings.Count(before, agentInstructionsStart) != 1 || strings.Count(before, agentInstructionsEnd) != 1 {
			return plan, errors.New("ACD instruction markers are incomplete or duplicated; review the file manually")
		}
		plan.after = []byte(before[:start] + block + before[end+len(agentInstructionsEnd):])
		plan.Status = "update the existing ACD section; preserve other instructions"
	case strings.Contains(before, "## Commits: ACD"):
		plan.after = plan.before
		plan.Status = "existing ACD guidance found; review the copy manually"
	default:
		separator := ""
		if len(before) > 0 {
			separator = "\n\n"
		}
		plan.after = []byte(before + separator + block + "\n")
		plan.Status = "add the ACD section; preserve other instructions"
	}
	plan.Changed = !bytes.Equal(plan.before, plan.after)
	if !plan.Changed && start >= 0 {
		plan.Status = "ACD instructions are already current"
	}
	return plan, nil
}

func (plan agentInstructionsPlan) revalidate() error {
	if plan.Path == "" {
		return nil
	}
	target, err := agentInstructionsTarget(plan.Path)
	if err != nil || target != plan.Target {
		return fmt.Errorf("instruction path changed after preview: %s", plan.Path)
	}
	info, err := os.Stat(target)
	if errors.Is(err, os.ErrNotExist) && !plan.existed {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("instruction file is not a regular file: %s", target)
	}
	body, err := os.ReadFile(target)
	if err != nil {
		return err
	}
	if !plan.existed || !bytes.Equal(body, plan.before) || info.Mode().Perm() != plan.mode {
		return fmt.Errorf("instruction file changed after preview; rerun the guide: %s", plan.Path)
	}
	return nil
}

func (plan agentInstructionsPlan) apply(roots paths.Roots) (string, error) {
	if !plan.Changed {
		return "", plan.revalidate()
	}
	if err := plan.revalidate(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(plan.Target), 0o755); err != nil {
		return "", err
	}
	lock, err := os.OpenFile(plan.Target+".acd.lock", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("cannot lock instruction file; another setup may be running: %w", err)
	}
	lockInfo, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return "", err
	}
	defer func() {
		_ = lock.Close()
		if info, err := os.Lstat(plan.Target + ".acd.lock"); err == nil && os.SameFile(lockInfo, info) {
			_ = os.Remove(plan.Target + ".acd.lock")
		}
	}()
	var backup string
	if plan.existed {
		dir := filepath.Join(roots.State, "agent-instruction-backups")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
		file, err := os.CreateTemp(dir, "AGENTS-*")
		if err != nil {
			return "", err
		}
		backup = file.Name()
		_, writeErr := file.Write(plan.before)
		syncErr, closeErr := file.Sync(), file.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return backup, err
		}
	}
	temporary, err := os.CreateTemp(filepath.Dir(plan.Target), ".acd-agent-instructions-*")
	if err != nil {
		return backup, err
	}
	defer func() { _ = temporary.Close(); _ = os.Remove(temporary.Name()) }()
	if err := temporary.Chmod(plan.mode); err != nil {
		return backup, err
	}
	_, writeErr := temporary.Write(plan.after)
	syncErr, closeErr := temporary.Sync(), temporary.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return backup, err
	}
	if err := plan.revalidate(); err != nil {
		return backup, err
	}
	if err := os.Rename(temporary.Name(), plan.Target); err != nil {
		return backup, err
	}
	return backup, nil
}
