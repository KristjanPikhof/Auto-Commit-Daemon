package supervisor

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const workerLabelPrefix = ServiceLabel + ".worker."

// WorkerRuntimeStatus lets a worker wrapper report protection readiness and
// restart failures without coupling the supervisor to the repository daemon.
type WorkerRuntimeStatus struct {
	RepositoryID string `json:"repository_id"`
	PID          int    `json:"pid,omitempty"`
	State        string `json:"state"`
	Restarts     int    `json:"restarts"`
	LastError    string `json:"last_error,omitempty"`
	UpdatedTS    int64  `json:"updated_ts"`
}

func workerLabel(repositoryID string) (string, error) {
	if !validRepositoryID(repositoryID) {
		return "", fmt.Errorf("supervisor: invalid repository id %q", repositoryID)
	}
	return workerLabelPrefix + repositoryID, nil
}

func workerServicePath(roots paths.Roots, repositoryID string) (string, error) {
	label, err := workerLabel(repositoryID)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(roots.SupervisorSocketPath()), label+".plist"), nil
}

func renderWorkerService(roots paths.Roots, binary, repositoryID string, args []string) ([]byte, error) {
	label, err := workerLabel(repositoryID)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(binary) || !filepath.IsAbs(roots.SupervisorLogPath()) {
		return nil, errors.New("supervisor: worker service paths must be absolute")
	}
	arguments := append([]string{binary}, args...)
	var argumentXML string
	for _, argument := range arguments {
		var escaped strings.Builder
		_ = xml.EscapeText(&escaped, []byte(argument))
		argumentXML += "<string>" + escaped.String() + "</string>"
	}
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>%s</string>
<key>ProgramArguments</key><array>%s</array>
<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
<key>ProcessType</key><string>Interactive</string>
<key>ThrottleInterval</key><integer>1</integer>
<key>StandardOutPath</key><string>%s</string>
<key>StandardErrorPath</key><string>%s</string>
</dict></plist>
`, label, argumentXML, xmlEscape(roots.SupervisorLogPath()), xmlEscape(roots.SupervisorLogPath()))
	return []byte(content), nil
}

func writeWorkerService(roots paths.Roots, repositoryID string, content []byte) (string, error) {
	path, err := workerServicePath(roots, repositoryID)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("supervisor: create worker service directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".worker-service-*")
	if err != nil {
		return "", err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return "", err
	}
	if _, err := temp.Write(content); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return "", err
	}
	return path, nil
}

func validRepositoryID(repositoryID string) bool {
	if len(repositoryID) != 16 {
		return false
	}
	for _, char := range repositoryID {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func WriteWorkerRuntimeStatus(roots paths.Roots, status WorkerRuntimeStatus) error {
	if !validRepositoryID(status.RepositoryID) {
		return fmt.Errorf("supervisor: invalid repository id %q", status.RepositoryID)
	}
	dir := filepath.Dir(roots.WorkerStatusPath(status.RepositoryID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("supervisor: create worker status directory: %w", err)
	}
	status.UpdatedTS = time.Now().UnixMilli()
	temp, err := os.CreateTemp(dir, ".worker-status-*")
	if err != nil {
		return fmt.Errorf("supervisor: create worker status: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("supervisor: secure worker status: %w", err)
	}
	if err := json.NewEncoder(temp).Encode(status); err != nil {
		_ = temp.Close()
		return fmt.Errorf("supervisor: encode worker status: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("supervisor: sync worker status: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("supervisor: close worker status: %w", err)
	}
	if err := os.Rename(tempPath, roots.WorkerStatusPath(status.RepositoryID)); err != nil {
		return fmt.Errorf("supervisor: replace worker status: %w", err)
	}
	return nil
}

func ReadWorkerRuntimeStatus(roots paths.Roots, repositoryID string) (WorkerRuntimeStatus, error) {
	if !validRepositoryID(repositoryID) {
		return WorkerRuntimeStatus{}, fmt.Errorf("supervisor: invalid repository id %q", repositoryID)
	}
	content, err := os.ReadFile(roots.WorkerStatusPath(repositoryID))
	if err != nil {
		return WorkerRuntimeStatus{}, err
	}
	var status WorkerRuntimeStatus
	if err := json.Unmarshal(content, &status); err != nil {
		return WorkerRuntimeStatus{}, fmt.Errorf("supervisor: decode worker status: %w", err)
	}
	if status.RepositoryID != repositoryID {
		return WorkerRuntimeStatus{}, errors.New("supervisor: worker status identity mismatch")
	}
	return status, nil
}

func RemoveWorkerRuntimeStatus(roots paths.Roots, repositoryID string) error {
	if !validRepositoryID(repositoryID) {
		return fmt.Errorf("supervisor: invalid repository id %q", repositoryID)
	}
	err := os.Remove(roots.WorkerStatusPath(repositoryID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
