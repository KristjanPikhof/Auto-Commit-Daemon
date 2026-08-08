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
)

const serviceAccessLabelPrefix = ServiceLabel + ".access-check."

// ServiceAccessStatus is the durable handshake between the one-shot macOS
// service probe and setup. Writing the target before opening the repository
// lets setup identify the protected folder even when macOS blocks the read.
type ServiceAccessStatus struct {
	State     string `json:"state"`
	Target    string `json:"target,omitempty"`
	Error     string `json:"error,omitempty"`
	UpdatedTS int64  `json:"updated_ts"`
}

func ServiceAccessLabel(operationID string) (string, error) {
	if !validServiceAccessID(operationID) {
		return "", fmt.Errorf("supervisor: invalid access-check operation id %q", operationID)
	}
	return serviceAccessLabelPrefix + operationID, nil
}

func RenderServiceAccessCheck(
	binary, logPath, operationID, resultPath string,
	targets []string,
) (ServiceDefinition, error) {
	label, err := ServiceAccessLabel(operationID)
	if err != nil {
		return ServiceDefinition{}, err
	}
	if !filepath.IsAbs(binary) || !filepath.IsAbs(logPath) || !filepath.IsAbs(resultPath) {
		return ServiceDefinition{}, errors.New("supervisor: access-check service paths must be absolute")
	}
	if len(targets) == 0 {
		return ServiceDefinition{}, errors.New("supervisor: access-check service requires a repository")
	}
	arguments := []string{binary, "internal", "worker", "access-check", "--result", resultPath}
	for _, target := range targets {
		if !filepath.IsAbs(target) {
			return ServiceDefinition{}, fmt.Errorf("supervisor: access-check target must be absolute: %s", target)
		}
		arguments = append(arguments, "--path", target)
	}
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
<key>RunAtLoad</key><true/>
<key>ProcessType</key><string>Interactive</string>
<key>StandardOutPath</key><string>%s</string>
<key>StandardErrorPath</key><string>%s</string>
</dict></plist>
`, label, argumentXML, xmlEscape(logPath), xmlEscape(logPath))
	return ServiceDefinition{Platform: "launchd", Content: []byte(content)}, nil
}

func WriteServiceAccessStatus(path string, status ServiceAccessStatus) error {
	if !filepath.IsAbs(path) {
		return errors.New("supervisor: access-check result path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("supervisor: create access-check result directory: %w", err)
	}
	status.UpdatedTS = time.Now().UnixMilli()
	temp, err := os.CreateTemp(filepath.Dir(path), ".access-check-*")
	if err != nil {
		return fmt.Errorf("supervisor: create access-check result: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if err := json.NewEncoder(temp).Encode(status); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return nil
}

func ReadServiceAccessStatus(path string) (ServiceAccessStatus, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return ServiceAccessStatus{}, err
	}
	var status ServiceAccessStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return ServiceAccessStatus{}, fmt.Errorf("supervisor: decode access-check result: %w", err)
	}
	return status, nil
}

func validServiceAccessID(value string) bool {
	if value == "" || len(value) > 96 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}
