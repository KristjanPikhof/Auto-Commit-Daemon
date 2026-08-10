package supervisor

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const ServiceLabel = "io.github.kristjanpikhof.acd"

type ServiceDefinition struct {
	Platform string `json:"platform"`
	Path     string `json:"path"`
	Content  []byte `json:"-"`
	Binary   string `json:"binary"`
	LogPath  string `json:"log_path"`
}

func RenderService(home, binary, logPath string) (ServiceDefinition, error) {
	if !filepath.IsAbs(home) || !filepath.IsAbs(binary) || !filepath.IsAbs(logPath) {
		return ServiceDefinition{}, fmt.Errorf("supervisor: service paths must be absolute")
	}
	switch runtime.GOOS {
	case "darwin":
		// A launchd process does not inherit the caller's macOS privacy grants.
		// Keep the legacy plist path so setup can back up and remove an older
		// service transactionally, but run the new supervisor as a descendant of
		// the authorized terminal or agent session instead.
		return ServiceDefinition{
			Platform: "session",
			Path:     filepath.Join(home, "Library", "LaunchAgents", ServiceLabel+".plist"),
			Binary:   binary,
			LogPath:  logPath,
		}, nil
	case "linux":
		if !systemdUserManagerAvailable() {
			return ServiceDefinition{}, fmt.Errorf("supervisor: systemd user manager is unavailable")
		}
		content := fmt.Sprintf(`[Unit]
Description=ACD checkpoint supervisor
After=default.target

[Service]
ExecStart=%s internal supervisor run
Restart=always
RestartSec=1
StandardOutput=append:%s
StandardError=append:%s

[Install]
WantedBy=default.target
`, systemdEscape(binary), systemdEscape(logPath), systemdEscape(logPath))
		return ServiceDefinition{Platform: "systemd", Path: filepath.Join(home, ".config", "systemd", "user", "acd-supervisor.service"), Content: []byte(content), Binary: binary, LogPath: logPath}, nil
	default:
		return ServiceDefinition{}, fmt.Errorf("supervisor: unsupported operating system %s", runtime.GOOS)
	}
}

func ValidateService(definition ServiceDefinition, binary string) error {
	if definition.Path == "" || definition.Binary != binary || definition.LogPath == "" {
		return fmt.Errorf("supervisor: incomplete service definition")
	}
	if definition.Platform == "session" {
		if len(definition.Content) != 0 {
			return fmt.Errorf("supervisor: session service must not install a background service file")
		}
		return nil
	}
	if len(definition.Content) == 0 {
		return fmt.Errorf("supervisor: incomplete service definition")
	}
	text := string(definition.Content)
	if !strings.Contains(text, binary) || !strings.Contains(text, "internal") || !strings.Contains(text, "supervisor") {
		return fmt.Errorf("supervisor: service definition does not invoke the managed supervisor")
	}
	if definition.Platform == "launchd" {
		var value any
		if err := xml.Unmarshal(definition.Content, &value); err != nil {
			return fmt.Errorf("supervisor: invalid launchd XML: %w", err)
		}
	}
	return nil
}

func systemdUserManagerAvailable() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	_, err := os.Stat(filepath.Join("/run/user", fmt.Sprintf("%d", os.Getuid()), "systemd", "private"))
	return err == nil
}

func xmlEscape(value string) string {
	var builder strings.Builder
	_ = xml.EscapeText(&builder, []byte(value))
	return builder.String()
}

func systemdEscape(value string) string {
	return strings.ReplaceAll(value, "%", "%%")
}
