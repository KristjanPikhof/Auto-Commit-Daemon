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
}

func RenderService(home, binary, logPath string) (ServiceDefinition, error) {
	if !filepath.IsAbs(home) || !filepath.IsAbs(binary) || !filepath.IsAbs(logPath) {
		return ServiceDefinition{}, fmt.Errorf("supervisor: service paths must be absolute")
	}
	switch runtime.GOOS {
	case "darwin":
		content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>%s</string>
<key>ProgramArguments</key><array><string>%s</string><string>internal</string><string>supervisor</string><string>run</string></array>
<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>%s</string>
<key>StandardErrorPath</key><string>%s</string>
</dict></plist>
`, ServiceLabel, xmlEscape(binary), xmlEscape(logPath), xmlEscape(logPath))
		return ServiceDefinition{Platform: "launchd", Path: filepath.Join(home, "Library", "LaunchAgents", ServiceLabel+".plist"), Content: []byte(content)}, nil
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
		return ServiceDefinition{Platform: "systemd", Path: filepath.Join(home, ".config", "systemd", "user", "acd-supervisor.service"), Content: []byte(content)}, nil
	default:
		return ServiceDefinition{}, fmt.Errorf("supervisor: unsupported operating system %s", runtime.GOOS)
	}
}

func ValidateService(definition ServiceDefinition, binary string) error {
	if definition.Path == "" || len(definition.Content) == 0 {
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
