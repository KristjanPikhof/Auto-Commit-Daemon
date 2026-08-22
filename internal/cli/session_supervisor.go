package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func runtimeCompatibility() supervisor.Compatibility {
	return installer.RuntimeCompatibility()
}

func ensureMutationSupervisor(ctx context.Context, roots paths.Roots) error {
	_, err := ensureMutationSupervisorMode(ctx, roots, false)
	return err
}

func ensureMutationSupervisorMode(ctx context.Context, roots paths.Roots, backgroundUpgrade bool) (func() error, error) {
	_ = backgroundUpgrade
	binary := roots.ManagedBinaryPath()
	info, err := os.Stat(binary)
	if err != nil {
		return nil, fmt.Errorf("managed ACD binary is unavailable; run `acd setup`: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("managed ACD binary is not a regular file; run `acd setup`")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve ACD executable: %w", err)
	}
	sourceDigest, err := version.FileDigest(executable)
	if err != nil {
		return nil, fmt.Errorf("digest ACD executable: %w", err)
	}
	managedDigest, err := version.FileDigest(binary)
	if err != nil {
		return nil, fmt.Errorf("digest managed ACD binary; run `acd setup`: %w", err)
	}
	if managedDigest != sourceDigest {
		return nil, errors.New("the installed ACD runtime does not match this command; run `acd setup` to review and apply the global upgrade")
	}
	if runtime.GOOS == "darwin" {
		if err := supervisor.EnsureSession(ctx, roots, binary, roots.SupervisorLogPath()); err != nil {
			return nil, err
		}
	}
	response, err := (supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 3 * time.Second,
	}).Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "mutation-version-check", Method: "status",
		DeadlineMS: time.Now().Add(3 * time.Second).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, errors.New(response.Error.Message)
	}
	status, err := decodeProductData[supervisor.Status](response.Data)
	if err != nil {
		return nil, err
	}
	compatibility := runtimeCompatibility()
	if status.Version != version.String() || status.BinaryDigest != sourceDigest {
		return nil, errors.New("the installed ACD runtime does not match this command; run `acd setup` to review and apply the global upgrade")
	}
	if !status.Compatibility.Equal(compatibility) {
		return nil, errors.New("running ACD does not advertise the current compatibility contract; run `acd setup` once to complete the compatibility cutover")
	}
	return nil, nil
}
