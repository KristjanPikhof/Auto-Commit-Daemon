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
	if runtime.GOOS == "darwin" {
		binary := roots.ManagedBinaryPath()
		if _, err := os.Stat(binary); err != nil {
			return nil, fmt.Errorf("managed ACD binary is unavailable; run `acd setup`: %w", err)
		}
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
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve ACD executable: %w", err)
	}
	digest, err := version.FileDigest(executable)
	if err != nil {
		return nil, fmt.Errorf("digest ACD executable: %w", err)
	}
	compatibility := runtimeCompatibility()
	if status.Version != version.String() || status.BinaryDigest != digest {
		return nil, errors.New("the installed ACD runtime does not match this command; run `acd setup` to review and apply the global upgrade")
	}
	if !status.Compatibility.Equal(compatibility) {
		return nil, errors.New("running ACD does not advertise the current compatibility contract; run `acd setup` once to complete the compatibility cutover")
	}
	return nil, nil
}
