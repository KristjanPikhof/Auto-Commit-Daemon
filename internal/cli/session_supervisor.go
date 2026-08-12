package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func ensureMutationSupervisor(ctx context.Context, roots paths.Roots) error {
	if runtime.GOOS == "darwin" {
		binary := roots.ManagedBinaryPath()
		if _, err := os.Stat(binary); err != nil {
			return fmt.Errorf("managed ACD binary is unavailable; run `acd setup`: %w", err)
		}
		if err := supervisor.EnsureSession(ctx, roots, binary, roots.SupervisorLogPath()); err != nil {
			return err
		}
	}
	response, err := (supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 3 * time.Second,
	}).Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "mutation-version-check", Method: "status",
		DeadlineMS: time.Now().Add(3 * time.Second).UnixMilli(),
	})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errors.New(response.Error.Message)
	}
	status, err := decodeProductData[supervisor.Status](response.Data)
	if err != nil {
		return err
	}
	if status.Version != "" && status.Version != version.String() {
		return fmt.Errorf("CLI version %s does not match supervisor version %s; run `acd setup`",
			version.String(), status.Version)
	}
	return nil
}
