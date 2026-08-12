//go:build darwin

package supervisor

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func validatePeerUser(conn net.Conn) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("supervisor: expected Unix socket peer")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return fmt.Errorf("supervisor: inspect peer socket: %w", err)
	}
	var (
		cred    *unix.Xucred
		credErr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return fmt.Errorf("supervisor: inspect peer credentials: %w", err)
	}
	if credErr != nil {
		return fmt.Errorf("supervisor: read peer credentials: %w", credErr)
	}
	if cred == nil || cred.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("supervisor: reject peer uid")
	}
	return nil
}
