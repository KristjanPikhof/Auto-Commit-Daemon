//go:build !darwin

package supervisor

import "net"

func validatePeerUser(net.Conn) error { return nil }
