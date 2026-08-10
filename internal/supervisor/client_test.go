package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClientCancellationInterruptsBlockedResponse(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("acd-client-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		<-time.After(5 * time.Second)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (Client{SocketPath: socket, Timeout: time.Minute}).Do(ctx, Request{
			Version: ProtocolVersion, ID: "cancel", Method: "status",
		})
		done <- err
	}()
	<-accepted
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled client error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client cancellation did not interrupt blocked response")
	}
}
