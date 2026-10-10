package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type clientWorkerHandlerFunc func(context.Context, Request) (any, *ProtocolError)

func (f clientWorkerHandlerFunc) HandleWorkerRequest(ctx context.Context, request Request) (any, *ProtocolError) {
	return f(ctx, request)
}

func clientTestWorkerSocket(t *testing.T, handler clientWorkerHandlerFunc) string {
	t.Helper()
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("acd-client-%d.sock", time.Now().UnixNano()))
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			serveWorkerConnection(ctx, conn, handler)
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		_ = os.Remove(socket)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("client fixture worker did not stop")
		}
	})
	return socket
}

func TestClientCallerDeadlineReturnsWorkerTimeoutDiagnostic(t *testing.T) {
	t.Parallel()
	seen := make(chan int64, 1)
	socket := clientTestWorkerSocket(t, func(ctx context.Context, request Request) (any, *ProtocolError) {
		seen <- request.DeadlineMS
		deadline, _ := ctx.Deadline()
		// Checkpoint barriers leave time to transmit their final proof/error.
		timer := time.NewTimer(time.Until(deadline) - 100*time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		return nil, &ProtocolError{Code: "checkpoint_timeout", Message: "checkpoint barrier timed out (accepted_epoch=9 covered_epoch=8)", Retryable: true}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	response, err := (Client{SocketPath: socket, Timeout: CheckpointBarrierTimeout}).Do(ctx, Request{
		Version: ProtocolVersion, ID: "maintenance-checkpoint", Method: "checkpoint_barrier",
		DeadlineMS: time.Now().Add(CheckpointBarrierTimeout).UnixMilli(),
	})
	if err != nil || response.Error == nil || response.Error.Code != "checkpoint_timeout" || !strings.Contains(response.Error.Message, "accepted_epoch=9 covered_epoch=8") || !response.Error.Retryable {
		t.Fatalf("caller lost worker timeout proof: response=%+v err=%v", response, err)
	}
	if got := <-seen; got != callerDeadline.UnixMilli() {
		t.Fatalf("worker kept abandoned long deadline: got=%d caller=%d", got, callerDeadline.UnixMilli())
	}
}

func TestClientPreservesEarlierAndDefaultProtocolDeadlines(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, method string
		callerLimit  bool
		requested    time.Duration
	}{
		{"earlier_explicit", "checkpoint_barrier", true, time.Second},
		{"default_read_only", "status", false, 0},
		{"explicit_without_caller_limit", "checkpoint_barrier", false, CheckpointBarrierTimeout},
		{"caller_limited_read_only", "status", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan Request, 1)
			socket := clientTestWorkerSocket(t, func(ctx context.Context, request Request) (any, *ProtocolError) {
				seen <- request
				return map[string]bool{"read_only": true}, nil
			})
			ctx := context.Background()
			if tc.callerLimit {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
			}
			request := Request{Version: ProtocolVersion, ID: "deadline-contract", Method: tc.method}
			if tc.requested > 0 {
				request.DeadlineMS = time.Now().Add(tc.requested).UnixMilli()
			}
			want := request.DeadlineMS
			response, err := (Client{SocketPath: socket}).Do(ctx, request)
			if err != nil || !response.OK || response.Error != nil {
				t.Fatalf("protocol result changed: response=%+v err=%v", response, err)
			}
			if received := <-seen; received.DeadlineMS != want || received.Method != request.Method || received.ID != request.ID {
				t.Fatalf("deadline contract changed: got=%+v want_deadline=%d", received, want)
			}
		})
	}
}

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
