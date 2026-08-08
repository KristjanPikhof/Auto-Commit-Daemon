package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

func WorkerSocketPath(roots interface{ SupervisorSocketPath() string }, repositoryID string) string {
	return filepath.Join(filepath.Dir(roots.SupervisorSocketPath()), "worker-"+repositoryID+".sock")
}

type WorkerHandler interface {
	HandleWorkerRequest(context.Context, Request) (any, *ProtocolError)
}

func ServeWorker(ctx context.Context, socketPath string, handler WorkerHandler) error {
	if handler == nil {
		return errors.New("supervisor: nil worker handler")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return err
	}
	if err := removeStaleSocket(socketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return err
	}
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveWorkerConnection(ctx, conn, handler)
	}
}

func serveWorkerConnection(parent context.Context, conn net.Conn, handler WorkerHandler) {
	defer conn.Close()
	var request Request
	if err := json.NewDecoder(bufio.NewReader(io.LimitReader(conn, 1<<20))).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(Response{Version: ProtocolVersion, Error: &ProtocolError{Code: "invalid_request", Message: "invalid JSON request"}})
		return
	}
	response := Response{Version: ProtocolVersion, ID: request.ID}
	if err := request.Validate(); err != nil {
		response.Error = &ProtocolError{Code: "invalid_request", Message: err.Error()}
		_ = json.NewEncoder(conn).Encode(response)
		return
	}
	ctx := parent
	var cancel context.CancelFunc
	if request.DeadlineMS > 0 {
		ctx, cancel = context.WithDeadline(parent, time.UnixMilli(request.DeadlineMS))
	} else {
		ctx, cancel = context.WithTimeout(parent, 30*time.Second)
	}
	defer cancel()
	response.Data, response.Error = handler.HandleWorkerRequest(ctx, request)
	response.OK = response.Error == nil
	_ = json.NewEncoder(conn).Encode(response)
}

func DoWorker(ctx context.Context, socketPath string, request Request, timeout time.Duration) (Response, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := Client{SocketPath: socketPath, Timeout: timeout}
	response, err := client.Do(ctx, request)
	if err != nil {
		return Response{}, fmt.Errorf("worker unavailable: %w", err)
	}
	return response, nil
}
