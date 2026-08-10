package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type Client struct {
	SocketPath string
	Timeout    time.Duration
}

func (c Client) Do(ctx context.Context, request Request) (Response, error) {
	if request.Version == 0 {
		request.Version = ProtocolVersion
	}
	if err := request.Validate(); err != nil {
		return Response{}, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return Response{}, fmt.Errorf("supervisor unavailable: %w", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if request.DeadlineMS > 0 {
		requested := time.UnixMilli(request.DeadlineMS)
		if requested.Before(deadline) {
			deadline = requested
		}
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return Response{}, err
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer stopCancellation()
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, fmt.Errorf("send supervisor request: %w", err)
	}
	var response Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&response); err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, fmt.Errorf("read supervisor response: %w", err)
	}
	if response.Version != ProtocolVersion || response.ID != request.ID {
		return Response{}, fmt.Errorf("invalid supervisor response identity")
	}
	return response, nil
}
