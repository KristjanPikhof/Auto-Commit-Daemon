package git

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"unicode/utf8"
)

// FinishBlobIsText proves that an entire frozen blob contains UTF-8 text and
// no NUL. Streaming bounds memory even when a small diff edits a large blob;
// unchanged binary bytes must still prevent textual provider disclosure.
func FinishBlobIsText(ctx context.Context, repo, oid string) (bool, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(childCtx, "git", "cat-file", "blob", oid)
	cmd.Dir = repo
	cmd.Env = scrubEnv(nil)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}
	buffer := make([]byte, 32*1024+utf8.UTFMax-1)
	carry := 0
	text := true
	for {
		n, readErr := stdout.Read(buffer[carry:])
		chunk := buffer[:carry+n]
		if bytes.IndexByte(chunk, 0) >= 0 {
			text = false
			break
		}
		for len(chunk) > 0 {
			if !utf8.FullRune(chunk) {
				break
			}
			r, width := utf8.DecodeRune(chunk)
			if r == utf8.RuneError && width == 1 {
				text = false
				break
			}
			chunk = chunk[width:]
		}
		if !text {
			break
		}
		carry = copy(buffer, chunk)
		if readErr == io.EOF {
			text = carry == 0
			break
		}
		if readErr != nil {
			cancel()
			_ = cmd.Wait()
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, fmt.Errorf("git: read finish blob: %w", readErr)
		}
	}
	if !text {
		cancel()
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if !text {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("git: inspect finish blob: %w", err)
	}
	return true, nil
}
