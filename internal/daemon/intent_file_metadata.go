package daemon

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func attachIntentFileMetadata(ctx context.Context, repo string, captures []IntentCandidateCapture) error {
	seen := make(map[string]bool)
	var ids []string
	for _, capture := range captures {
		for _, op := range capture.Ops {
			for _, oid := range []string{op.BeforeOID.String, op.AfterOID.String} {
				if oid != "" && !seen[oid] {
					seen[oid] = true
					ids = append(ids, oid)
				}
			}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	out, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo, Stdin: strings.NewReader(strings.Join(ids, "\n") + "\n")}, int64(len(ids)*192), "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if err != nil {
		return err
	}
	sizes := make(map[string]int64, len(ids))
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || !seen[fields[0]] || fields[1] != "blob" {
			return fmt.Errorf("invalid captured blob metadata")
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 {
			return fmt.Errorf("invalid captured blob size")
		}
		sizes[fields[0]] = size
	}
	if len(sizes) != len(ids) {
		return fmt.Errorf("incomplete captured blob metadata")
	}
	for i, capture := range captures {
		metadata := &ai.IntentFileMetadata{Kind: "text"}
		for _, op := range capture.Ops {
			metadata.BeforeBytes += sizes[op.BeforeOID.String]
			metadata.AfterBytes += sizes[op.AfterOID.String]
		}
		binary := strings.Contains(capture.CapturedDiff, "Binary files ") || strings.ContainsRune(capture.CapturedDiff, 0) || !utf8.ValidString(capture.CapturedDiff)
		switch strings.ToLower(path.Ext(capture.Event.Path)) {
		case ".bin", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".pdf", ".zip", ".gz", ".mp3", ".mp4", ".sqlite":
			binary = true
		}
		if binary {
			metadata.Kind = "binary"
			metadata.DiffOmittedReason = "binary"
			captures[i].CapturedDiff = ""
		} else if capture.CapturedDiff == "" {
			metadata.Kind = "unknown"
			metadata.DiffOmittedReason = "not_requested_or_unavailable"
		}
		captures[i].FileMetadata = metadata
	}
	return nil
}
