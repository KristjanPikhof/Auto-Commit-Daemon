package git

import (
	"context"
	"strings"
	"testing"
)

func TestFinishWorkerBlobTextClassification(t *testing.T) {
	repo := initRepo(t)
	for _, tc := range []struct {
		name, body string
		text       bool
	}{
		{"empty", "", true},
		{"large_unicode", strings.Repeat("界", 40000), true},
		{"late_nul", strings.Repeat("unchanged\n", 1200) + "\x00tail\n", false},
		{"late_invalid_utf8", strings.Repeat("unchanged\n", 1200) + "\xfftail\n", false},
		{"incomplete_utf8", "valid\xe7\x95", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oid, err := HashObjectStdin(context.Background(), repo, []byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			text, err := FinishBlobIsText(context.Background(), repo, oid)
			if err != nil || text != tc.text {
				t.Fatalf("text=%t want=%t error=%v", text, tc.text, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FinishBlobIsText(ctx, repo, "HEAD"); err == nil {
		t.Fatal("canceled classification succeeded")
	}
}
