package ai

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestHistoryMessagesRejectsInvalidSubprocessVersion(t *testing.T) {
	skipIfWindows(t)
	for _, corrected := range []bool{false, true} {
		name := "rejected"
		if corrected {
			name = "corrected"
		}
		t.Run(name, func(t *testing.T) {
			evidence := finishTestEvidence()
			commits := []HistoryMessageCommit{{ID: "original", Author: "author-a", TreeOID: strings.Repeat("a", 40), OriginalMessage: "Preserve authored settings\n\n- Keep authored values ahead of defaults.", ChangeIDs: []string{evidence.Changes[0].ID}}}
			goals := finishTestOutput(finish.Goal{Purpose: "Preserve authored settings", Boundary: "Keep the original authored scope", Message: "Preserve authored settings\n\n- Keep authored values ahead of defaults.", ChangeIDs: []string{"original"}})
			response := fmt.Sprintf(`{"version":2,"finish_plan":%s}`, goals)
			script := "while IFS= read -r line; do\n printf '%s\\n' '" + response + "'\n done\n"
			if corrected {
				good := fmt.Sprintf(`{"version":1,"finish_plan":%s}`, goals)
				script = "first=1\nwhile IFS= read -r line; do\n if [ \"$first\" = 1 ]; then\n printf '%s\\n' '" + response + "'\n first=0\n else\n printf '%s\\n' '" + good + "'\n fi\n done\n"
			}
			bin := writePluginScript(t, t.TempDir(), "history-version", script)
			provider := NewSubprocessProvider("history-version", SubprocessOptions{LookPath: fixedLookPath("acd-provider-history-version", bin), Stderr: io.Discard, Timeout: 10 * time.Second})
			t.Cleanup(func() { _ = provider.Close() })
			_, result, err := PlanHistoryMessages(t.Context(), provider, evidence, commits, finishTestOptions())
			if result.Calls != 2 {
				t.Fatalf("invalid protocol version bypassed correction: calls=%d error=%v", result.Calls, err)
			}
			if corrected && err != nil {
				t.Fatalf("corrected response rejected: %v", err)
			}
			if !corrected && err == nil {
				t.Fatal("accepted goals from invalid protocol version")
			}
		})
	}
}
