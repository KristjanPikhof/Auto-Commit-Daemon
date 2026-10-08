package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestProviderRetryStatusExplainsCaptureAndDueRetry(t *testing.T) {
	for _, tc := range []struct {
		name          string
		phase         string
		remaining     int64
		responsive    bool
		wantLabel     string
		wantListPhase string
		wantStatus    string
	}{
		{"cooldown", "provider_wait", 300, true, "waiting for the Intent provider retry (5m remaining); file capture continues", "provider-wait:5m", "waiting"},
		{"due", "provider_wait", 0, true, "AI provider retry is due; file capture continues", "provider-retry-due", "waiting"},
		{"worker unavailable", "provider_wait", 0, false, "AI provider retry is due", "provider-retry-due", "waiting"},
		{"provider active", "provider_call", 0, true, "waiting for the current Intent provider response", "provider-call", "working"},
		{"history reconstruction", "history_reconstruction", 0, true, "reconstructing verified goals on a new branch; file capture continues", "history-reconstruct", "working"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			progress := publicationProgressReport{
				Phase: tc.phase, WaitRemainingSeconds: tc.remaining,
				WorkerResponsive: tc.responsive,
			}
			committed := false
			var out bytes.Buffer
			err := renderProductEnvelope(&out, productEnvelope{
				State: productStateWaiting,
				Data: productStatusData{
					Enabled: true, Protected: true,
					PublicationOutcome:  publicationOutcome{BranchCommitted: &committed, WaitingChanges: 1, ReasonCode: tc.phase},
					PublicationProgress: progress,
				},
			}, false)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "Status: "+tc.wantLabel+"\n") ||
				!strings.Contains(out.String(), "Current changes saved: yes") {
				t.Fatalf("status hid provider retry/protection: %s", out.String())
			}
			if !tc.responsive && strings.Contains(out.String(), "file capture continues") {
				t.Fatalf("unavailable worker claimed ongoing capture: %s", out.String())
			}
			entry := productListEntry{PublicationProgress: progress}
			if phase, status := productListPhase(entry), productListStatus(entry); phase != tc.wantListPhase || status != tc.wantStatus {
				t.Fatalf("list phase/status=%s/%s want=%s/%s", phase, status, tc.wantListPhase, tc.wantStatus)
			}
		})
	}
}
