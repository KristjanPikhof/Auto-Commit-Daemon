package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestLegacyAttentionBoundsHumanEvidenceWithoutHidingRecovery(t *testing.T) {
	reason := "Legacy available versions are retained.\nlegacy objects missing\nlegacy blob abc is unavailable\n" + strings.Repeat("legacy object unavailable\n", 190)
	legacy := state.FinishLegacyCutover{State: "needs_attention", ReasonCode: "legacy_object_missing", Reason: reason}
	commandErr := actionRequiredError("finish_needs_attention", reason)
	if len(commandErr.Error()) > 1024 || !strings.Contains(commandErr.Error(), "legacy blob abc") || ExitCode(commandErr) != ExitActionRequired {
		t.Fatalf("command flooded output or hid its required action: %v", commandErr)
	}
	report := statusReport{Daemon: "running", PID: os.Getpid(), Protected: true, PublicationOutcome: publicationOutcome{Legacy: legacy, PendingChanges: 1, CountsKnown: true}}
	var result controlResult
	applyFinishControlStatus(&result, report, true)
	if result.NextAction != "Run `acd finish` when this work is complete." {
		t.Fatalf("archived evidence displaced current-work action: %s", result.NextAction)
	}
	var shared, product bytes.Buffer
	renderFinishOutcomeHuman(&shared, report.PublicationOutcome)
	if err := renderProductEnvelope(&product, envelopeFromControl(result), false); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"summary": result.Summary, "shared": shared.String(), "product": product.String()} {
		if len(body) > 1500 || !strings.Contains(body, "legacy blob abc") || !strings.Contains(body, "acd support diagnose --json") {
			t.Fatalf("%s lost useful evidence or flooded output: %q", name, body)
		}
	}
	raw, err := json.Marshal(report.PublicationOutcome)
	if err != nil {
		t.Fatal(err)
	}
	var decoded publicationOutcome
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Legacy.Reason != reason {
		t.Fatal("human formatting changed full JSON evidence")
	}
}

func TestUnreadableLegacyArchiveDoesNotHideCurrentProtection(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "current.txt", "protected current work")
	finishStatusCheckpoint(t, rec, db)
	if _, err := db.SQL().ExecContext(t.Context(), `INSERT INTO finish_legacy_cutover(id,worktree_id,manifest_json,manifest_digest,state,created_ts,updated_ts) VALUES(1,?,'{','invalid','completed',1,1)`, rec.WorktreeID); err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if !report.Protected || report.PublicationOutcome.PendingChanges != 1 || report.PublicationOutcome.Legacy.ReasonCode != "legacy_archive_unreadable" {
		t.Fatalf("archived corruption hid current protection: %+v", report)
	}
	finishStatusAssertViews(t, rec, db, report)
	var raw string
	if err := db.ReadSQL().QueryRowContext(t.Context(), "SELECT manifest_json FROM finish_legacy_cutover").Scan(&raw); err != nil || raw != "{" {
		t.Fatalf("status modified archived evidence: %q error=%v", raw, err)
	}
}
