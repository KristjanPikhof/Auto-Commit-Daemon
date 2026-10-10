package daemon

import (
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

func TestIntentLegacyFallbackSubjectRepartitionsOfferedMember(t *testing.T) {
	t.Parallel()
	testIntentUnassignedReplay(t, "Update files", false)
}

func TestIntentLegacyFallbackSubjectKeepsPurposefulWaitClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, subject, body string
		readiness           ai.IntentCandidateReadiness
		allowed             bool
	}{
		{"empty", "", "", ai.IntentCandidateWait, true},
		{"known_legacy_label", "Update files", "", ai.IntentCandidateWait, true},
		{"purposeful_goal", "Restore speech recognition after pauses", "", ai.IntentCandidateWait, false},
		{"other_generic_label", "Update changes", "", ai.IntentCandidateWait, false},
		{"nonempty_body", "Update files", "- Restore speech recognition after pauses", ai.IntentCandidateWait, false},
		{"ready", "Update files", "", ai.IntentCandidateReady, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assignment := ai.IntentCandidateAssignment{
				Subject: tc.subject, Body: tc.body, Readiness: tc.readiness,
				Purpose:           "retain dependency component until its goal is known",
				MissingCompanions: []string{unclassifiedIntentCompanion}, GroupingReason: "bounded fallback requires planner review",
			}
			if got := isUnclassifiedIntentFallbackAssignment(assignment); got != tc.allowed {
				t.Fatalf("fallback provenance admitted=%t want=%t: %+v", got, tc.allowed, assignment)
			}
		})
	}
}
