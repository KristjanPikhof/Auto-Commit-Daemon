package daemon

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Reuse the exact recorded named-import proof, including the .js/.ts ambiguity
// check. Published tests stay read-only and still need the shared HEAD proof.
func discoverIntentPublishedTypeScriptRegressions(ctx context.Context, db *state.DB, input IntentCandidateEvaluation) ([]state.IntentCandidate, map[string]map[string]bool, error) {
	var sources []intentTypeScriptRecordedFile
	var captures []IntentCandidateCapture
	remaining := intentTypeScriptReferenceTotalCap
	load := func(capture IntentCandidateCapture) (intentTypeScriptRecordedFile, bool, error) {
		if len(capture.Ops) != 1 || path.Ext(capture.Event.Path) != ".ts" || remaining <= 0 {
			return intentTypeScriptRecordedFile{}, false, nil
		}
		op := capture.Ops[0]
		if op.Path != capture.Event.Path || !op.AfterOID.Valid || op.AfterMode.String != git.RegularFileMode || op.Op == "rename" {
			return intentTypeScriptRecordedFile{}, false, nil
		}
		limit := min(remaining, intentSourceReferenceScanCap)
		contents, err := git.CatFileBlobLimited(ctx, input.RepoPath, op.AfterOID.String, int64(limit))
		if errors.Is(err, git.ErrStdoutOverflow) {
			remaining -= limit
			return intentTypeScriptRecordedFile{}, false, nil
		}
		if err != nil {
			return intentTypeScriptRecordedFile{}, false, err
		}
		remaining -= len(contents)
		return newIntentTypeScriptRecordedFile(capture.Event.Seq, capture.Event.Path, string(contents), capture.Event.BaseHead), true, nil
	}
	for _, capture := range input.Captures {
		if strings.HasSuffix(capture.Event.Path, ".test.ts") || strings.HasSuffix(capture.Event.Path, ".spec.ts") || len(sources) >= state.IntentCandidateMaxCaptures-1 {
			continue
		}
		file, found, err := load(capture)
		if err != nil {
			return nil, nil, err
		}
		if found {
			sources = append(sources, file)
			captures = append(captures, capture)
		}
	}
	if len(sources) == 0 {
		return nil, nil, nil
	}
	if input.LatestCommit == nil || input.LatestCommit.OID == "" {
		return nil, nil, nil
	}
	head, err := git.RevParse(ctx, input.RepoPath, input.LatestCommit.OID+"^{commit}")
	if errors.Is(err, git.ErrRefNotFound) || errors.Is(err, git.ErrRefAmbiguous) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT candidate.id,event.seq
FROM capture_events event
JOIN intent_candidate_events member ON member.event_seq=event.seq AND member.membership_state='active'
JOIN intent_candidates candidate ON candidate.id=member.candidate_id
WHERE event.branch_ref=? AND event.branch_generation=? AND event.state='published'
 AND candidate.status='published' AND event.commit_oid=candidate.published_commit_oid
 AND (substr(event.path,-8)='.test.ts' OR substr(event.path,-8)='.spec.ts')
 AND event.seq=(SELECT MAX(latest.seq) FROM capture_events latest
  WHERE latest.path=event.path AND latest.branch_ref=event.branch_ref
   AND latest.branch_generation=event.branch_generation AND latest.state='published')
ORDER BY event.seq DESC LIMIT ?`, input.BranchRef, input.BranchGeneration, min(64, state.IntentCandidateMaxCaptures-len(sources)))
	if err != nil {
		return nil, nil, err
	}
	type publishedTest struct {
		id  string
		seq int64
	}
	var tests []publishedTest
	for rows.Next() {
		var test publishedTest
		if err := rows.Scan(&test.id, &test.seq); err != nil {
			rows.Close()
			return nil, nil, err
		}
		tests = append(tests, test)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	files := append([]intentTypeScriptRecordedFile(nil), sources...)
	bySeq := make(map[int64]string)
	for _, test := range tests {
		event, err := loadIntentCaptureEvent(ctx, db, test.seq)
		if err != nil {
			return nil, nil, err
		}
		ops, err := state.LoadCaptureOps(ctx, db, test.seq)
		if err != nil {
			return nil, nil, err
		}
		capture := IntentCandidateCapture{Event: event, Ops: ops}
		file, found, err := load(capture)
		if err != nil {
			return nil, nil, err
		}
		if found {
			file.baseHead = head
			files = append(files, file)
			captures = append(captures, capture)
			bySeq[test.seq] = test.id
		}
	}
	if err := proveIntentTypeScriptJavaScriptTargets(ctx, input.RepoPath, files, captures); err != nil {
		return nil, nil, err
	}
	matched := make(map[string]map[string]bool)
	for _, test := range files[len(sources):] {
		pair := append(append([]intentTypeScriptRecordedFile(nil), files[:len(sources)]...), test)
		context := intentTypeScriptReferenceContexts(pair)[test.seq]
		if !strings.HasPrefix(context, " import ") && !strings.Contains(context, "\n import ") {
			continue
		}
		id := bySeq[test.seq]
		if matched[id] == nil {
			matched[id] = make(map[string]bool)
		}
		matched[id][test.path] = true
	}
	var candidates []state.IntentCandidate
	seen := make(map[string]bool)
	for _, test := range tests {
		if seen[test.id] || len(matched[test.id]) == 0 || len(candidates) >= state.IntentCandidateMaxOpenPerPair {
			continue
		}
		seen[test.id] = true
		candidate, found, err := state.IntentCandidateByID(ctx, db, test.id)
		if err != nil {
			return nil, nil, err
		}
		if found && candidate.BranchRef == input.BranchRef && candidate.BranchGeneration == input.BranchGeneration {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, matched, nil
}
