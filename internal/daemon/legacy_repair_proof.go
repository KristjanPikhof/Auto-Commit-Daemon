package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func recoveredIntentRepairPlanDigest(
	ctx context.Context,
	repoRoot string,
	repair state.IntentRepair,
	mappings []state.IntentRepairCommit,
) (string, error) {
	membersByCandidate := make(map[string][]int64)
	for _, member := range repair.Members {
		membersByCandidate[member.CandidateID] = append(
			membersByCandidate[member.CandidateID], member.EventSeq)
	}
	var candidates []intentRepairDigestCandidate
	for i := 0; i < len(mappings); {
		candidateID := mappings[i].CandidateID.String
		newOID := mappings[i].NewOID.String
		candidate := intentRepairDigestCandidate{
			candidateID: candidateID,
			authorOID:   newOID,
			eventSeqs:   append([]int64(nil), membersByCandidate[candidateID]...),
		}
		for i < len(mappings) &&
			mappings[i].CandidateID.Valid &&
			mappings[i].CandidateID.String == candidateID {
			if !mappings[i].NewOID.Valid || mappings[i].NewOID.String != newOID {
				return "", intentRepairRecoveryProofError(
					"repair %s has an inconsistent candidate mapping",
					repair.ID)
			}
			candidate.replaces = append(candidate.replaces, mappings[i].OldOID)
			i++
		}
		resolvedCommit, err := git.RevParse(
			ctx, repoRoot, newOID+"^{commit}")
		if err != nil {
			if errors.Is(err, git.ErrRefNotFound) {
				return "", intentRepairRecoveryProofError(
					"repair %s mapped commit %s is missing or invalid",
					repair.ID, newOID)
			}
			return "", fmt.Errorf(
				"daemon: recover intent repair: resolve rebuilt commit: %w", err)
		}
		if resolvedCommit != newOID {
			return "", intentRepairRecoveryProofError(
				"repair %s mapping %s does not name a commit directly",
				repair.ID, newOID)
		}
		tree, err := git.RevParse(ctx, repoRoot, newOID+"^{tree}")
		if err != nil {
			if errors.Is(err, git.ErrRefNotFound) {
				return "", intentRepairRecoveryProofError(
					"repair %s mapped commit %s has no valid tree",
					repair.ID, newOID)
			}
			return "", fmt.Errorf(
				"daemon: recover intent repair: resolve rebuilt tree: %w", err)
		}
		message, err := git.RunWithLimit(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, intentRepairMessageReadCap,
			"show", "-s", "--format=%B", newOID)
		if err != nil {
			if errors.Is(err, git.ErrStdoutOverflow) {
				return "", intentRepairRecoveryProofError(
					"repair %s rebuilt commit message exceeds %d bytes",
					repair.ID, intentRepairMessageReadCap)
			}
			readErr := fmt.Errorf(
				"daemon: recover intent repair: read rebuilt message: %w", err)
			return "", classifyRecoveredIntentRepairMetadataError(
				ctx, repoRoot, repair.ID, newOID, "message", readErr)
		}
		candidate.treeOID = tree
		candidate.message = string(message)
		candidates = append(candidates, candidate)
	}
	digest, err := hashIntentRepairCandidates(ctx, repoRoot, candidates)
	if errors.Is(err, git.ErrStdoutOverflow) {
		return "", intentRepairRecoveryProofError(
			"repair %s rebuilt author identity exceeds %d bytes",
			repair.ID, intentRepairAuthorReadCap)
	}
	var authorErr *intentRepairAuthorReadError
	if errors.As(err, &authorErr) {
		err = classifyRecoveredIntentRepairMetadataError(
			ctx, repoRoot, repair.ID, authorErr.oid,
			"author", err)
	}
	return digest, err
}

// ErrIntentRepairRecoveryProof marks durable repair evidence that cannot be
// reconciled by retrying the same recovery pass. Callers use it to distinguish
// operator-attention cases from transient Git or SQLite failures.
var ErrIntentRepairRecoveryProof = errors.New(
	"daemon: intent repair recovery proof is invalid")

type intentRepairDigestCandidate struct {
	candidateID string
	replaces    []string
	eventSeqs   []int64
	treeOID     string
	message     string
	authorOID   string
}

type intentRepairAuthorReadError struct {
	oid string
	err error
}

func intentRepairRecoveryProofError(format string, args ...any) error {
	values := make([]any, 0, len(args)+1)
	values = append(values, ErrIntentRepairRecoveryProof)
	values = append(values, args...)
	return fmt.Errorf("%w: "+format, values...)
}

func classifyRecoveredIntentRepairMetadataError(
	ctx context.Context,
	repoRoot string,
	repairID string,
	commitOID string,
	evidence string,
	readErr error,
) error {
	if errors.Is(readErr, context.Canceled) ||
		errors.Is(readErr, context.DeadlineExceeded) {
		return readErr
	}
	_, err := git.RevParse(ctx, repoRoot, commitOID+"^{commit}")
	if errors.Is(err, git.ErrRefNotFound) {
		return intentRepairRecoveryProofError(
			"repair %s mapped commit %s has missing or invalid %s evidence",
			repairID, commitOID, evidence)
	}
	return readErr
}

const (
	intentRepairBackupRetention = 7 * 24 * time.Hour
	intentRepairBackupCap       = 50
	intentRepairMessageReadCap  = int64(64 << 10)
	intentRepairAuthorReadCap   = int64(16 << 10)
)

func hashIntentRepairCandidates(
	ctx context.Context,
	repoRoot string,
	candidates []intentRepairDigestCandidate,
) (string, error) {
	hash := sha256.New()
	writeField := func(value string) {
		_, _ = fmt.Fprintf(hash, "%d:", len(value))
		_, _ = hash.Write([]byte(value))
	}
	version := "acd-intent-repair-plan-v1"
	for _, candidate := range candidates {
		if len(candidate.eventSeqs) > 0 {
			version = "acd-intent-repair-plan-v2"
			break
		}
	}
	writeField(version)
	for _, candidate := range candidates {
		author, err := git.RunWithLimit(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, intentRepairAuthorReadCap,
			"show", "-s", "--format=%an%x00%ae%x00%aI", candidate.authorOID)
		if err != nil {
			return "", &intentRepairAuthorReadError{
				oid: candidate.authorOID,
				err: err,
			}
		}
		writeField(candidate.candidateID)
		eventSeqs := append([]int64(nil), candidate.eventSeqs...)
		sort.Slice(eventSeqs, func(i, j int) bool { return eventSeqs[i] < eventSeqs[j] })
		for _, seq := range eventSeqs {
			writeField(strconv.FormatInt(seq, 10))
		}
		for _, oldOID := range candidate.replaces {
			writeField(oldOID)
		}
		writeField(candidate.treeOID)
		writeField(strings.TrimRight(candidate.message, "\n"))
		writeField(strings.TrimRight(string(author), "\n"))
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func (err *intentRepairAuthorReadError) Error() string {
	return fmt.Sprintf("daemon: intent repair: read author identity: %v", err.err)
}

func (err *intentRepairAuthorReadError) Unwrap() error { return err.err }
