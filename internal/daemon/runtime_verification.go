package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

type intentRepairVerificationFailure struct {
	Status    string
	Output    string
	CheckedTS float64
}

func (e *intentRepairVerificationFailure) Error() string {
	status := strings.TrimSpace(e.Status)
	if status == "" {
		status = "needs_attention"
	}
	return "runtime repair verification: exact rebuilt commit check " + status
}

func runtimeIntentCandidateVerifier(
	repoRoot string,
	gitDir string,
	parent string,
	revisionID int64,
	command verification.ApprovedCommand,
) IntentCandidateVerifier {
	var mu sync.Mutex
	currentParent := parent
	return func(
		ctx context.Context,
		assignment ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(captures) == 0 {
			return IntentCandidateVerification{},
				errors.New("runtime verification: candidate has no captures")
		}
		ordered := append([]IntentCandidateCapture(nil), captures...)
		sort.Slice(ordered, func(i, j int) bool {
			return ordered[i].Event.Seq < ordered[j].Event.Seq
		})
		treeOID, err := evaluatePublication(ctx, func(jobCtx context.Context) (string, error) {
			return materializeIntentCandidateTree(jobCtx, repoRoot, gitDir, currentParent, ordered)
		})
		if err != nil {
			return IntentCandidateVerification{},
				fmt.Errorf("runtime verification: materialize exact candidate: %w", err)
		}
		commitOID, err := git.CommitTreeWithIdentity(
			ctx, repoRoot, treeOID, "Verify Intent v2 candidate",
			"Auto Commit Daemon", "acd@localhost", currentParent)
		if err != nil {
			return IntentCandidateVerification{},
				fmt.Errorf("runtime verification: build exact candidate commit: %w", err)
		}
		candidateID := assignment.CandidateID
		if strings.TrimSpace(candidateID) == "" {
			candidateID = runtimeVerificationCandidateID(revisionID, ordered)
		}
		cacheKey, cache, err := loadRuntimeVerificationCache(ctx, []any{treeOID, currentParent, revisionID, command, candidateID})
		if err != nil {
			return IntentCandidateVerification{}, err
		}
		if cached, ok := cache[cacheKey]; ok {
			currentParent = commitOID
			return cached, nil
		}
		result, err := evaluatePublication(ctx, func(jobCtx context.Context) (verification.Result, error) {
			return (verification.Runner{}).Run(jobCtx, verification.Request{
				RepoPath: repoRoot, CandidateID: candidateID, CommitOID: commitOID, Command: command,
			})
		})
		observed := IntentCandidateVerification{
			Status: string(result.Status), Output: result.Output,
			CheckedTS: float64(time.Now().UTC().UnixNano()) / 1e9,
		}
		if err != nil {
			return observed, fmt.Errorf("runtime verification: %w", err)
		}
		if result.NeedsAttention || result.Status != verification.StatusPassed {
			return observed, fmt.Errorf("runtime verification: exact candidate check %s",
				result.Status)
		}
		if err := storeRuntimeVerificationCache(ctx, cacheKey, cache, observed); err != nil {
			return observed, err
		}
		currentParent = commitOID
		return observed, nil
	}
}

func runtimeIntentRepairCommitVerifier(
	repoRoot string,
	revisionID int64,
	command verification.ApprovedCommand,
) git.IntentRepairCommitVerifier {
	return func(ctx context.Context, commitOID string, index int) error {
		candidateID := fmt.Sprintf(
			"repair-%d-%d-%s",
			revisionID,
			index,
			shortRuntimeVerificationOID(commitOID),
		)
		result, err := evaluatePublication(ctx, func(jobCtx context.Context) (verification.Result, error) {
			return (verification.Runner{}).Run(jobCtx, verification.Request{
				RepoPath: repoRoot, CandidateID: candidateID,
				CommitOID: commitOID, Command: command,
			})
		})
		if err != nil {
			return fmt.Errorf("runtime repair verification: %w", err)
		}
		if result.NeedsAttention ||
			result.Status != verification.StatusPassed {
			failure := &intentRepairVerificationFailure{
				Status: string(result.Status), Output: result.Output,
				CheckedTS: float64(time.Now().UTC().UnixNano()) / 1e9,
			}
			return fmt.Errorf("%w: %w",
				git.ErrIntentRepairVerification, failure)
		}
		return nil
	}
}

func shortRuntimeVerificationOID(oid string) string {
	oid = strings.TrimSpace(oid)
	if len(oid) > 12 {
		return oid[:12]
	}
	return oid
}

func runtimeVerificationCandidateID(
	revisionID int64,
	captures []IntentCandidateCapture,
) string {
	var body strings.Builder
	body.WriteString(strconv.FormatInt(revisionID, 10))
	for _, capture := range captures {
		body.WriteByte(0)
		body.WriteString(strconv.FormatInt(capture.Event.Seq, 10))
	}
	sum := sha256.Sum256([]byte(body.String()))
	return "runtime-" + hex.EncodeToString(sum[:12])
}

// Keep only the latest sixteen proven successful checks. Results are keyed by
// exact materialized tree, parent and approved command/revision, never age or
// candidate name alone. Successful diagnostic output is bounded to 1 KiB.
const runtimeVerificationCacheMeta = "intent.v2.verification_cache"

type runtimeVerificationCache map[string]IntentCandidateVerification

func loadRuntimeVerificationCache(ctx context.Context, input any) (string, runtimeVerificationCache, error) {
	evaluation, _ := ctx.Value(publicationEvaluationKey{}).(*publicationEvaluation)
	cache := runtimeVerificationCache{}
	if evaluation == nil || evaluation.db == nil {
		return "", cache, nil
	}
	data, err := json.Marshal(input)
	if err != nil {
		return "", cache, err
	}
	hash := sha256.Sum256(data)
	key := hex.EncodeToString(hash[:])
	raw, found, err := state.MetaGet(ctx, evaluation.db, runtimeVerificationCacheMeta)
	if err != nil {
		return "", nil, err
	}
	// This is derived evidence: malformed cache data requires a fresh check,
	// never a permanent publication block. Database errors still propagate.
	if found && json.Unmarshal([]byte(raw), &cache) != nil {
		cache = nil
	}
	if cache == nil {
		cache = runtimeVerificationCache{}
	}
	return key, cache, nil
}

func storeRuntimeVerificationCache(ctx context.Context, key string, cache runtimeVerificationCache, result IntentCandidateVerification) error {
	evaluation, _ := ctx.Value(publicationEvaluationKey{}).(*publicationEvaluation)
	if evaluation == nil || evaluation.db == nil || key == "" || result.Status != "passed" {
		return nil
	}
	if len(result.Output) > 1024 {
		result.Output = result.Output[:1024]
	}
	cache[key] = result
	for len(cache) > 16 {
		oldest := ""
		for candidate, record := range cache {
			if oldest == "" || record.CheckedTS < cache[oldest].CheckedTS || record.CheckedTS == cache[oldest].CheckedTS && candidate < oldest {
				oldest = candidate
			}
		}
		delete(cache, oldest)
	}
	return state.MetaSetJSON(ctx, evaluation.db, runtimeVerificationCacheMeta, cache)
}
