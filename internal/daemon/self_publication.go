package daemon

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const selfPublicationTerminalRetention = 7 * 24 * time.Hour

// SelfPublicationCheckpoint identifies a deterministic publication boundary
// exposed to fault-injection and recovery tests.
type SelfPublicationCheckpoint string

const (
	SelfPublicationBeforeCAS        SelfPublicationCheckpoint = "before_cas"
	SelfPublicationAfterCAS         SelfPublicationCheckpoint = "after_cas"
	SelfPublicationBeforeCompletion SelfPublicationCheckpoint = "before_completion"
	SelfPublicationAfterCompletion  SelfPublicationCheckpoint = "after_completion"
)

// SelfPublicationCheckpointEvent carries the complete immutable identity at a
// publication boundary. Group is "event", "intent", or the durable Intent v2
// candidate ID. Members is copied before invoking the callback.
type SelfPublicationCheckpointEvent struct {
	Checkpoint SelfPublicationCheckpoint
	Group      string
	SourceHead string
	TargetOID  string
	TreeOID    string
	Members    []state.SelfPublicationMember
	Journal    state.SelfPublication
}

type selfPublicationAttempt struct {
	journal state.SelfPublication
	group   string
	hook    func(SelfPublicationCheckpointEvent) error
}

var errSelfPublicationCASAmbiguous = errors.New(
	"daemon: self-publication CAS outcome is ambiguous")
var errSelfPublicationCASNotApplied = errors.New(
	"daemon: self-publication CAS was not applied")

func prepareSelfPublication(
	ctx context.Context,
	db *state.DB,
	cctx CaptureContext,
	sourceHead, targetOID, treeOID, group string,
	members []state.SelfPublicationMember,
	hook func(SelfPublicationCheckpointEvent) error,
) (selfPublicationAttempt, error) {
	id, err := newSelfPublicationAttemptID()
	if err != nil {
		return selfPublicationAttempt{}, err
	}
	ordered := append([]state.SelfPublicationMember(nil), members...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].EventSeq < ordered[j].EventSeq
	})
	journal := state.SelfPublication{
		ID:               id,
		BranchRef:        cctx.BranchRef,
		BranchGeneration: cctx.BranchGeneration,
		SourceHead:       sourceHead,
		TargetCommitOID:  targetOID,
		TargetTreeOID:    treeOID,
		Members:          ordered,
	}
	if _, err := state.PrepareSelfPublication(ctx, db, journal); err != nil {
		return selfPublicationAttempt{},
			fmt.Errorf("daemon: prepare self-publication: %w", err)
	}
	return selfPublicationAttempt{journal: journal, group: group, hook: hook}, nil
}

func (attempt selfPublicationAttempt) checkpoint(
	checkpoint SelfPublicationCheckpoint,
) error {
	if attempt.hook == nil {
		return nil
	}
	journal := attempt.journal
	journal.Members = append(
		[]state.SelfPublicationMember(nil), attempt.journal.Members...)
	return attempt.hook(SelfPublicationCheckpointEvent{
		Checkpoint: checkpoint,
		Group:      attempt.group,
		SourceHead: journal.SourceHead,
		TargetOID:  journal.TargetCommitOID,
		TreeOID:    journal.TargetTreeOID,
		Members: append(
			[]state.SelfPublicationMember(nil), journal.Members...),
		Journal: journal,
	})
}

// applyCAS exposes the raw post-CAS checkpoint while the journal is still
// prepared, then durably records git_applied. A failed or canceled pre-CAS
// attempt is abandoned with a non-canceled context so its target and
// membership can be retried under a fresh attempt ID.
func (attempt selfPublicationAttempt) applyCAS(
	ctx context.Context,
	repoRoot, oldOID string,
	db *state.DB,
	apply func() error,
) error {
	if err := attempt.checkpoint(SelfPublicationBeforeCAS); err != nil {
		if abandonErr := attempt.abandonPrepared(ctx, db, err); abandonErr != nil {
			return errors.Join(err, abandonErr)
		}
		return errors.Join(errSelfPublicationCASNotApplied, err)
	}
	if err := apply(); err != nil {
		return attempt.resolveCASError(ctx, repoRoot, oldOID, db, err)
	}
	if err := attempt.checkpoint(SelfPublicationAfterCAS); err != nil {
		return err
	}
	return attempt.markGitApplied(ctx, db)
}

func (attempt selfPublicationAttempt) markGitApplied(
	ctx context.Context,
	db *state.DB,
) error {
	stateCtx, cancel := selfPublicationStateContext(ctx)
	defer cancel()
	if _, err := state.MarkSelfPublicationGitApplied(
		stateCtx, db, attempt.journal, selfPublicationNow()); err != nil {
		return fmt.Errorf("daemon: mark self-publication git applied: %w", err)
	}
	return nil
}

// resolveCASError distinguishes an error returned before a ref mutation from
// a transport/cancellation error reported after Git applied the literal HEAD
// CAS. Only an exact current target is promoted here. If the target remains in
// current ancestry but HEAD has moved again, the prepared row is deliberately
// retained for restart recovery instead of guessing.
func (attempt selfPublicationAttempt) resolveCASError(
	ctx context.Context,
	repoRoot, oldOID string,
	db *state.DB,
	cause error,
) error {
	probeCtx, cancel := selfPublicationStateContext(ctx)
	defer cancel()
	head, err := git.RevParse(probeCtx, repoRoot, "HEAD")
	if errors.Is(err, git.ErrRefNotFound) && oldOID == "" {
		if abandonErr := attempt.abandonPrepared(ctx, db, cause); abandonErr != nil {
			return errors.Join(cause, abandonErr)
		}
		return errors.Join(errSelfPublicationCASNotApplied, cause)
	}
	if err != nil {
		return errors.Join(errSelfPublicationCASAmbiguous, cause,
			fmt.Errorf("probe HEAD after CAS error: %w", err))
	}
	if head == attempt.journal.TargetCommitOID {
		if err := attempt.checkpoint(SelfPublicationAfterCAS); err != nil {
			return err
		}
		if err := attempt.markGitApplied(ctx, db); err != nil {
			return errors.Join(cause, err)
		}
		return nil
	}
	if head == oldOID {
		if abandonErr := attempt.abandonPrepared(ctx, db, cause); abandonErr != nil {
			return errors.Join(cause, abandonErr)
		}
		return errors.Join(errSelfPublicationCASNotApplied, cause)
	}
	targetApplied, ancestryErr := git.IsAncestor(
		probeCtx, repoRoot, attempt.journal.TargetCommitOID, head)
	if ancestryErr != nil {
		return errors.Join(errSelfPublicationCASAmbiguous, cause,
			fmt.Errorf("prove self-publication target ancestry: %w", ancestryErr))
	}
	if targetApplied {
		return errors.Join(errSelfPublicationCASAmbiguous, cause)
	}
	if abandonErr := attempt.abandonPrepared(ctx, db, cause); abandonErr != nil {
		return errors.Join(cause, abandonErr)
	}
	return errors.Join(errSelfPublicationCASNotApplied, cause)
}

func (attempt selfPublicationAttempt) complete(
	ctx context.Context,
	db *state.DB,
	completion state.SelfPublicationCompletion,
) error {
	if err := attempt.checkpoint(SelfPublicationBeforeCompletion); err != nil {
		return err
	}
	stateCtx, cancel := selfPublicationStateContext(ctx)
	defer cancel()
	if _, err := state.CompleteSelfPublication(
		stateCtx, db, attempt.journal, completion); err != nil {
		return fmt.Errorf("daemon: complete self-publication: %w", err)
	}
	if err := attempt.checkpoint(SelfPublicationAfterCompletion); err != nil {
		return err
	}
	return nil
}

func (attempt selfPublicationAttempt) abandon(
	ctx context.Context,
	db *state.DB,
	cause error,
) error {
	if err := attempt.abandonPrepared(ctx, db, cause); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (attempt selfPublicationAttempt) abandonPrepared(
	ctx context.Context,
	db *state.DB,
	cause error,
) error {
	stateCtx, cancel := selfPublicationStateContext(ctx)
	defer cancel()
	reason := "publication stopped before ref mutation"
	if cause != nil {
		reason = cause.Error()
	}
	if _, err := state.AbandonSelfPublication(
		stateCtx, db, attempt.journal, reason, selfPublicationNow()); err != nil {
		return fmt.Errorf("daemon: abandon self-publication: %w", err)
	}
	return nil
}

func selfPublicationMembers(
	items []intentReplayItem,
) []state.SelfPublicationMember {
	var members []state.SelfPublicationMember
	for _, item := range items {
		candidateID := sql.NullString{
			String: item.candidateID,
			Valid:  item.candidateID != "",
		}
		for _, event := range item.allCoveredEvents() {
			members = append(members, state.SelfPublicationMember{
				EventSeq: event.Seq, CandidateID: candidateID,
			})
		}
	}
	return members
}

func selfPublicationCompletion(
	opts ReplayOpts,
	message sql.NullString,
) state.SelfPublicationCompletion {
	now := selfPublicationNow()
	completion := state.SelfPublicationCompletion{
		PublishedTS:     now,
		Message:         message,
		CandidateStatus: state.IntentCandidatePublished,
	}
	if opts.IntentRepairEnabled {
		horizon := opts.IntentRepairHorizon
		if horizon <= 0 {
			horizon = 10 * time.Minute
		}
		completion.CandidateStatus = state.IntentCandidateSoftPublished
		completion.SoftPublicationDeadline = sql.NullFloat64{
			Float64: now + horizon.Seconds(),
			Valid:   true,
		}
	}
	return completion
}

func selfPublicationStateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func maintainSelfPublicationJournal(
	ctx context.Context,
	db *state.DB,
) (int64, error) {
	return state.PruneTerminalSelfPublicationsBefore(
		ctx, db,
		selfPublicationNow()-selfPublicationTerminalRetention.Seconds(),
		state.SelfPublicationMaxPruneBatch,
	)
}

func newSelfPublicationAttemptID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("daemon: generate self-publication attempt id: %w", err)
	}
	return "sp_" + hex.EncodeToString(raw[:]), nil
}

func selfPublicationNow() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}
