package daemon

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

// FinishOptions contains only caller choices. Reconnection checks these before
// reading new worktree, branch or index state; later edits never replace a target.
type FinishOptions struct {
	Purpose       string
	Message       string
	DryRun        bool
	IncludeStaged bool
	RetryID       string
}

// FinishWorkerConfig is one resolved, approved runtime snapshot. ConfigDigest
// identifies the caller's immutable revision; the worker also hashes the exact
// provider, approval, author and verification inputs it actually uses.
type FinishWorkerConfig struct {
	Mode                string
	Style               string
	ConfigDigest        string
	Provider            ai.ProviderConfig
	Approved            []ai.ConfirmationRequirement
	Capture             CaptureOpts
	VerificationMode    string
	VerificationTimeout time.Duration
	Command             verification.ApprovedCommand
	AuthorName          string
	AuthorEmail         string
}

// FinishWorker executes explicit requests on the canonical worker stack. The
// caller serializes Start/Execute and holds OperationGate.RLock when supplied.
// Execute receives the worker context: disconnecting a client must not cancel it.
// Only immutable provider exchanges and isolated checks run in the background.
type FinishWorker struct {
	RepoPath                       string
	RepositoryID                   string
	DB                             *state.DB
	Config                         FinishWorkerConfig
	OperationGate                  *sync.RWMutex
	ProtectionGate                 *sync.Mutex
	Wake, Files, Changes, Shutdown <-chan struct{}
	EvaluationInterval             time.Duration
	Runner                         verification.Runner
	// Transport is an embedding/test override already approved by the caller.
	// Nil builds the configured strict provider only for Intent execution.
	Transport ai.FinishTransport
	executing sync.Mutex
}

type FinishCheck struct {
	TreeOID  string              `json:"tree_oid"`
	Policy   string              `json:"policy"`
	Executed bool                `json:"executed"`
	Reused   bool                `json:"reused"`
	Result   verification.Result `json:"result"`
}

type FinishPlanStep struct {
	finish.Goal
	TreeOID string      `json:"tree_oid"`
	Author  string      `json:"author"`
	Check   FinishCheck `json:"check"`
}

// FinishSavedPlan avoids storing a full tree map for every candidate. Exact
// Git tree objects and the immutable source/target proof retain those versions.
type FinishSavedPlan struct {
	Version    int                 `json:"version"`
	Steps      []FinishPlanStep    `json:"steps"`
	Disclosure ai.FinishDisclosure `json:"disclosure"`
}

type FinishResult struct {
	state.FinishPublicationResult
	DryRun    bool `json:"dry_run"`
	NoChanges bool `json:"no_changes"`
}

func (w *FinishWorker) worktree(ctx context.Context) (git.Worktree, error) {
	if w.DB == nil {
		return git.Worktree{}, errors.New("finish: state is required")
	}
	wt, err := git.ResolveWorktree(ctx, w.RepoPath)
	if err != nil {
		return wt, err
	}
	dbPath, err := filepath.EvalSymlinks(w.DB.Path())
	if err != nil {
		return wt, err
	}
	if filepath.Clean(dbPath) != filepath.Clean(state.DBPathFromGitDir(wt.GitDir)) {
		return wt, state.ErrFinishIdentity
	}
	return wt, nil
}

func (w *FinishWorker) resolved(ctx context.Context) (git.Worktree, FinishWorkerConfig, string, error) {
	wt, err := w.worktree(ctx)
	if err != nil {
		return wt, FinishWorkerConfig{}, "", err
	}
	cfg := w.Config
	if cfg.Mode == "" {
		cfg.Mode = "local"
	}
	if cfg.Style == "" {
		cfg.Style = finish.StyleImperative
	}
	if cfg.Mode != "local" && cfg.Mode != "intent" {
		return wt, cfg, "", errors.New("finish: unsupported mode")
	}
	if cfg.Style != finish.StyleImperative && cfg.Style != finish.StyleConventional {
		return wt, cfg, "", errors.New("finish: unsupported message style")
	}
	if cfg.VerificationMode == "" {
		cfg.VerificationMode = "structural"
	}
	if cfg.VerificationMode != "none" && cfg.VerificationMode != "structural" && cfg.VerificationMode != "fast" && cfg.VerificationMode != "full" {
		return wt, cfg, "", errors.New("finish: unsupported verification policy")
	}
	if cfg.VerificationTimeout <= 0 {
		cfg.VerificationTimeout = 5 * time.Minute
	}
	if cfg.Provider.Timeout <= 0 {
		cfg.Provider.Timeout = ai.DefaultProviderTimeout
	}
	if cfg.AuthorName == "" || cfg.AuthorEmail == "" {
		return wt, cfg, "", errors.New("finish: frozen author name and email are required")
	}
	if cfg.Capture.SensitiveMatcher == nil {
		cfg.Capture.SensitiveMatcher = state.NewSensitiveMatcher()
	}
	if cfg.Capture.SafeIgnoreMatcher == nil {
		cfg.Capture.SafeIgnoreMatcher = state.NewSafeIgnoreMatcher()
	}
	if cfg.ConfigDigest == "" {
		return wt, cfg, "", errors.New("finish: approved configuration identity is required")
	}
	if cfg.VerificationMode == "fast" || cfg.VerificationMode == "full" {
		identity := cfg.Command.Identity()
		if string(identity.Mode) != cfg.VerificationMode || identity.Digest == "" {
			return wt, cfg, "", errors.New("finish: exact approved verification command is required")
		}
	}
	provider := cfg.Provider
	identity := []any{cfg.ConfigDigest, cfg.Mode, cfg.Style, provider.Mode, provider.BaseURL, provider.Model, provider.Timeout, provider.CAFile, provider.DiffEgress, cfg.Approved, cfg.VerificationMode, cfg.VerificationTimeout, cfg.Command.Identity(), cfg.AuthorName, cfg.AuthorEmail, cfg.Capture.MaxFileBytes, cfg.Capture.SubmodulePaths, cfg.Capture.SensitiveMatcher.Patterns(), cfg.Capture.SafeIgnoreMatcher.Patterns()}
	raw, err := json.Marshal(identity)
	if err != nil {
		return wt, cfg, "", err
	}
	return wt, cfg, fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func finishCallerMatches(r state.FinishRequest, cfg FinishWorkerConfig, digest string, options FinishOptions) bool {
	return r.Target.Mode == cfg.Mode && r.Target.Style == cfg.Style && r.Target.ConfigDigest == digest && r.Target.Purpose == options.Purpose && r.Target.Message == options.Message && r.Target.IncludeStaged == options.IncludeStaged && r.DryRun == options.DryRun
}

func newFinishID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "finish-" + hex.EncodeToString(value), nil
}

func finishBranchHead(ctx context.Context, repo string) (string, string, error) {
	out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "--quiet", "--no-recurse", "HEAD")
	if err != nil {
		return "", "", git.ErrFinishUnsafe
	}
	branch := strings.TrimSpace(string(out))
	head, err := git.RevParse(ctx, repo, branch)
	if errors.Is(err, git.ErrRefNotFound) {
		err = nil
		head = ""
	}
	return branch, head, err
}

// Start freezes one fresh complete checkpoint. Identical active callers attach
// first, even if files or staging changed while the original job was running.
func (w *FinishWorker) Start(ctx context.Context, options FinishOptions) (state.FinishRequest, bool, error) {
	wt, cfg, digest, err := w.resolved(ctx)
	if err != nil {
		return state.FinishRequest{}, false, err
	}
	worktreeID := checkpoint.WorktreeID(wt.Root)
	if options.RetryID != "" {
		if options.Purpose != "" || options.Message != "" || options.IncludeStaged {
			return state.FinishRequest{}, false, errors.New("finish: retry cannot change purpose, message or staged consent")
		}
		old, err := state.LoadFinishRequest(ctx, w.DB, options.RetryID)
		if err != nil {
			return old, false, err
		}
		if old.Target.WorktreeID != worktreeID || old.Target.Mode != cfg.Mode || old.Target.Style != cfg.Style || old.Target.ConfigDigest != digest {
			return old, false, state.ErrFinishIdentity
		}
		if old.State == state.FinishRunning || old.State == state.FinishPrepared {
			if old.DryRun != options.DryRun {
				return old, false, state.ErrFinishBusy
			}
			return old, false, nil
		}
		if err := git.CheckFinishPublicationSafety(ctx, wt.Root, old.Target.BranchRef, old.Target.ExpectedHead, old.Target.IndexDigest, old.Target.IncludeStaged); err != nil {
			return old, false, err
		}
		if _, _, err := w.evidence(ctx, wt, old.Target, cfg); err != nil {
			return old, false, err
		}
		retried, err := state.RetryFinishRequest(ctx, w.DB, old.ID, finishTimestamp(time.Now().Add(cfg.Provider.Timeout)), options.DryRun)
		return retried, err == nil, err
	}
	if err := finish.ValidatePurpose(options.Purpose); err != nil {
		return state.FinishRequest{}, false, err
	}
	if options.Message != "" {
		if err := finish.ValidateMessage(cfg.Style, options.Message); err != nil {
			return state.FinishRequest{}, false, err
		}
	}
	active, err := state.ActiveFinishRequest(ctx, w.DB, worktreeID)
	if err == nil {
		if finishCallerMatches(active, cfg, digest, options) {
			return active, false, nil
		}
		return active, false, state.ErrFinishBusy
	}
	if !errors.Is(err, state.ErrFinishNotFound) {
		return active, false, err
	}
	branch, head, err := finishBranchHead(ctx, wt.Root)
	if err != nil {
		return active, false, err
	}
	index, err := git.IndexContentDigest(ctx, wt.Root)
	if err != nil {
		return active, false, err
	}
	if err := git.CheckFinishPublicationSafety(ctx, wt.Root, branch, head, index, options.IncludeStaged); err != nil {
		return active, false, err
	}
	id, err := newFinishID()
	if err != nil {
		return active, false, err
	}
	cp, err := w.protect(ctx, wt, cfg, true)
	if err != nil {
		return active, false, err
	}
	base, err := readFinishTree(ctx, wt.Root, head)
	if err != nil {
		return active, false, err
	}
	source, err := readFinishTree(ctx, wt.Root, cp.TreeOID)
	if err != nil {
		return active, false, err
	}
	target, err := finishEligibleTarget(ctx, wt.Root, base, source, cfg.Capture)
	if err != nil {
		return active, false, err
	}
	if options.IncludeStaged {
		if err := overlayFinishIndex(ctx, wt.Root, base, target, cfg.Capture); err != nil {
			return active, false, err
		}
	}
	tree, err := materializeFinishTree(ctx, wt, w.DB, target)
	if err != nil {
		return active, false, err
	}
	repoID := w.RepositoryID
	if repoID == "" {
		repoID = checkpoint.WorktreeID(wt.CommonDir)
	}
	frozen := state.FinishTarget{Version: state.FinishVersion, RepositoryID: repoID, WorktreeID: worktreeID, BranchRef: branch, ExpectedHead: head, CheckpointID: cp.ID, CheckpointTree: cp.TreeOID, TargetTree: tree, IndexDigest: index, IncludeStaged: options.IncludeStaged, Mode: cfg.Mode, Style: cfg.Style, ConfigDigest: digest, Purpose: options.Purpose, Message: options.Message}
	if tree != cp.TreeOID {
		commit, err := git.CommitTreeDurable(ctx, wt.Root, tree, "acd finish target "+id, checkpoint.IdentityName, checkpoint.IdentityEmail)
		if err != nil {
			return active, false, err
		}
		frozen.TargetRef = git.FinishRefPrefix + worktreeID + "/" + id + "/target"
		frozen.TargetCommitOID = commit
		if _, err := git.EnsurePrivateRefDurable(ctx, wt.Root, git.FinishRefPrefix, frozen.TargetRef, commit); err != nil {
			return active, false, err
		}
	}
	if cp.ObservedRef != branch || cp.ObservedHead != head {
		return active, false, git.ErrFinishUnsafe
	}
	if err := git.CheckFinishPublicationSafety(ctx, wt.Root, branch, head, index, options.IncludeStaged); err != nil {
		return active, false, err
	}
	return state.StartFinishRequest(ctx, w.DB, state.FinishRequest{ID: id, Target: frozen, DryRun: options.DryRun, DeadlineTS: finishTimestamp(time.Now().Add(cfg.Provider.Timeout))})
}

func finishTimestamp(t time.Time) float64 { return float64(t.UnixNano()) / float64(time.Second) }
func finishDeadline(r state.FinishRequest) time.Time {
	return time.Unix(0, int64(r.DeadlineTS*float64(time.Second)))
}

func (w *FinishWorker) protect(ctx context.Context, wt git.Worktree, cfg FinishWorkerConfig, fresh bool) (state.Checkpoint, error) {
	if w.ProtectionGate != nil {
		w.ProtectionGate.Lock()
		defer w.ProtectionGate.Unlock()
	}
	branch, head, err := finishBranchHead(ctx, wt.Root)
	if err != nil {
		branch = ""
		head, _ = git.RevParse(ctx, wt.Root, "HEAD")
	} // Protection remains independent of unsafe publication state.
	opts := cfg.Capture
	base, err := readFinishTree(ctx, wt.Root, head)
	if err != nil {
		return state.Checkpoint{}, err
	}
	opts.SubmodulePaths = maps.Clone(opts.SubmodulePaths)
	if opts.SubmodulePaths == nil {
		opts.SubmodulePaths = map[string]bool{}
	}
	for path, entry := range base {
		if entry.Mode == "160000" {
			opts.SubmodulePaths[path] = true
		}
	}
	opts.WorktreeID = checkpoint.WorktreeID(wt.Root)
	opts.GitDir = wt.GitDir
	opts.CheckpointStore = &checkpoint.Store{DB: w.DB}
	if fresh {
		epoch, err := BeginProtectionObservation(ctx, w.DB)
		if err != nil {
			return state.Checkpoint{}, err
		}
		if err := RequireProtectionCheckpoint(ctx, w.DB, opts.WorktreeID, epoch); err != nil {
			return state.Checkpoint{}, err
		}
		opts.ObservationEpoch = epoch
		opts.CheckpointReason = state.CheckpointReasonManualBarrier
	} else {
		opts.ObservationEpoch = 0
		opts.CheckpointReason = state.CheckpointReasonWatch
	}
	summary, err := ProtectWorktree(ctx, wt.Root, w.DB, CaptureContext{BranchRef: branch, BaseHead: head}, opts)
	if err != nil {
		return state.Checkpoint{}, err
	}
	cp, err := state.ResolveCheckpoint(ctx, w.DB.Path(), summary.CheckpointID)
	if err != nil {
		return cp, err
	}
	if cp.Partial || cp.Phase != state.CheckpointCompleted {
		return cp, errors.New("finish: protection checkpoint is incomplete")
	}
	return cp, nil
}

func readFinishTree(ctx context.Context, repo, tree string) (finish.Tree, error) {
	result := finish.Tree{}
	if tree == "" {
		return result, nil
	}
	entries, err := git.LsTree(ctx, repo, tree, true)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		result[entry.Path] = finish.Entry{Mode: entry.Mode, OID: entry.OID}
	}
	return result, nil
}

func finishEligibility(ctx context.Context, repo string, paths []string, opts CaptureOpts) (map[string]bool, error) {
	sensitive := opts.SensitiveMatcher
	if sensitive == nil {
		sensitive = state.NewSensitiveMatcher()
	}
	safe := opts.SafeIgnoreMatcher
	if safe == nil {
		safe = state.NewSafeIgnoreMatcher()
	}
	checker := opts.IgnoreChecker
	if checker == nil {
		checker = git.NewIgnoreChecker(repo)
		defer checker.Close()
	}
	ignored, err := classifyIgnoredBatched(ctx, checker, paths, ignoreCheckBatchSize)
	if err != nil {
		return nil, err
	}
	result := map[string]bool{}
	for index, path := range paths {
		eligible := !ignored[index] && !sensitive.Match(path) && !safe.MatchFile(path) && !opts.SubmodulePaths[path]
		for parent := filepath.ToSlash(filepath.Dir(path)); eligible && parent != "."; parent = filepath.ToSlash(filepath.Dir(parent)) {
			eligible = !sensitive.MatchDirectory(parent) && !safe.MatchDirectory(parent) && !opts.SubmodulePaths[parent]
		}
		result[path] = eligible
	}
	return result, nil
}

func finishEligibleTarget(ctx context.Context, repo string, base, source finish.Tree, opts CaptureOpts) (finish.Tree, error) {
	paths := make([]string, 0, len(base))
	for path := range base {
		paths = append(paths, path)
	}
	eligible, err := finishEligibility(ctx, repo, paths, opts)
	if err != nil {
		return nil, err
	}
	target := finish.CloneTree(base)
	for path, entry := range base {
		if entry.Mode != "160000" && eligible[path] {
			delete(target, path)
		}
	}
	for path, entry := range source {
		target[path] = entry
	}
	return target, nil
}

func overlayFinishIndex(ctx context.Context, repo string, base, target finish.Tree, opts CaptureOpts) error {
	out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-only", "--no-renames", "--ita-visible-in-index", "-z")
	if err != nil {
		return err
	}
	paths := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
	if len(out) == 0 {
		return nil
	}
	eligible, err := finishEligibility(ctx, repo, paths, opts)
	if err != nil {
		return err
	}
	entries, err := git.LsFilesStaged(ctx, repo)
	if err != nil {
		return err
	}
	index := map[string]git.IndexEntry{}
	for _, entry := range entries {
		if entry.Stage != 0 {
			return git.ErrFinishUnsafe
		}
		index[entry.Path] = entry
	}
	for _, path := range paths {
		entry, exists := index[path]
		if !eligible[path] || entry.Mode == "160000" || base[path].Mode == "160000" {
			return fmt.Errorf("staged_scope_excluded: staged selection contains an excluded path")
		}
		if exists {
			target[path] = finish.Entry{Mode: entry.Mode, OID: entry.OID}
		} else {
			delete(target, path)
		}
	}
	return nil
}

func materializeFinishTree(ctx context.Context, wt git.Worktree, db *state.DB, tree finish.Tree) (string, error) {
	entries := make([]git.IndexEntry, 0, len(tree))
	for path, entry := range tree {
		entries = append(entries, git.IndexEntry{Path: path, Mode: entry.Mode, OID: entry.OID})
	}
	slices.SortFunc(entries, func(a, b git.IndexEntry) int { return strings.Compare(a.Path, b.Path) })
	id, err := newFinishID()
	if err != nil {
		return "", err
	}
	return git.WriteTreeDurable(ctx, wt.Root, filepath.Join(filepath.Dir(db.Path()), id+".index"), entries)
}

func (w *FinishWorker) evidence(ctx context.Context, wt git.Worktree, target state.FinishTarget, cfg FinishWorkerConfig) (finish.Evidence, string, error) {
	cp, err := state.ResolveCheckpoint(ctx, w.DB.Path(), target.CheckpointID)
	if err != nil {
		return finish.Evidence{}, "", err
	}
	if cp.Partial || cp.WorktreeID != target.WorktreeID || cp.TreeOID != target.CheckpointTree {
		return finish.Evidence{}, "", state.ErrFinishIdentity
	}
	pin := cp.Ref
	oid := cp.CommitOID
	if target.TargetRef != "" {
		pin = target.TargetRef
		oid = target.TargetCommitOID
	}
	observed, err := git.RevParse(ctx, wt.Root, pin)
	if err != nil {
		return finish.Evidence{}, "", err
	}
	tree, err := git.RevParse(ctx, wt.Root, oid+"^{tree}")
	if err != nil || observed != oid || tree != target.TargetTree {
		return finish.Evidence{}, "", state.ErrFinishIdentity
	}
	base, err := readFinishTree(ctx, wt.Root, target.ExpectedHead)
	if err != nil {
		return finish.Evidence{}, "", err
	}
	frozen, err := readFinishTree(ctx, wt.Root, target.TargetTree)
	if err != nil {
		return finish.Evidence{}, "", err
	}
	evidence := finish.Evidence{Base: base, Target: frozen, Changes: finish.NetChanges(base, frozen, "author-1")}
	baseTree := target.ExpectedHead
	if baseTree == "" {
		baseTree, err = materializeFinishTree(ctx, wt, w.DB, base)
		if err != nil {
			return evidence, "", err
		}
	}
	if err := finishRenameGroups(ctx, wt.Root, baseTree, target.TargetTree, evidence.Changes); err != nil {
		return evidence, "", err
	}
	return evidence, baseTree, finish.ValidateEvidence(evidence)
}

func finishRenameGroups(ctx context.Context, repo, base, target string, changes []finish.Change) error {
	out, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, state.FinishJSONByteCap, "diff", "--name-status", "--find-renames=50%", "-z", base, target, "--")
	if err != nil {
		return err
	}
	byPath := map[string]int{}
	for index, change := range changes {
		byPath[change.Path] = index
	}
	fields := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
	for i := 0; i < len(fields) && fields[i] != ""; {
		status := fields[i]
		if i+1 >= len(fields) {
			return errors.New("finish: malformed rename evidence")
		}
		if strings.HasPrefix(status, "R") {
			if i+2 >= len(fields) {
				return errors.New("finish: missing rename target")
			}
			old, oldOK := byPath[fields[i+1]]
			next, nextOK := byPath[fields[i+2]]
			if !oldOK || !nextOK {
				return errors.New("finish: rename differs from frozen evidence")
			}
			group := fmt.Sprintf("rename-%d", i)
			changes[old].AtomicGroup = group
			changes[next].AtomicGroup = group
			i += 3
		} else {
			i += 2
		}
	}
	return nil
}

type finishEvaluatedTransport struct{ ai.FinishTransport }

func (t finishEvaluatedTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	return evaluatePublication(ctx, func(jobCtx context.Context) ([]byte, error) { return t.FinishTransport.ExchangeFinish(jobCtx, body) })
}

func (w *FinishWorker) evaluationContext(ctx context.Context, wt git.Worktree, cfg FinishWorkerConfig, digest string, r state.FinishRequest) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	evaluation := &publicationEvaluation{gate: w.OperationGate, db: w.DB, cancel: cancel, wake: w.Wake, files: w.Files, changes: w.Changes, shutdown: w.Shutdown, interval: w.EvaluationInterval}
	evaluation.identity = func(checkCtx context.Context) (string, error) {
		_, _, current, err := w.resolved(checkCtx)
		if err != nil {
			return "", err
		}
		if current != digest {
			return "", state.ErrFinishIdentity
		}
		if err := git.CheckFinishPublicationSafety(checkCtx, wt.Root, r.Target.BranchRef, r.Target.ExpectedHead, r.Target.IndexDigest, r.Target.IncludeStaged); err != nil {
			return "", err
		}
		return r.ID + "/" + strconv.Itoa(r.Attempt) + "/" + r.Target.TargetTree + "/" + digest, nil
	}
	evaluation.protect = func(protectCtx context.Context) error { _, err := w.protect(protectCtx, wt, cfg, false); return err }
	return context.WithValue(ctx, publicationEvaluationKey{}, evaluation), cancel
}

// Execute starts no retry loop. Concurrent attachment returns the saved status;
// only this canonical stack plans, checks and publishes the explicit attempt.
func (w *FinishWorker) Execute(ctx context.Context, id string) (state.FinishRequest, error) {
	if !w.executing.TryLock() {
		return state.LoadFinishRequest(ctx, w.DB, id)
	}
	defer w.executing.Unlock()
	r, err := state.LoadFinishRequest(ctx, w.DB, id)
	if err != nil {
		return r, err
	}
	if r.State == state.FinishPrepared || r.State == state.FinishNeedsAttention {
		wt, err := w.worktree(ctx)
		if err != nil {
			return r, err
		}
		if r.Target.WorktreeID != checkpoint.WorktreeID(wt.Root) {
			return r, state.ErrFinishIdentity
		}
		return w.publish(ctx, wt, r, true)
	}
	if r.State != state.FinishRunning {
		return r, nil
	}
	wt, cfg, digest, err := w.resolved(ctx)
	if err != nil {
		return w.fail(r, err)
	}
	if r.Target.WorktreeID != checkpoint.WorktreeID(wt.Root) || r.Target.ConfigDigest != digest {
		return w.fail(r, state.ErrFinishIdentity)
	}
	planningCtx := ctx
	if cfg.Mode == "intent" {
		var planningCancel context.CancelFunc
		planningCtx, planningCancel = context.WithDeadline(ctx, finishDeadline(r))
		defer planningCancel()
		if err := planningCtx.Err(); err != nil {
			return w.fail(r, &ai.FinishPlanningError{Code: "provider_deadline", Err: err})
		}
	}
	if err := git.CheckFinishPublicationSafety(planningCtx, wt.Root, r.Target.BranchRef, r.Target.ExpectedHead, r.Target.IndexDigest, r.Target.IncludeStaged); err != nil {
		return w.fail(r, err)
	}
	evidence, baseTree, err := w.evidence(planningCtx, wt, r.Target, cfg)
	if err != nil {
		return w.fail(r, err)
	}
	if maps.Equal(evidence.Base, evidence.Target) {
		result, _ := json.Marshal(FinishResult{DryRun: r.DryRun, NoChanges: true})
		completed, err := state.UpdateFinishRequest(ctx, w.DB, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishCompleted, Result: result})
		if err != nil {
			return w.fail(r, err)
		}
		return completed, nil
	}
	evalCtx, cancel := w.evaluationContext(ctx, wt, cfg, digest, r)
	defer cancel()
	var plan finish.Plan
	var disclosure ai.FinishDisclosure
	if cfg.Mode == "local" {
		plan, err = finish.Local(evidence, finish.LocalOptions{Style: cfg.Style, Purpose: r.Target.Purpose, Message: r.Target.Message})
	} else {
		transport := w.Transport
		if transport == nil {
			var closer interface{ Close() error }
			transport, closer, err = ai.BuildFinishTransport(cfg.Provider, cfg.Approved)
			if closer != nil {
				defer closer.Close()
			}
		}
		if err == nil {
			var diffs []ai.FinishDiff
			diffCtx, diffCancel := context.WithDeadline(evalCtx, finishDeadline(r))
			diffs, err = finishProviderDiffs(diffCtx, wt.Root, baseTree, r.Target.TargetTree, evidence.Changes, cfg.Provider.DiffEgress)
			diffCancel()
			if err == nil {
				var planned ai.FinishPlanResult
				planned, err = ai.PlanFinish(evalCtx, finishEvaluatedTransport{transport}, evidence, ai.FinishOptions{Deadline: finishDeadline(r), Style: cfg.Style, Purpose: r.Target.Purpose, Message: r.Target.Message, Diffs: diffs, DiffEgress: cfg.Provider.DiffEgress, BeforeCall: func(callCtx context.Context) error {
					_, err := state.ConsumeFinishPlanningCall(callCtx, w.DB, r.ID, r.Attempt)
					return err
				}})
				plan, disclosure = planned.Plan, planned.Disclosure
			}
		}
	}
	if err != nil {
		return w.failPlan(r, FinishSavedPlan{Version: 1, Disclosure: disclosure}, err)
	}
	if err := finish.Validate(evidence, plan); err != nil {
		return w.fail(r, err)
	}
	if _, err := state.UpdateFinishRequest(ctx, w.DB, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishRunning, Stage: "verifying"}); err != nil {
		return w.fail(r, err)
	}
	saved := FinishSavedPlan{Version: 1, Disclosure: disclosure}
	commits := make([]git.FinishCommit, 0, len(plan.Steps))
	verifyCtx, verifyCancel := context.WithTimeout(evalCtx, cfg.VerificationTimeout)
	defer verifyCancel()
	for _, step := range plan.Steps {
		tree, err := materializeFinishTree(verifyCtx, wt, w.DB, step.Tree)
		if err != nil {
			return w.fail(r, err)
		}
		saved.Steps = append(saved.Steps, FinishPlanStep{Goal: step.Goal, TreeOID: tree, Author: step.Author})
		commits = append(commits, git.FinishCommit{TreeOID: tree, Message: step.Message, AuthorName: cfg.AuthorName, AuthorEmail: cfg.AuthorEmail})
	}
	proof, err := git.BuildFinishChain(verifyCtx, wt.Root, git.FinishBuildOptions{RequestID: r.ID, Attempt: r.Attempt, WorktreeID: r.Target.WorktreeID, BranchRef: r.Target.BranchRef, ExpectedHead: r.Target.ExpectedHead, TargetTree: r.Target.TargetTree, IndexDigest: r.Target.IndexDigest, IncludeStaged: r.Target.IncludeStaged, ObjectRef: git.FinishRefPrefix + r.Target.WorktreeID + "/" + r.ID + "/" + strconv.Itoa(r.Attempt) + "/chain", Commits: commits, Verify: func(checkCtx context.Context, index int, oid, tree string) error {
		check := FinishCheck{TreeOID: tree, Policy: cfg.VerificationMode}
		check.Result, err = evaluatePublication(checkCtx, func(jobCtx context.Context) (verification.Result, error) {
			candidateID := r.ID + "-" + strconv.Itoa(r.Attempt) + "-" + strconv.Itoa(index)
			if cfg.VerificationMode == "none" || cfg.VerificationMode == "structural" {
				return w.Runner.CheckStructural(jobCtx, verification.StructuralRequest{RepoPath: wt.Root, CandidateID: candidateID, CommitOID: oid})
			}
			return w.Runner.Run(jobCtx, verification.Request{RepoPath: wt.Root, CandidateID: candidateID, CommitOID: oid, Command: cfg.Command})
		})
		check.Executed = check.Result.Executed
		saved.Steps[index].Check = check
		if err != nil {
			return err
		}
		if check.Result.Status != verification.StatusPassed || check.Result.NeedsAttention {
			return fmt.Errorf("finish: candidate verification %s", check.Result.Status)
		}
		return nil
	}})
	if err != nil {
		return w.failPlan(r, saved, err)
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		return w.fail(r, err)
	}
	if r.DryRun {
		result, _ := json.Marshal(FinishResult{DryRun: true})
		completed, err := state.UpdateFinishRequest(ctx, w.DB, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishCompleted, Plan: raw, Result: result})
		if err != nil {
			return w.failPlan(r, saved, err)
		}
		return completed, nil
	}
	prepared, err := state.PrepareFinishPublication(ctx, w.DB, r.ID, r.Attempt, proof, raw)
	if err != nil {
		return w.failPlan(r, saved, err)
	}
	return w.publish(ctx, wt, prepared, false)
}

func (w *FinishWorker) fail(r state.FinishRequest, cause error) (state.FinishRequest, error) {
	return w.failPlan(r, FinishSavedPlan{}, cause)
}
func (w *FinishWorker) failPlan(r state.FinishRequest, plan FinishSavedPlan, cause error) (state.FinishRequest, error) {
	if r.State != state.FinishRunning {
		return r, cause
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code := "finish_failed"
	var planning *ai.FinishPlanningError
	if errors.As(cause, &planning) {
		code = planning.Code
	}
	var raw json.RawMessage
	if len(plan.Steps) > 0 || plan.Disclosure.MetadataIncluded {
		raw, _ = json.Marshal(plan)
		if len(raw) > state.FinishJSONByteCap {
			raw = nil
			code = "finish_plan_too_large"
		}
	}
	failed, err := state.UpdateFinishRequest(ctx, w.DB, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishFailed, Plan: raw, ReasonCode: code, Reason: ai.SanitizePlannerError(cause.Error())})
	return failed, errors.Join(cause, err)
}

func (w *FinishWorker) publish(ctx context.Context, wt git.Worktree, r state.FinishRequest, resume bool) (state.FinishRequest, error) {
	p, err := state.LoadFinishPublication(ctx, w.DB, r.ID)
	if err != nil {
		return r, err
	}
	result, applyErr := git.ApplyFinishPublication(ctx, wt.Root, p, resume)
	if applyErr == nil {
		applyErr = git.WithProvenFinishPublication(ctx, wt.Root, p, func() error {
			var err error
			r, err = state.CompleteFinishPublication(ctx, w.DB, r.ID, r.Attempt, result)
			return err
		})
	}
	if applyErr == nil {
		return r, nil
	}
	recordCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	attention, err := state.MarkFinishPublicationAttention(recordCtx, w.DB, r.ID, r.Attempt, ai.SanitizePlannerError(applyErr.Error()), result)
	return attention, errors.Join(applyErr, err)
}

// RecoverPrepared never resumes planning or renews a deadline. It interrupts
// prepublication work and reconciles only the saved exact Git publication proof.
func (w *FinishWorker) RecoverPrepared(ctx context.Context) (state.FinishRequest, error) {
	wt, err := w.worktree(ctx)
	if err != nil {
		return state.FinishRequest{}, err
	}
	worktreeID := checkpoint.WorktreeID(wt.Root)
	if _, err := state.InterruptFinishRequests(ctx, w.DB, worktreeID); err != nil {
		return state.FinishRequest{}, err
	}
	r, err := state.ActiveFinishRequest(ctx, w.DB, worktreeID)
	if errors.Is(err, state.ErrFinishNotFound) {
		return state.FinishRequest{}, nil
	}
	if err != nil {
		return r, err
	}
	return w.publish(ctx, wt, r, true)
}

func finishProviderDiffs(ctx context.Context, repo, base, target string, changes []finish.Change, egress bool) ([]ai.FinishDiff, error) {
	if !egress {
		return nil, nil
	}
	var diffs []ai.FinishDiff
	for _, change := range changes {
		diff := ai.FinishDiff{ChangeID: change.ID}
		for index, entry := range []finish.Entry{change.Before, change.After} {
			if entry.OID == "" {
				continue
			}
			out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "cat-file", "-s", entry.OID)
			if err != nil {
				return nil, err
			}
			size, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err != nil {
				return nil, err
			}
			if index == 0 {
				diff.BeforeSize = size
			} else {
				diff.AfterSize = size
			}
			body, err := git.CatFileBlobLimited(ctx, repo, entry.OID, 8000)
			if err != nil && !errors.Is(err, git.ErrStdoutOverflow) {
				return nil, err
			}
			if strings.ContainsRune(string(body), 0) || (err == nil && !utf8.Valid(body)) {
				diff.Binary = true
			}
		}
		if !diff.Binary {
			out, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, ai.IntentStageDiffCap+1, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--no-renames", base, target, "--", ":(literal)"+change.Path)
			if errors.Is(err, git.ErrStdoutOverflow) || len(out) > ai.IntentStageDiffCap {
				return nil, &ai.FinishPlanningError{Code: "provider_input_too_large", Err: fmt.Errorf("exact textual diff is at least %d bytes; limit is %d bytes; use an explicit local finish for the complete offline batch", len(out), ai.IntentStageDiffCap)}
			}
			if err != nil {
				return nil, err
			}
			if strings.ContainsRune(string(out), 0) || !utf8.Valid(out) {
				diff.Binary = true
			} else {
				diff.Text = string(out)
			}
		}
		diffs = append(diffs, diff)
	}
	return diffs, nil
}
