// Package checkpoint owns the protection-plane transaction spanning durable
// Git objects, the per-repository operation ledger, and private checkpoint
// refs. It does not scan the filesystem or publish normal Git commits.
package checkpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	IdentityName  = "ACD Checkpoint"
	IdentityEmail = "checkpoint@localhost"
)

// Entry is one eligible worktree path in a complete checkpoint tree. Blob
// objects must already have been written through Git's durable object helper.
type Entry struct {
	Path string
	Mode string
	OID  string
}

// Request is a complete, already-scanned protection snapshot. Exclusions are
// category counts only; paths are deliberately absent from this boundary.
type Request struct {
	RepoRoot         string
	WorktreeID       string
	Reason           string
	ObservationEpoch int64
	CoverageEpoch    int64
	ObservedHead     string
	ObservedRef      string
	Entries          []Entry
	EventSeqs        []int64
	Exclusions       []state.CheckpointExclusion
	Now              time.Time
}

// Result identifies one completed private checkpoint.
type Result struct {
	Checkpoint state.Checkpoint
	RefCreated bool
}

// Store coordinates the two durable stores. The worker remains the only
// writer; Store introduces no goroutines or additional ownership model.
type Store struct {
	DB *state.DB
	// retentionInventory is a test seam for proving that retention work is
	// bounded by policy decisions rather than by the number of retained refs.
	retentionInventory func(context.Context, string, []string) (map[string]int64, error)
}

// WorktreeID returns the stable 16-hex identity derived from the canonical
// worktree root. Common-directory identity is computed separately by the
// registry/supervisor boundary.
func WorktreeID(canonicalRoot string) string {
	digest := sha256.Sum256([]byte(canonicalRoot))
	return hex.EncodeToString(digest[:8])
}

// Create writes tree and commit objects, prepares SQLite, creates the private
// ref with create-only CAS, rereads it, then completes SQLite. A failure after
// prepare is intentionally recoverable by RecoverPrepared.
func (s Store) Create(ctx context.Context, request Request) (Result, error) {
	if s.DB == nil {
		return Result{}, errors.New("checkpoint: nil state database")
	}
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	now := request.Now
	if now.IsZero() {
		now = time.Now()
	}
	id, err := NewID(now)
	if err != nil {
		return Result{}, err
	}
	entries := append([]Entry(nil), request.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	indexPath := filepath.Join(filepath.Dir(s.DB.Path()), "checkpoint-"+id+".index")
	indexEntries := make([]gitpkg.IndexEntry, 0, len(entries))
	for _, entry := range entries {
		indexEntries = append(indexEntries, gitpkg.IndexEntry{
			Mode: entry.Mode,
			OID:  entry.OID,
			Path: entry.Path,
		})
	}
	treeOID, err := gitpkg.WriteTreeDurable(ctx, request.RepoRoot, indexPath, indexEntries)
	if err != nil {
		return Result{}, fmt.Errorf("checkpoint: build tree: %w", err)
	}
	message := "acd checkpoint " + id + "\n"
	commitOID, err := gitpkg.CommitTreeDurable(ctx, request.RepoRoot, treeOID,
		message, IdentityName, IdentityEmail)
	if err != nil {
		return Result{}, fmt.Errorf("checkpoint: create commit: %w", err)
	}
	checkpointRef := gitpkg.CheckpointRefPrefix + request.WorktreeID + "/" + id
	operationID := "op-" + id
	planDigest := requestDigest(request, entries, treeOID, commitOID, checkpointRef)
	checkpoint := state.Checkpoint{
		ID:               id,
		OperationID:      operationID,
		WorktreeID:       request.WorktreeID,
		Reason:           request.Reason,
		ObservationEpoch: request.ObservationEpoch,
		CoverageEpoch:    request.CoverageEpoch,
		ObservedHead:     request.ObservedHead,
		ObservedRef:      request.ObservedRef,
		TreeOID:          treeOID,
		CommitOID:        commitOID,
		Ref:              checkpointRef,
		CreatedTS:        float64(now.UnixNano()) / float64(time.Second),
		EventSeqs:        append([]int64(nil), request.EventSeqs...),
		Exclusions:       append([]state.CheckpointExclusion(nil), request.Exclusions...),
	}
	if _, err := state.PrepareCheckpoint(ctx, s.DB, checkpoint, planDigest); err != nil {
		return Result{}, fmt.Errorf("checkpoint: prepare state: %w", err)
	}
	created, err := gitpkg.EnsureCheckpointRef(ctx, request.RepoRoot, checkpointRef, commitOID)
	if err != nil {
		if errors.Is(err, gitpkg.ErrCheckpointRefCollision) {
			_ = state.MarkCheckpointNeedsAction(context.Background(), s.DB, id,
				"checkpoint ref points at an unexpected object")
		}
		return Result{}, fmt.Errorf("checkpoint: create private ref: %w", err)
	}
	if err := state.CompleteCheckpoint(ctx, s.DB, id, checkpointRef, commitOID,
		float64(time.Now().UnixNano())/float64(time.Second)); err != nil {
		return Result{}, fmt.Errorf("checkpoint: complete state: %w", err)
	}
	checkpoint.Phase = state.CheckpointCompleted
	checkpoint.CompletedTS.Valid = true
	checkpoint.CompletedTS.Float64 = float64(time.Now().UnixNano()) / float64(time.Second)
	return Result{Checkpoint: checkpoint, RefCreated: created}, nil
}

// RecoverPrepared reconciles the only safe crash states: absent ref (retry
// create), or exact expected ref (complete). A different target is durable
// needs_action and is never overwritten or deleted.
func (s Store) RecoverPrepared(ctx context.Context, repoRoot string) error {
	if s.DB == nil {
		return errors.New("checkpoint: nil state database")
	}
	projection, err := state.ReadCheckpointProjection(ctx, s.DB.Path(), 100)
	if err != nil {
		return err
	}
	for _, checkpoint := range projection.Recoverable {
		if checkpoint.Phase == state.CheckpointNeedsAction {
			continue
		}
		observed, err := gitpkg.RevParse(ctx, repoRoot, checkpoint.Ref)
		switch {
		case err == nil && observed == checkpoint.CommitOID:
			if err := state.CompleteCheckpoint(ctx, s.DB, checkpoint.ID,
				checkpoint.Ref, checkpoint.CommitOID, 0); err != nil {
				return fmt.Errorf("checkpoint: recover exact ref %s: %w", checkpoint.ID, err)
			}
		case err == nil:
			if markErr := state.MarkCheckpointNeedsAction(context.Background(), s.DB,
				checkpoint.ID, "checkpoint ref points at an unexpected object"); markErr != nil {
				return fmt.Errorf("checkpoint: mark ambiguous ref %s: %w", checkpoint.ID, markErr)
			}
			return fmt.Errorf("checkpoint: %w: %s points at %s, want %s",
				gitpkg.ErrCheckpointRefCollision, checkpoint.Ref, observed, checkpoint.CommitOID)
		case errors.Is(err, gitpkg.ErrRefNotFound):
			if _, err := gitpkg.EnsureCheckpointRef(ctx, repoRoot, checkpoint.Ref, checkpoint.CommitOID); err != nil {
				return fmt.Errorf("checkpoint: recreate ref %s: %w", checkpoint.ID, err)
			}
			if err := state.CompleteCheckpoint(ctx, s.DB, checkpoint.ID,
				checkpoint.Ref, checkpoint.CommitOID, 0); err != nil {
				return fmt.Errorf("checkpoint: complete recreated ref %s: %w", checkpoint.ID, err)
			}
		default:
			return fmt.Errorf("checkpoint: inspect prepared ref %s: %w", checkpoint.ID, err)
		}
	}
	return nil
}

// NewID returns the public checkpoint identifier format.
func NewID(now time.Time) (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("checkpoint: random id: %w", err)
	}
	return "cp-" + strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(random), nil
}

func validateRequest(request Request) error {
	if strings.TrimSpace(request.RepoRoot) == "" {
		return errors.New("checkpoint: repository root is required")
	}
	if len(request.WorktreeID) != 16 || strings.ToLower(request.WorktreeID) != request.WorktreeID {
		return errors.New("checkpoint: invalid worktree id")
	}
	if request.ObservationEpoch < 0 || request.CoverageEpoch < 0 || request.CoverageEpoch > request.ObservationEpoch {
		return errors.New("checkpoint: invalid coverage epoch")
	}
	seen := make(map[string]struct{}, len(request.Entries))
	for _, entry := range request.Entries {
		clean := filepath.ToSlash(filepath.Clean(entry.Path))
		if entry.Path == "" || filepath.IsAbs(entry.Path) || clean != entry.Path || clean == "." ||
			strings.HasPrefix(clean, "../") || strings.ContainsRune(entry.Path, 0) {
			return fmt.Errorf("checkpoint: invalid entry path %q", entry.Path)
		}
		if entry.Mode != gitpkg.RegularFileMode && entry.Mode != gitpkg.ExecutableFileMode && entry.Mode != gitpkg.SymlinkMode {
			return fmt.Errorf("checkpoint: invalid mode for %q", entry.Path)
		}
		if strings.TrimSpace(entry.OID) == "" {
			return fmt.Errorf("checkpoint: missing object id for %q", entry.Path)
		}
		if _, duplicate := seen[entry.Path]; duplicate {
			return fmt.Errorf("checkpoint: duplicate entry path %q", entry.Path)
		}
		seen[entry.Path] = struct{}{}
	}
	return nil
}

func requestDigest(request Request, entries []Entry, treeOID, commitOID, checkpointRef string) string {
	hash := sha256.New()
	fields := []string{
		request.WorktreeID, request.Reason,
		strconv.FormatInt(request.ObservationEpoch, 10),
		strconv.FormatInt(request.CoverageEpoch, 10),
		request.ObservedHead, request.ObservedRef, treeOID, commitOID, checkpointRef,
	}
	for _, field := range fields {
		hash.Write([]byte(field))
		hash.Write([]byte{0})
	}
	for _, entry := range entries {
		hash.Write([]byte(entry.Path))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.Mode))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.OID))
		hash.Write([]byte{0})
	}
	for _, seq := range request.EventSeqs {
		hash.Write([]byte(strconv.FormatInt(seq, 10)))
		hash.Write([]byte{0})
	}
	for _, exclusion := range request.Exclusions {
		hash.Write([]byte(exclusion.Category))
		hash.Write([]byte{0})
		hash.Write([]byte(strconv.FormatInt(exclusion.Count, 10)))
		hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

// RemoveScratchIndexes removes only checkpoint scratch indexes left by a
// killed worker. Private refs, objects, and ledger rows are never touched.
func RemoveScratchIndexes(stateDir string) error {
	matches, err := filepath.Glob(filepath.Join(stateDir, "checkpoint-cp-*.index"))
	if err != nil {
		return err
	}
	for _, match := range matches {
		if err := os.Remove(match); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
