package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

const migrationRefPrefix = "refs/acd/migration/"

var scanBridgeEntries = daemon.ScanProtectedEntries

type BridgeSnapshot struct {
	RepositoryID string  `json:"repository_id"`
	WorktreeID   string  `json:"worktree_id"`
	Repo         string  `json:"repo"`
	Ref          string  `json:"ref"`
	CommitOID    string  `json:"commit_oid"`
	TreeOID      string  `json:"tree_oid"`
	Digest       string  `json:"digest"`
	CreatedTS    float64 `json:"created_ts"`
}

// Bridge continuously makes source-independent private refs while setup
// quiesces v19 writers and proves held v20 workers. It never opens state.db.
type Bridge struct {
	operationID string
	records     []central.RepoRecord
	interval    time.Duration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	latest      map[string]BridgeSnapshot
	created     []BridgeSnapshot
	errors      map[string]error
}

func StartBridge(ctx context.Context, operationID string, records []central.RepoRecord, interval time.Duration) (*Bridge, error) {
	return StartBridgeWithProgress(ctx, operationID, records, interval, nil)
}

// StartBridgeWithProgress starts the migration bridge and reports each
// repository before its initial protection scan. Later background rescans do
// not emit progress because they overlap other setup phases.
func StartBridgeWithProgress(ctx context.Context, operationID string, records []central.RepoRecord, interval time.Duration, progress func(completed, total int, repo string)) (*Bridge, error) {
	if operationID == "" {
		return nil, errors.New("migration: bridge operation id is required")
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	bridgeCtx, cancel := context.WithCancel(ctx)
	bridge := &Bridge{operationID: operationID, interval: interval, cancel: cancel,
		latest: make(map[string]BridgeSnapshot), errors: make(map[string]error)}
	for _, record := range records {
		if !record.LifecycleDisabled() {
			bridge.records = append(bridge.records, record)
		}
	}
	sort.Slice(bridge.records, func(i, j int) bool { return bridge.records[i].WorktreeID < bridge.records[j].WorktreeID })
	for index, record := range bridge.records {
		if progress != nil {
			progress(index+1, len(bridge.records), record.Path)
		}
		if _, err := bridge.capture(bridgeCtx, record); err != nil {
			cancel()
			return bridge, err
		}
	}
	bridge.wg.Add(1)
	go bridge.loop(bridgeCtx)
	return bridge, nil
}

func (b *Bridge) loop(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, record := range b.records {
				_, err := b.capture(ctx, record)
				b.mu.Lock()
				if err != nil {
					b.errors[record.WorktreeID] = err
				} else {
					// A complete later rescan covers a transient read or
					// stability failure. Only a repository that is still
					// incomplete at the final bridge barrier may block the
					// cutover.
					delete(b.errors, record.WorktreeID)
				}
				b.mu.Unlock()
			}
		}
	}
}

func (b *Bridge) capture(ctx context.Context, record central.RepoRecord) (BridgeSnapshot, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
	if err != nil {
		return BridgeSnapshot{}, fmt.Errorf("migration: bridge resolve %s: %w", record.Path, err)
	}
	checker := gitpkg.NewIgnoreChecker(wt.Root)
	entries, _, _, err := scanBridgeEntries(ctx, wt.Root, daemon.CaptureOpts{
		IgnoreChecker: checker, CheckpointStore: &checkpoint.Store{},
	})
	_ = checker.Close()
	if err != nil {
		return BridgeSnapshot{}, fmt.Errorf("migration: bridge scan %s: %w", record.Path, err)
	}
	digest := bridgeTreeDigest(entries)
	b.mu.Lock()
	prior, unchanged := b.latest[record.WorktreeID]
	b.mu.Unlock()
	if unchanged && prior.Digest == digest {
		return prior, nil
	}
	indexEntries := make([]gitpkg.IndexEntry, 0, len(entries))
	for _, entry := range entries {
		indexEntries = append(indexEntries, gitpkg.IndexEntry{Mode: entry.Mode, OID: entry.OID, Path: entry.Path})
	}
	sequence := time.Now().UnixNano()
	index := filepath.Join(wt.GitDir, "acd", fmt.Sprintf("migration-bridge-%d.index", sequence))
	treeOID, err := gitpkg.WriteTreeDurable(ctx, wt.Root, index, indexEntries)
	if err != nil {
		return BridgeSnapshot{}, err
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, wt.Root, treeOID,
		"acd migration bridge "+b.operationID+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		return BridgeSnapshot{}, err
	}
	ref := fmt.Sprintf("%s%s/%s/%d", migrationRefPrefix, b.operationID, record.WorktreeID, sequence)
	if _, err := gitpkg.EnsurePrivateRefDurable(ctx, wt.Root, migrationRefPrefix, ref, commitOID); err != nil {
		return BridgeSnapshot{}, err
	}
	snapshot := BridgeSnapshot{RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID,
		Repo: wt.Root, Ref: ref, CommitOID: commitOID, TreeOID: treeOID, Digest: digest,
		CreatedTS: float64(time.Now().UnixNano()) / float64(time.Second)}
	b.mu.Lock()
	b.latest[record.WorktreeID] = snapshot
	b.created = append(b.created, snapshot)
	b.mu.Unlock()
	return snapshot, nil
}

// Finalize performs one last synchronous snapshot after held workers report
// ready, then stops the bridge. The caller must complete one later worker
// checkpoint barrier before deleting these refs.
func (b *Bridge) Finalize(ctx context.Context) ([]BridgeSnapshot, error) {
	if b == nil {
		return nil, nil
	}
	b.cancel()
	b.wg.Wait()
	for _, record := range b.records {
		if _, err := b.capture(ctx, record); err != nil {
			return nil, err
		}
		b.mu.Lock()
		delete(b.errors, record.WorktreeID)
		b.mu.Unlock()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.errors) > 0 {
		for _, err := range b.errors {
			return nil, err
		}
	}
	latest := make([]BridgeSnapshot, 0, len(b.latest))
	for _, snapshot := range b.latest {
		latest = append(latest, snapshot)
	}
	sort.Slice(latest, func(i, j int) bool { return latest[i].WorktreeID < latest[j].WorktreeID })
	return latest, nil
}

func (b *Bridge) Stop() {
	if b == nil {
		return
	}
	b.cancel()
	b.wg.Wait()
}

func (b *Bridge) Cleanup(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	created := append([]BridgeSnapshot(nil), b.created...)
	b.mu.Unlock()
	var combined error
	for _, snapshot := range created {
		err := gitpkg.DeletePrivateRefDurable(ctx, snapshot.Repo, migrationRefPrefix,
			snapshot.Ref, snapshot.CommitOID)
		combined = errors.Join(combined, err)
	}
	return combined
}

func (b *Bridge) WriteRecoveryManifest(path string) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	created := append([]BridgeSnapshot(nil), b.created...)
	b.mu.Unlock()
	body, err := json.MarshalIndent(map[string]any{
		"operation_id": b.operationID, "retained_for_recovery": created,
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return replaceBytes(path, append(body, '\n'), 0o600)
}

// CreatedCount reports how many durable bridge refs rollback retained. It is
// used only for truthful operation-journal and user-facing rollback status.
func (b *Bridge) CreatedCount() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.created)
}

func bridgeTreeDigest(entries []checkpoint.Entry) string {
	hash := sha256.New()
	for _, entry := range entries {
		hash.Write([]byte(entry.Path))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.Mode))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.OID))
		hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}
