package installer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func revalidateSetupPlan(ctx context.Context, roots paths.Roots, reviewed Plan, ignoreServiceState bool) error {
	selection := "none"
	if len(reviewed.Integrations) > 0 {
		selection = strings.Join(reviewed.Integrations, ",")
	}
	current, err := BuildPlan(ctx, roots, Options{
		Repo: reviewed.Repo, Integrations: selection, Executable: reviewed.SourceExecutable,
		SkipServiceCheck: reviewed.ServiceCheckSkipped,
		Configuration:    reviewed.Configuration,
	})
	if err != nil {
		return err
	}
	if ignoreServiceState {
		copyRuntimeServiceState(&current.PriorService, reviewed.PriorService)
		current.Digest = digestPlan(current)
	}
	if current.Digest != reviewed.Digest {
		return errors.New("setup: live installation state changed after preview; preview again")
	}
	return nil
}

func copyRuntimeServiceState(target *ServiceState, reviewed ServiceState) {
	target.Loaded = reviewed.Loaded
	target.Enabled = reviewed.Enabled
	target.LegacyLoaded = reviewed.LegacyLoaded
	target.SessionLoaded = reviewed.SessionLoaded
}

// Only newly completed, registered checkpoint refs may extend the reviewed
// inventory before apply or after the final barrier. All other inputs stay exact.
func refreshUninstallPurgeRefs(ctx context.Context, roots paths.Roots, reviewed *UninstallPlan, checkServiceState bool) error {
	current, err := BuildUninstallPlan(ctx, roots, reviewed.PurgeData)
	if err != nil {
		return err
	}
	if !checkServiceState {
		copyRuntimeServiceState(&current.PriorService, reviewed.PriorService)
		if current.PublicationHoldDigest != reviewed.PublicationHoldDigest && current.PublicationHoldDigest != fileDigest(uninstallHoldBody(*reviewed), 0600) {
			return errors.New("uninstall: owned publication hold changed after shutdown")
		}
		current.PublicationHoldDigest = reviewed.PublicationHoldDigest
	}
	if len(current.PurgeTargets) != len(reviewed.PurgeTargets) {
		return errors.New("uninstall: repository inventory changed after preview")
	}
	freshRefs := make([][]gitpkg.RefExpectation, len(current.PurgeTargets))
	additions := map[string]string{}
	for i, target := range current.PurgeTargets {
		freshRefs[i] = target.Refs
		actual := map[string]string{}
		for _, ref := range target.Refs {
			actual[ref.Ref] = ref.OID
		}
		for _, ref := range reviewed.PurgeTargets[i].Refs {
			if actual[ref.Ref] != ref.OID {
				return fmt.Errorf("uninstall: private ref changed after preview: %s", ref.Ref)
			}
			delete(actual, ref.Ref)
		}
		for ref, oid := range actual {
			additions[target.RepositoryID+"\x00"+ref] = oid
		}
		current.PurgeTargets[i].Refs = reviewed.PurgeTargets[i].Refs
	}
	if digestUninstallPlan(current) != reviewed.Digest {
		return errors.New("uninstall: live installation state changed after preview; preview again")
	}
	for key, oid := range additions {
		if current.checkpointRefs[key] == oid {
			delete(additions, key)
		}
	}
	if len(additions) > 0 {
		return errors.New("uninstall: unreviewed private refs added after preview; preview again")
	}
	// The journal retains the reviewed namespace plan ID; CAS uses the proved final refs.
	for i := range reviewed.PurgeTargets {
		reviewed.PurgeTargets[i].Refs = freshRefs[i]
	}
	return nil
}

// Checkpoint growth is part of the reviewed namespace, but only SQLite-completed
// identities with exact current direct refs can be omitted from its fingerprint.
func qualifyUninstallCheckpoints(ctx context.Context, targets []PurgeTarget) (map[string]string, error) {
	actual := map[string]string{}
	for _, target := range targets {
		for _, ref := range target.Refs {
			actual[target.RepositoryID+"\x00"+ref.Ref] = ref.OID
		}
	}
	qualified := map[string]string{}
	for _, target := range targets {
		prefix := target.RepositoryID + "\x00" + gitpkg.CheckpointRefPrefix + target.WorktreeID + "/"
		needed := false
		for key := range actual {
			if strings.HasPrefix(key, prefix) {
				needed = true
				break
			}
		}
		if target.Unavailable != "" || !needed {
			continue
		}
		dbPath := state.DBPathFromGitDir(target.GitDir)
		if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		db, err := state.OpenReadOnly(ctx, dbPath)
		if err != nil {
			return nil, err
		}
		version, err := db.UserVersion(ctx)
		if err != nil {
			db.Close()
			return nil, err
		}
		if version < 20 {
			db.Close()
			continue
		}
		rows, err := db.ReadSQL().QueryContext(ctx, `SELECT cp.checkpoint_ref,cp.commit_oid FROM checkpoints cp JOIN operations o ON o.id=cp.operation_id WHERE cp.worktree_id=? AND cp.phase='completed' AND cp.retained=1 AND o.kind='checkpoint' AND o.status='completed'`, target.WorktreeID)
		if err != nil {
			db.Close()
			return nil, err
		}
		for rows.Next() {
			var ref, oid string
			if err = rows.Scan(&ref, &oid); err != nil {
				break
			}
			key := target.RepositoryID + "\x00" + ref
			if !strings.HasPrefix(key, prefix) || actual[key] == "" {
				continue
			}
			if actual[key] != oid {
				err = fmt.Errorf("uninstall: checkpoint ref differs from completed proof: %s", ref)
				break
			}
			qualified[key] = oid
		}
		err = errors.Join(err, rows.Err(), rows.Close(), db.Close())
		if err != nil {
			return nil, err
		}
	}
	return qualified, nil
}
