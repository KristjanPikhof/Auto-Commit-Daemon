package git

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Small native transactions bound both argument size and open ref locks.
const privateRefBatchSize = 64

// Object inspection has no native ref locks; its stdin/stdout can use a larger
// bounded group without increasing the transaction's file descriptor count.
const objectTypeBatchSize = 256

// ObjectTypes reads exact identities in bounded batches. Missing objects are
// returned as "missing" so migration can preserve the other available objects.
func ObjectTypes(ctx context.Context, repo string, oids []string) (map[string]string, error) {
	types := make(map[string]string, len(oids))
	for start := 0; start < len(oids); start += objectTypeBatchSize {
		batch := oids[start:min(start+objectTypeBatchSize, len(oids))]
		for _, oid := range batch {
			if !exactObjectID(oid) {
				return nil, errors.New("git: invalid batch object identity")
			}
		}
		out, err := RunWithLimit(ctx, RunOpts{Dir: repo, Stdin: strings.NewReader(strings.Join(batch, "\n") + "\n"), Timeout: DefaultReadTimeout},
			int64(len(batch)*128), "cat-file", "--batch-check=%(objectname) %(objecttype)")
		if err != nil {
			return nil, err
		}
		lines := bytes.Split(bytes.TrimSuffix(out, []byte("\n")), []byte("\n"))
		if len(lines) != len(batch) {
			return nil, errors.New("git: incomplete batch object response")
		}
		for i, line := range lines {
			fields := strings.Fields(string(line))
			if len(fields) != 2 || fields[0] != batch[i] {
				return nil, errors.New("git: invalid batch object response")
			}
			types[batch[i]] = fields[1]
		}
	}
	return types, nil
}

func exactObjectID(oid string) bool {
	_, err := hex.DecodeString(oid)
	return err == nil && strings.ToLower(oid) == oid && (len(oid) == 40 || len(oid) == 64)
}

// EnsurePrivateRefsDurable creates absent direct pins and verifies existing
// pins in bounded native transactions. It never retargets an existing ref.
// Earlier groups stay retained if a later group requires attention.
func EnsurePrivateRefsDurable(ctx context.Context, repo, prefix string, refs []RefExpectation) error {
	if prefix != CheckpointRefPrefix && prefix != "refs/acd/migration/" && prefix != FinishRefPrefix {
		return errors.New("git: unsupported private ref namespace")
	}
	unique, err := uniquePrivateRefs(refs, prefix)
	if err != nil {
		return err
	}
	var failures error
	for start := 0; start < len(unique); start += privateRefBatchSize {
		batch := unique[start:min(start+privateRefBatchSize, len(unique))]
		oids := make([]string, len(batch))
		for i, item := range batch {
			oids[i] = item.OID
		}
		types, err := ObjectTypes(ctx, repo, oids)
		if err != nil {
			return errors.Join(failures, err)
		}
		for _, item := range batch {
			if types[item.OID] != "commit" {
				err = fmt.Errorf("git: private pin object %s is not a commit", item.OID)
				break
			}
		}
		if err == nil {
			err = lockPrivateRefBatch(ctx, repo, batch, true)
		}
		if err == nil {
			continue
		}
		// One moved or aliased ref must not prevent independently valid
		// captured objects in this group from receiving immutable pins.
		if ctx.Err() != nil {
			return errors.Join(failures, ctx.Err())
		}
		for _, item := range batch {
			_, pinErr := EnsurePrivateRefDurable(ctx, repo, prefix, item.Ref, item.OID)
			failures = errors.Join(failures, pinErr)
			if ctx.Err() != nil {
				return errors.Join(failures, ctx.Err())
			}
		}
	}
	return failures
}

// VerifyPrivateRefs proves direct identities under native locks, without
// creating missing pins or touching refs, the index, or the worktree.
func VerifyPrivateRefs(ctx context.Context, repo string, refs []RefExpectation) error {
	unique, err := uniquePrivateRefs(refs, "refs/acd/")
	if err != nil {
		return err
	}
	oids := make([]string, len(unique))
	for i, item := range unique {
		oids[i] = item.OID
	}
	types, err := ObjectTypes(ctx, repo, oids)
	if err != nil {
		return err
	}
	for _, item := range unique {
		if types[item.OID] != "commit" {
			return fmt.Errorf("git: private pin object %s is not a commit", item.OID)
		}
	}
	for start := 0; start < len(unique); start += privateRefBatchSize {
		if err := lockPrivateRefBatch(ctx, repo, unique[start:min(start+privateRefBatchSize, len(unique))], false); err != nil {
			return err
		}
	}
	return nil
}

func uniquePrivateRefs(refs []RefExpectation, prefix string) ([]RefExpectation, error) {
	unique := make([]RefExpectation, 0, len(refs))
	seen := make(map[string]string, len(refs))
	for _, item := range refs {
		if !isValidFullRef(item.Ref) || !strings.HasPrefix(item.Ref, prefix) || len(item.Ref) == len(prefix) || !exactObjectID(item.OID) {
			return nil, errors.New("git: invalid private ref expectation")
		}
		if prior, ok := seen[item.Ref]; ok {
			if prior != item.OID {
				return nil, ErrCheckpointRefCollision
			}
			continue
		}
		seen[item.Ref] = item.OID
		unique = append(unique, item)
	}
	return unique, nil
}

// A prepared no-deref verify rejects dangling aliases but can accept a
// same-OID alias. Read symref identities again while all literal locks are held.
func lockPrivateRefBatch(ctx context.Context, repo string, refs []RefExpectation, create bool) error {
	current, err := readPrivateRefBatch(ctx, repo, refs)
	if err != nil {
		return err
	}
	creates, verifies := map[string]string{}, map[string]string{}
	primary, expected, replacement := refs[0].Ref, refs[0].OID, []string(nil)
	for i, item := range refs {
		actual := current[item.Ref]
		if actual != "" && actual != item.OID {
			return ErrCheckpointRefCollision
		}
		if actual == "" && !create {
			return ErrRefNotFound
		}
		if i == 0 {
			if actual == "" {
				zero, err := repositoryZeroOID(ctx, repo)
				if err != nil {
					return err
				}
				expected, replacement = zero, []string{item.OID}
			}
		} else if actual == "" {
			creates[item.Ref] = item.OID
		} else {
			verifies[item.Ref] = item.OID
		}
	}
	created := len(creates) > 0 || len(replacement) > 0
	err = withLockedExpectedRefsTransaction(ctx, repo, primary, expected, "", "", false, DefaultWriteTimeout,
		func(lockedCtx context.Context) error {
			locked, err := readPrivateRefBatch(lockedCtx, repo, refs)
			if err != nil {
				return err
			}
			for _, item := range refs {
				if locked[item.Ref] != current[item.Ref] {
					return ErrCheckpointRefCollision
				}
			}
			return nil
		}, creates, verifies, replacement...)
	if err != nil || !created {
		return err
	}
	// Include the post-create interval: hooks can replace a pin after Git
	// releases creation locks, before this helper reports durable success.
	return lockPrivateRefBatch(ctx, repo, refs, false)
}

func readPrivateRefBatch(ctx context.Context, repo string, refs []RefExpectation) (map[string]string, error) {
	args := []string{"for-each-ref", "--format=%(refname)%00%(objectname)%00%(symref)"}
	wanted := make(map[string]bool, len(refs))
	for _, item := range refs {
		args = append(args, item.Ref)
		wanted[item.Ref] = true
	}
	out, err := RunWithLimit(ctx, RunOpts{Dir: repo, Timeout: DefaultReadTimeout}, 1<<20, args...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(refs))
	for _, line := range bytes.Split(bytes.TrimSuffix(out, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(string(line), "\x00")
		if len(fields) != 3 {
			return nil, errors.New("git: malformed private ref response")
		}
		if !wanted[fields[0]] {
			continue
		}
		if fields[2] != "" {
			return nil, fmt.Errorf("%w: %s is symbolic", ErrCheckpointRefCollision, fields[0])
		}
		result[fields[0]] = fields[1]
	}
	return result, nil
}
