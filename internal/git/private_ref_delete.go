package git

import (
	"context"
	"errors"
	"time"
)

var ErrPrivateRefDeleteOutcomeUnknown = errors.New("git: private ref deletion outcome requires attention")

// DeletePrivateRefsCAS reports exactly which bounded groups were removed. The
// caller can restore those groups after a later failure without overwriting refs.
func DeletePrivateRefsCAS(ctx context.Context, repo string, refs []RefExpectation) ([]RefExpectation, error) {
	return deletePrivateRefsCAS(ctx, repo, refs, DefaultWriteTimeout)
}

func deletePrivateRefsCAS(ctx context.Context, repo string, refs []RefExpectation, timeout time.Duration) ([]RefExpectation, error) {
	unique, err := uniquePrivateRefs(refs, "refs/acd/")
	if err != nil || len(unique) == 0 {
		return nil, err
	}
	zero, err := repositoryZeroOID(ctx, repo)
	if err != nil {
		return nil, err
	}
	var removed []RefExpectation
	for start := 0; start < len(unique); start += privateRefBatchSize {
		batch := unique[start:min(start+privateRefBatchSize, len(unique))]
		deletes := make(map[string]string, len(batch)-1)
		for _, item := range batch[1:] {
			deletes[item.Ref] = item.OID
		}
		err = withLockedExpectedRefsChanges(ctx, repo, batch[0].Ref, batch[0].OID, "", "", false, timeout,
			func(lockedCtx context.Context) error {
				actual, err := readPrivateRefBatch(lockedCtx, repo, batch)
				if err != nil {
					return err
				}
				for _, item := range batch {
					if actual[item.Ref] != item.OID {
						return ErrCheckpointRefCollision
					}
				}
				return nil
			}, nil, nil, deletes, zero)
		if err != nil {
			// Git may commit before losing its acknowledgement. Probe with a
			// fresh bound; rollback creates only proved absent literal refs.
			probeCtx, cancel := context.WithTimeout(context.Background(), DefaultWriteTimeout)
			for _, item := range batch {
				if directErr := requireDirectPrivateRef(probeCtx, repo, item.Ref); directErr != nil {
					err = errors.Join(err, ErrPrivateRefDeleteOutcomeUnknown, directErr)
					continue
				}
				actual, readErr := RevParse(probeCtx, repo, item.Ref)
				if errors.Is(readErr, ErrRefNotFound) {
					removed = append(removed, item)
				} else if readErr != nil || actual != item.OID {
					err = errors.Join(err, ErrPrivateRefDeleteOutcomeUnknown, readErr)
				}
			}
			cancel()
			return removed, err
		}
		removed = append(removed, batch...)
	}
	return removed, nil
}
