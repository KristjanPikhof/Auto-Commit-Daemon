package git

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const CheckpointRefPrefix = "refs/acd/checkpoints/v1/"

var ErrCheckpointRefCollision = errors.New("git: checkpoint ref collision")

// BlobHasher computes the object ID Git assigns to raw blob content. Keeping
// this tiny value in the git package lets complete worktree scans avoid one
// hash-object subprocess per file while still honoring SHA-1 and SHA-256
// repositories.
type BlobHasher struct {
	newHash func() hash.Hash
}

// DurableBlob is one already-hashed blob that a checkpoint tree will retain.
// Content is required only when the object does not already exist in Git.
type DurableBlob struct {
	OID     string
	Content []byte
}

// NewBlobHasher resolves the repository's object format without mutating it.
func NewBlobHasher(ctx context.Context, repoDir string) (BlobHasher, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		"rev-parse", "--show-object-format")
	if err != nil {
		return BlobHasher{}, fmt.Errorf("git: resolve object format: %w", err)
	}
	switch strings.TrimSpace(string(out)) {
	case "sha1":
		return BlobHasher{newHash: sha1.New}, nil
	case "sha256":
		return BlobHasher{newHash: sha256.New}, nil
	default:
		return BlobHasher{}, fmt.Errorf("git: unsupported object format %q", strings.TrimSpace(string(out)))
	}
}

// BlobOID returns the exact Git object ID for content without writing it.
func (h BlobHasher) BlobOID(content []byte) (string, error) {
	if h.newHash == nil {
		return "", errors.New("git: uninitialized blob hasher")
	}
	digest := h.newHash()
	_, _ = io.WriteString(digest, "blob "+strconv.Itoa(len(content)))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(content)
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// EnsureBlobObjectsDurable proves every supplied blob exists with the exact
// type, writing and fsyncing only objects Git does not already have. A normal
// checkpoint scan mostly reuses blobs already retained by HEAD or the index,
// so one bounded batch reread replaces thousands of per-file subprocesses.
func EnsureBlobObjectsDurable(ctx context.Context, repoDir string, blobs []DurableBlob) error {
	unique := make([]DurableBlob, 0, len(blobs))
	seen := make(map[string]struct{}, len(blobs))
	for _, blob := range blobs {
		if blob.OID == "" {
			return errors.New("git: durable blob has an empty object ID")
		}
		if _, ok := seen[blob.OID]; ok {
			continue
		}
		seen[blob.OID] = struct{}{}
		unique = append(unique, blob)
	}
	if len(unique) == 0 {
		return nil
	}

	var input bytes.Buffer
	for _, blob := range unique {
		input.WriteString(blob.OID)
		input.WriteByte('\n')
	}
	out, err := Run(ctx, RunOpts{
		Dir: repoDir, Stdin: &input, Timeout: DefaultReadTimeout,
	}, "cat-file", "--batch-check=%(objectname) %(objecttype)")
	if err != nil {
		return fmt.Errorf("git: batch reread durable blobs: %w", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for index, blob := range unique {
		if !scanner.Scan() {
			if scanErr := scanner.Err(); scanErr != nil {
				return fmt.Errorf("git: batch reread durable blobs: %w", scanErr)
			}
			return fmt.Errorf("git: batch reread durable blobs returned %d of %d rows", index, len(unique))
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || fields[0] != blob.OID {
			return errors.New("git: malformed durable blob batch response")
		}
		switch fields[1] {
		case "blob":
			continue
		case "missing":
			written, writeErr := HashObjectStdinDurable(ctx, repoDir, blob.Content)
			if writeErr != nil {
				return writeErr
			}
			if written != blob.OID {
				return fmt.Errorf("git: durable blob wrote %s, want %s", written, blob.OID)
			}
		default:
			return fmt.Errorf("git: durable object %s has type %s, want blob", blob.OID, fields[1])
		}
	}
	if scanner.Scan() {
		return errors.New("git: durable blob batch returned extra rows")
	}
	return scanner.Err()
}

// DurabilitySupport validates the Git configuration knobs ACD relies on for
// checkpoint object and reference fsync. It performs no repository writes.
func DurabilitySupport(ctx context.Context, repoDir string) error {
	for _, key := range []string{"core.fsync", "core.fsyncMethod"} {
		if _, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
			"-c", "core.fsync=loose-object,reference",
			"-c", "core.fsyncMethod=fsync",
			"config", "--get", key); err == nil {
			// `git config --get` returns the temporary -c value. Continue so
			// both keys are proven accepted by this Git binary.
			continue
		} else {
			return fmt.Errorf("git: required durability setting %s is unsupported: %w", key, err)
		}
	}
	return nil
}

// HashObjectStdinDurable writes one blob with Git's loose-object fsync mode
// enabled and rereads its exact type before returning.
func HashObjectStdinDurable(ctx context.Context, repoDir string, content []byte) (string, error) {
	out, err := runDurable(ctx, repoDir, bytes.NewReader(content), "loose-object",
		"hash-object", "-w", "--stdin")
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if err := verifyObjectType(ctx, repoDir, oid, "blob"); err != nil {
		return "", err
	}
	return oid, nil
}

// WriteTreeDurable builds a complete tree through an isolated scratch index.
// The caller supplies already-written blob OIDs. The live index and working
// tree are never read or mutated.
func WriteTreeDurable(ctx context.Context, repoDir, indexFile string, entries []IndexEntry) (string, error) {
	if strings.TrimSpace(indexFile) == "" {
		return "", errors.New("git: durable tree requires an isolated index path")
	}
	if err := os.MkdirAll(filepath.Dir(indexFile), 0o700); err != nil {
		return "", fmt.Errorf("git: create durable index directory: %w", err)
	}
	if err := os.Remove(indexFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("git: clear durable index: %w", err)
	}
	defer os.Remove(indexFile)
	extra := map[string]string{"GIT_INDEX_FILE": indexFile}
	if _, err := runDurableWithEnv(ctx, repoDir, nil, extra, "loose-object,index",
		"read-tree", "--empty"); err != nil {
		return "", fmt.Errorf("git: initialize durable index: %w", err)
	}
	if len(entries) > 0 {
		var input bytes.Buffer
		for _, entry := range entries {
			if entry.Stage != 0 || entry.Mode == "" || entry.OID == "" || entry.Path == "" {
				return "", fmt.Errorf("git: invalid durable tree entry %q", entry.Path)
			}
			fmt.Fprintf(&input, "%s %s\t%s\x00", entry.Mode, entry.OID, entry.Path)
		}
		if _, err := runDurableWithEnv(ctx, repoDir, &input, extra, "loose-object,index",
			"update-index", "-z", "--index-info"); err != nil {
			return "", fmt.Errorf("git: populate durable index: %w", err)
		}
	}
	out, err := runDurableWithEnv(ctx, repoDir, nil, extra, "loose-object,index",
		"write-tree")
	if err != nil {
		return "", fmt.Errorf("git: write durable tree: %w", err)
	}
	treeOID := strings.TrimSpace(string(out))
	if err := verifyObjectType(ctx, repoDir, treeOID, "tree"); err != nil {
		return "", err
	}
	return treeOID, nil
}

// CommitTreeDurable creates a rootless checkpoint commit with a fixed ACD
// identity and rereads both commit and tree identities before returning.
func CommitTreeDurable(ctx context.Context, repoDir, treeOID, message, name, email string) (string, error) {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(email) == "" {
		return "", errors.New("git: durable commit identity is required")
	}
	extra := map[string]string{
		"GIT_AUTHOR_NAME":     name,
		"GIT_AUTHOR_EMAIL":    email,
		"GIT_COMMITTER_NAME":  name,
		"GIT_COMMITTER_EMAIL": email,
	}
	out, err := runDurableWithEnv(ctx, repoDir, strings.NewReader(message), extra,
		"loose-object", "commit-tree", treeOID, "-F", "-")
	if err != nil {
		return "", err
	}
	commitOID := strings.TrimSpace(string(out))
	if err := verifyObjectType(ctx, repoDir, commitOID, "commit"); err != nil {
		return "", err
	}
	observedTree, err := RevParse(ctx, repoDir, commitOID+"^{tree}")
	if err != nil {
		return "", fmt.Errorf("git: reread durable commit tree: %w", err)
	}
	if observedTree != treeOID {
		return "", fmt.Errorf("git: durable commit tree is %s, want %s", observedTree, treeOID)
	}
	return commitOID, nil
}

// EnsureCheckpointRef performs create-only CAS in the private checkpoint
// namespace. An identical existing target is idempotent; a different target
// is retained and reported as an ambiguity.
func EnsureCheckpointRef(ctx context.Context, repoDir, ref, commitOID string) (created bool, err error) {
	return EnsurePrivateRefDurable(ctx, repoDir, CheckpointRefPrefix, ref, commitOID)
}

// EnsurePrivateRefDurable performs create-only CAS below one explicit ACD
// namespace. Setup's migration bridge uses refs/acd/migration/ while normal
// checkpoint capture remains restricted to CheckpointRefPrefix.
func EnsurePrivateRefDurable(ctx context.Context, repoDir, prefix, ref, commitOID string) (created bool, err error) {
	if prefix != CheckpointRefPrefix && prefix != "refs/acd/migration/" {
		return false, fmt.Errorf("git: unsupported private ref prefix %q", prefix)
	}
	if !strings.HasPrefix(ref, prefix) || len(ref) == len(prefix) {
		return false, fmt.Errorf("git: private ref %q must be below %s", ref, prefix)
	}
	if err := verifyObjectType(ctx, repoDir, commitOID, "commit"); err != nil {
		return false, err
	}
	existing, err := RevParse(ctx, repoDir, ref)
	if err == nil {
		if existing == commitOID {
			return false, nil
		}
		return false, fmt.Errorf("%w: %s points at %s, want %s",
			ErrCheckpointRefCollision, ref, existing, commitOID)
	}
	if !errors.Is(err, ErrRefNotFound) {
		return false, fmt.Errorf("git: resolve checkpoint ref: %w", err)
	}
	zeroOID, err := repositoryZeroOID(ctx, repoDir)
	if err != nil {
		return false, err
	}
	if _, err := runDurable(ctx, repoDir, nil, "reference", "update-ref", "--no-deref", ref, commitOID, zeroOID); err != nil {
		existing, readErr := RevParse(ctx, repoDir, ref)
		if readErr == nil && existing == commitOID {
			return false, nil
		}
		if readErr == nil {
			return false, fmt.Errorf("%w: %s raced to %s, want %s",
				ErrCheckpointRefCollision, ref, existing, commitOID)
		}
		return false, fmt.Errorf("git: create checkpoint ref: %w", err)
	}
	observed, err := RevParse(ctx, repoDir, ref)
	if err != nil {
		return false, fmt.Errorf("git: reread checkpoint ref: %w", err)
	}
	if observed != commitOID {
		return false, fmt.Errorf("%w: %s reread as %s, want %s",
			ErrCheckpointRefCollision, ref, observed, commitOID)
	}
	return true, nil
}

func repositoryZeroOID(ctx context.Context, repoDir string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		"rev-parse", "--show-object-format")
	if err != nil {
		return "", fmt.Errorf("git: resolve object format: %w", err)
	}
	switch strings.TrimSpace(string(out)) {
	case "sha1":
		return strings.Repeat("0", sha1.Size*2), nil
	case "sha256":
		return strings.Repeat("0", sha256.Size*2), nil
	default:
		return "", fmt.Errorf("git: unsupported object format %q", strings.TrimSpace(string(out)))
	}
}

// DeletePrivateRefDurable removes exactly the expected ACD-owned ref target.
// A missing ref is idempotent; a moved ref fails closed.
func DeletePrivateRefDurable(ctx context.Context, repoDir, prefix, ref, expectedOID string) error {
	if prefix != CheckpointRefPrefix && prefix != "refs/acd/migration/" {
		return fmt.Errorf("git: unsupported private ref prefix %q", prefix)
	}
	if !strings.HasPrefix(ref, prefix) || len(ref) == len(prefix) || expectedOID == "" {
		return errors.New("git: invalid private ref deletion")
	}
	observed, err := RevParse(ctx, repoDir, ref)
	if errors.Is(err, ErrRefNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if observed != expectedOID {
		return fmt.Errorf("%w: %s points at %s, want %s", ErrCheckpointRefCollision, ref, observed, expectedOID)
	}
	if _, err := runDurable(ctx, repoDir, nil, "reference", "update-ref", "--no-deref", "-d", ref, expectedOID); err != nil {
		return err
	}
	if observed, err := RevParse(ctx, repoDir, ref); err == nil {
		return fmt.Errorf("git: private ref %s remained at %s", ref, observed)
	} else if !errors.Is(err, ErrRefNotFound) {
		return err
	}
	return nil
}

func runDurable(ctx context.Context, repoDir string, stdin io.Reader, fsync string, args ...string) ([]byte, error) {
	return runDurableWithEnv(ctx, repoDir, stdin, nil, fsync, args...)
}

func runDurableWithEnv(ctx context.Context, repoDir string, stdin io.Reader, extra map[string]string, fsync string, args ...string) ([]byte, error) {
	gitArgs := []string{"-c", "core.fsync=" + fsync, "-c", "core.fsyncMethod=fsync"}
	gitArgs = append(gitArgs, args...)
	return Run(ctx, RunOpts{
		Dir:      repoDir,
		Stdin:    stdin,
		ExtraEnv: extra,
		Timeout:  DefaultWriteTimeout,
	}, gitArgs...)
}

func verifyObjectType(ctx context.Context, repoDir, oid, want string) error {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		"cat-file", "-t", oid)
	if err != nil {
		return fmt.Errorf("git: reread durable %s object %s: %w", want, oid, err)
	}
	if got := strings.TrimSpace(string(out)); got != want {
		return fmt.Errorf("git: durable object %s has type %s, want %s", oid, got, want)
	}
	return nil
}
