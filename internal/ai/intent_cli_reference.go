package ai

import (
	"path"
	"strings"
)

// This process-local proof is attached only after the host reads complete,
// immutable source blobs and proves a qualified command registration.
type intentCLIReferenceProof struct {
	seq        int64
	path       string
	after      int64
	references map[string]string
}

func WithIntentCLIReferenceProof(metadata *IntentFileMetadata, seq int64, sourcePath string, bytes int64, references map[string]string) *IntentFileMetadata {
	if seq <= 0 || path.Ext(sourcePath) != ".go" || strings.HasSuffix(sourcePath, "_test.go") || bytes <= 0 || bytes > IntentMaintenanceBlobCap || len(references) == 0 || len(references) > 32 {
		return metadata
	}
	copy := IntentFileMetadata{Kind: "text", AfterBytes: bytes}
	if metadata != nil {
		copy = *metadata
	}
	if copy.Kind != "text" {
		return metadata
	}
	bounded := make(map[string]string, len(references))
	for command, digest := range references {
		if len(command) == 0 || len(command) > 256 || !strings.HasPrefix(digest, "sha256:") || len(digest) != 39 {
			return metadata
		}
		bounded[command] = digest
	}
	copy.cliReferences = &intentCLIReferenceProof{seq: seq, path: sourcePath, after: copy.AfterBytes, references: bounded}
	return &copy
}

func (metadata *IntentFileMetadata) IntentCLIReferences(seq int64, sourcePath string) map[string]string {
	if metadata == nil || metadata.Kind != "text" || metadata.cliReferences == nil || metadata.cliReferences.seq != seq || metadata.cliReferences.path != sourcePath || metadata.cliReferences.after != metadata.AfterBytes {
		return nil
	}
	copy := make(map[string]string, len(metadata.cliReferences.references))
	for command, digest := range metadata.cliReferences.references {
		copy[command] = digest
	}
	return copy
}
