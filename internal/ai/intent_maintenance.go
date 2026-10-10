package ai

import (
	"bytes"
	"path"
	"unicode/utf8"
)

const IntentMaintenanceBlobCap = 256 << 10

// The proof is process-local and cannot be supplied by provider JSON. It binds
// the bounded, complete blob comparison to the capture that was inspected.
type intentSwiftBlankLineProof struct {
	seq    int64
	path   string
	before int64
	after  int64
}

// ProveIntentSwiftBlankLineMaintenance accepts one deliberately narrow source
// normalization. Nonblank lines remain byte-identical; multiline literals and
// ambiguous line endings stay on the ordinary semantic planning path.
func ProveIntentSwiftBlankLineMaintenance(seq int64, sourcePath, beforeMode, afterMode string, before, after []byte) *IntentFileMetadata {
	if seq <= 0 || path.Ext(sourcePath) != ".swift" || beforeMode != "100644" || afterMode != beforeMode ||
		len(before) == 0 || len(after) == 0 || len(before) > IntentMaintenanceBlobCap || len(after) > IntentMaintenanceBlobCap ||
		bytes.Equal(before, after) || !bytes.HasSuffix(before, []byte("\n")) || !bytes.HasSuffix(after, []byte("\n")) ||
		!utf8.Valid(before) || !utf8.Valid(after) || len(bytes.TrimSpace(before)) == 0 {
		return nil
	}
	for _, contents := range [][]byte{before, after} {
		for _, delimiter := range [][]byte{{0}, {'\r'}, []byte(`"""`), []byte("#/")} {
			if bytes.Contains(contents, delimiter) {
				return nil
			}
		}
	}
	lines := bytes.Split(before, []byte("\n"))
	for i := range lines[:len(lines)-1] {
		if len(bytes.Trim(lines[i], " \t")) == 0 {
			lines[i] = nil
		}
	}
	for len(lines) > 2 && len(lines[len(lines)-1]) == 0 && len(lines[len(lines)-2]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if !bytes.Equal(bytes.Join(lines, []byte("\n")), after) {
		return nil
	}
	return &IntentFileMetadata{
		Kind: "text", BeforeBytes: int64(len(before)), AfterBytes: int64(len(after)),
		swiftBlankLines: &intentSwiftBlankLineProof{seq: seq, path: sourcePath, before: int64(len(before)), after: int64(len(after))},
	}
}

func (metadata *IntentFileMetadata) ProvesSwiftBlankLineMaintenance(seq int64, sourcePath string) bool {
	return metadata != nil && metadata.Kind == "text" && metadata.swiftBlankLines != nil &&
		metadata.swiftBlankLines.seq == seq && metadata.swiftBlankLines.path == sourcePath &&
		metadata.swiftBlankLines.before == metadata.BeforeBytes && metadata.swiftBlankLines.after == metadata.AfterBytes
}
