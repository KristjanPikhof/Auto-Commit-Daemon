package ai

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestIntentSwiftBlankLineMaintenanceProvesExactOperation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		before, after string
	}{
		{"trailing_blank_line", "struct App {}\n\n", "struct App {}\n"},
		{"blank_horizontal_space", "struct App {\n \t \n}\n", "struct App {\n\n}\n"},
		{"combined", "struct App {\n\t \n}\n\n\n", "struct App {\n\n}\n"},
		{"at_blob_cap", strings.Repeat("a", IntentMaintenanceBlobCap-2) + "\n\n", strings.Repeat("a", IntentMaintenanceBlobCap-2) + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof := ProveIntentSwiftBlankLineMaintenance(7, "App.swift", "100644", "100644", []byte(tc.before), []byte(tc.after))
			if proof == nil || !proof.ProvesSwiftBlankLineMaintenance(7, "App.swift") || proof.ProvesSwiftBlankLineMaintenance(8, "App.swift") || proof.ProvesSwiftBlankLineMaintenance(7, "Other.swift") {
				t.Fatalf("exact capture proof not bound: %+v", proof)
			}
			copy := *proof
			if !copy.ProvesSwiftBlankLineMaintenance(7, "App.swift") {
				t.Fatal("metadata copy lost trusted process-local proof")
			}
			copy.AfterBytes++
			if copy.ProvesSwiftBlankLineMaintenance(7, "App.swift") {
				t.Fatal("altered metadata retained proof")
			}
		})
	}
}

func TestIntentSwiftBlankLineMaintenanceRejectsBroaderChanges(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, beforeMode, afterMode string
		before, after                     []byte
	}{
		{"behavior", "App.swift", "100644", "100644", []byte("let enabled = false\n\n"), []byte("let enabled = true\n")},
		{"nonblank_indent", "App.swift", "100644", "100644", []byte("  let enabled = true\n\n"), []byte("let enabled = true\n")},
		{"nonblank_trailing_space", "App.swift", "100644", "100644", []byte("let enabled = true \n\n"), []byte("let enabled = true\n")},
		{"interior_empty_line", "App.swift", "100644", "100644", []byte("struct App {\n\n}\n"), []byte("struct App {\n}\n")},
		{"multiline_string", "App.swift", "100644", "100644", []byte("let text = \"\"\"\n \n\"\"\"\n"), []byte("let text = \"\"\"\n\n\"\"\"\n")},
		{"extended_regex", "App.swift", "100644", "100644", []byte("let pattern = #/foo/#\n\n"), []byte("let pattern = #/foo/#\n")},
		{"crlf", "App.swift", "100644", "100644", []byte("struct App {}\r\n\r\n"), []byte("struct App {}\r\n")},
		{"nul", "App.swift", "100644", "100644", []byte("struct App {}\x00\n\n"), []byte("struct App {}\x00\n")},
		{"invalid_utf8", "App.swift", "100644", "100644", []byte{0xff, '\n', '\n'}, []byte{0xff, '\n'}},
		{"unterminated_after", "App.swift", "100644", "100644", []byte("struct App {}\n\n"), []byte("struct App {}")},
		{"empty_file", "App.swift", "100644", "100644", []byte(" \n\n"), []byte("\n")},
		{"unchanged", "App.swift", "100644", "100644", []byte("struct App {}\n"), []byte("struct App {}\n")},
		{"other_language", "App.py", "100644", "100644", []byte("App = 1\n\n"), []byte("App = 1\n")},
		{"mode_change", "App.swift", "100644", "100755", []byte("struct App {}\n\n"), []byte("struct App {}\n")},
		{"symlink", "App.swift", "120000", "120000", []byte("struct App {}\n\n"), []byte("struct App {}\n")},
		{"oversized", "App.swift", "100644", "100644", append(bytes.Repeat([]byte("a"), IntentMaintenanceBlobCap-1), '\n', '\n'), append(bytes.Repeat([]byte("a"), IntentMaintenanceBlobCap-1), '\n')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if proof := ProveIntentSwiftBlankLineMaintenance(1, tc.path, tc.beforeMode, tc.afterMode, tc.before, tc.after); proof != nil {
				t.Fatalf("broader change supplied maintenance proof: %+v", proof)
			}
		})
	}
}

func TestIntentSwiftBlankLineMaintenanceCannotComeFromJSON(t *testing.T) {
	t.Parallel()
	proof := ProveIntentSwiftBlankLineMaintenance(3, "App.swift", "100644", "100644", []byte("struct App {}\n\n"), []byte("struct App {}\n"))
	raw, err := json.Marshal(proof)
	if err != nil || strings.Contains(string(raw), "swiftBlankLines") || strings.Contains(string(raw), "App.swift") {
		t.Fatalf("private evidence leaked into provider JSON: %s err=%v", raw, err)
	}
	var decoded IntentFileMetadata
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.ProvesSwiftBlankLineMaintenance(3, "App.swift") {
		t.Fatalf("wire metadata gained proof: %+v err=%v", decoded, err)
	}
	if err := json.Unmarshal([]byte(`{"kind":"text","before_bytes":15,"after_bytes":14,"swiftBlankLines":{"seq":3,"path":"App.swift","before":15,"after":14}}`), &decoded); err != nil || decoded.ProvesSwiftBlankLineMaintenance(3, "App.swift") {
		t.Fatalf("provider forged private proof: %+v err=%v", decoded, err)
	}
}
