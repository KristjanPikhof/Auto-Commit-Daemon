package daemon

import "testing"

func TestClassifyRenameTargetIsPairedOnce(t *testing.T) {
	shadow := map[string]ShadowEntry{
		"old-a.txt": {Path: "old-a.txt", Mode: "100644", OID: "same"},
		"old-b.txt": {Path: "old-b.txt", Mode: "100644", OID: "same"},
	}
	live := map[string]LiveEntry{
		"new.txt": {Path: "new.txt", Mode: "100644", OID: "same"},
	}

	got := Classify(shadow, live)
	if len(got) != 2 {
		t.Fatalf("Classify returned %d ops, want 2: %+v", len(got), got)
	}
	if got[0].Op != "rename" || got[0].OldPath != "old-a.txt" || got[0].Path != "new.txt" {
		t.Fatalf("first op = %+v, want old-a.txt renamed to new.txt", got[0])
	}
	if got[1].Op != "delete" || got[1].Path != "old-b.txt" {
		t.Fatalf("second op = %+v, want old-b.txt deleted", got[1])
	}
}
