package ai

import "testing"

func TestIntentBinaryMetadataOmitsContentsAndClones(t *testing.T) {
	metadata := &IntentFileMetadata{Kind: "text", BeforeBytes: 10, AfterBytes: 20}
	request, err := NewIntentPlanRequestV2(IntentPlanRequestV2Options{IncludeCapturedDiffs: true, OfferedCaptures: []OfferedCapture{{Seq: 1, Path: "dictionary.custom", Op: "modify", CapturedDiff: "forced text diff\x00binary bytes", FileMetadata: metadata}}})
	if err != nil {
		t.Fatal(err)
	}
	capture := request.OfferedCaptures[0]
	if capture.CapturedDiff != "" || capture.FileMetadata.Kind != "binary" || capture.FileMetadata.DiffOmittedReason != "binary" {
		t.Fatalf("binary content escaped: %+v", capture)
	}
	metadata.AfterBytes = 999
	if capture.FileMetadata.AfterBytes != 20 {
		t.Fatal("metadata aliases caller memory")
	}
	legacy := LegacyIntentPlanRequest(request)
	if legacy.OfferedCaptures[0].FileMetadata != nil || legacy.OfferedCaptures[0].CapturedDiff != "" {
		t.Fatalf("legacy protocol changed or exposed binary contents: %+v", legacy)
	}
}
