package state

import "testing"

func TestLegacyEvidenceDigestSeparatesCurrentCheckpoints(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "current"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			db, path := openTestDB(t)
			if legacy {
				if _, err := db.SQL().ExecContext(t.Context(), LegacySchemaDDL); err != nil {
					t.Fatal(err)
				}
			}
			before, err := ReadLegacyEvidenceDigest(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			finishFixture(t, db, 1)
			after, err := ReadLegacyEvidenceDigest(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			if (before != after) != legacy {
				t.Fatalf("checkpoint changed digest=%t, legacy schema=%t", before != after, legacy)
			}
		})
	}
}
