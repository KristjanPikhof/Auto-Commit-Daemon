package state

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFinishLegacyImmutableCutoverAndReadOnlyProjection(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	oid := strings.Repeat("a", 40)
	ref := "refs/acd/migration/finish-v1/0123456789abcdef/proof/snapshot"
	m := FinishLegacyManifest{Objects: []FinishLegacyObject{{OID: oid, Type: "blob"}}, Retirements: []FinishLegacyRetirement{{Kind: "drain", ID: "old", PreviousState: "needs_action"}}}
	pins := []FinishLegacyPin{{ObjectOID: oid, ObjectType: "blob", PinRef: ref, PinOID: oid}}
	if _, err := PrepareFinishLegacyCutover(ctx, db, "0123456789abcdef", m, ref, oid, nil); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("missing pin accepted: %v", err)
	}
	saved, err := PrepareFinishLegacyCutover(ctx, db, "0123456789abcdef", m, ref, oid, pins)
	if err != nil {
		t.Fatal(err)
	}
	if saved.State != "pending" || saved.RetiredDrains != 1 {
		t.Fatalf("pending: %+v", saved)
	}
	if _, err = CompleteFinishLegacyCutover(ctx, db, "legacy_ref_moved", "Review retained old branch proof"); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFinishLegacyCutover(ctx, path)
	if err != nil || got.State != "needs_attention" || got.ManifestDigest != saved.ManifestDigest {
		t.Fatalf("read: %+v %v", got, err)
	}
	for _, q := range []string{"DELETE FROM finish_legacy_object_pins", "UPDATE finish_legacy_object_pins SET pin_oid='wrong'", "DELETE FROM finish_legacy_retirements", "UPDATE finish_legacy_cutover SET manifest_json='{}'", "DELETE FROM finish_legacy_cutover"} {
		if _, err = db.SQL().ExecContext(ctx, q); err == nil {
			t.Fatalf("evidence mutated: %s", q)
		}
	}
	if _, err = CompleteFinishLegacyCutover(ctx, db, "", ""); err != nil {
		t.Fatal(err)
	}
	again, err := PrepareFinishLegacyCutover(ctx, db, "0123456789abcdef", m, ref, oid, pins)
	if err != nil || again.State != "completed" {
		t.Fatalf("reconnect: %+v %v", again, err)
	}
	m.Objects = nil
	if _, err = PrepareFinishLegacyCutover(ctx, db, "0123456789abcdef", m, ref, oid, nil); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("identity changed: %v", err)
	}
}

func TestFinishLegacyReadOlderSchemaDoesNotUpgrade(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=30"); err != nil {
		t.Fatal(err)
	}
	r, err := ReadFinishLegacyCutover(ctx, path)
	if err != nil || r.Available {
		t.Fatalf("legacy read %+v %v", r, err)
	}
	version, err := ReadUserVersion(ctx, path)
	if err != nil || version != 30 {
		t.Fatalf("read upgraded %d %v", version, err)
	}
	if _, err = db.SQL().ExecContext(ctx, "PRAGMA user_version=34"); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadFinishLegacyCutover(ctx, path); !errors.Is(err, ErrSetupRequired) {
		t.Fatalf("future schema accepted: %v", err)
	}
}
