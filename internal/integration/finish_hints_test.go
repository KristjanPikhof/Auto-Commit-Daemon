package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestFinishIntegrationHintsMergeAndUninstallPreserveCustom(t *testing.T) {
	for name := range templateFiles {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			plans, err := BuildPlans(paths.Roots{}, name)
			if err != nil || len(plans) != 1 {
				t.Fatalf("initial plan: %v %+v", err, plans)
			}
			plan := plans[0]
			custom := []byte("# custom settings\n")
			if plan.Format == "json" {
				custom = []byte(`{"custom":"keep","hooks":{"customEvent":[{"command":"echo keep"}]}}`)
			}
			if err := os.MkdirAll(filepath.Dir(plan.Target), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(plan.Target, custom, 0600); err != nil {
				t.Fatal(err)
			}
			plans, err = BuildPlans(paths.Roots{}, name)
			if err != nil {
				t.Fatal(err)
			}
			plan = plans[0]
			for _, action := range []string{"acd finish", "acd commit-all", "logical_boundary", "soft_boundary", "acd flush"} {
				if strings.Contains(string(plan.Content), action) {
					t.Fatalf("installed hook includes publication action %q", action)
				}
			}
			if err := os.WriteFile(plan.Target, plan.Content, 0600); err != nil {
				t.Fatal(err)
			}
			ownership := filepath.Join(t.TempDir(), "ownership.json")
			if err := WriteOwnership(ownership, plans); err != nil {
				t.Fatal(err)
			}
			again, err := BuildPlans(paths.Roots{}, name)
			if err != nil || again[0].Changed {
				t.Fatalf("same template was not idempotent: %v %+v", err, again)
			}
			removal, err := BuildRemovalPlans(ownership)
			if err != nil || len(removal) != 1 {
				t.Fatalf("uninstall: %v %+v", err, removal)
			}
			body := string(removal[0].Content)
			if strings.Contains(body, "acd internal") || !strings.Contains(body, "custom") {
				t.Fatalf("uninstall lost custom settings or retained hooks: %s", body)
			}
		})
	}
}
