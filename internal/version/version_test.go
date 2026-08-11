package version

import "testing"

func TestCompareGitDescribeVersions(t *testing.T) {
	tests := []struct {
		left, right string
		want        int
		comparable  bool
	}{
		{"v2026-08-08", "v2026-08-07-999-gabcdef0", 1, true},
		{"v2026-08-07-180-gabcdef0", "v2026-08-07-179-g1234567", 1, true},
		{"v2026-08-07-179-gabcdef0-dirty", "v2026-08-07-179-gabcdef0", 1, true},
		{"dev (unknown)", "v2026-08-07", 0, false},
	}
	for _, test := range tests {
		got, comparable := Compare(test.left, test.right)
		if got != test.want || comparable != test.comparable {
			t.Errorf("Compare(%q, %q)=(%d,%v), want (%d,%v)", test.left, test.right, got, comparable, test.want, test.comparable)
		}
	}
}
