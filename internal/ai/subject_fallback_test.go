package ai

import "testing"

// TestDiffAwareSubject_Languages exercises the per-language extractors so
// we know each branch fires for at least one canonical shape.
func TestDiffAwareSubject_Languages(t *testing.T) {
	cases := []struct {
		name string
		op   OpItem
		diff string
		want string
	}{
		{
			name: "go-func",
			op:   OpItem{Op: "modify", Path: "internal/ai/subject_fallback.go"},
			diff: "@@ -1,2 +1,3 @@\n+func DiffAwareSubject(op OpItem, diff string) string {\n",
			want: "Update DiffAwareSubject",
		},
		{
			name: "go-method-with-pointer-receiver",
			op:   OpItem{Op: "modify", Path: "internal/daemon/replay.go"},
			diff: "@@\n+func (c *composed) PlanIntent(ctx context.Context) (IntentPlan, error) {\n",
			want: "Update PlanIntent",
		},
		{
			name: "go-method-with-value-receiver",
			op:   OpItem{Op: "modify", Path: "internal/ai/deterministic.go"},
			diff: "@@\n+func (DeterministicProvider) Name() string { return \"deterministic\" }\n",
			want: "Update Name",
		},
		{
			name: "go-generic-function",
			op:   OpItem{Op: "modify", Path: "internal/ai/foo.go"},
			diff: "@@\n+func Map[T any, U any](xs []T, f func(T) U) []U {\n",
			want: "Update Map",
		},
		{
			name: "ts-function-export",
			op:   OpItem{Op: "modify", Path: "src/auth.ts"},
			diff: "@@\n+export async function refreshToken(req: Request): Promise<Token> {\n",
			want: "Update refreshToken",
		},
		{
			name: "ts-class",
			op:   OpItem{Op: "create", Path: "src/Pricing.tsx"},
			diff: "@@\n+export default class PricingTable extends Component {\n",
			want: "Add PricingTable",
		},
		{
			name: "ts-const-arrow",
			op:   OpItem{Op: "modify", Path: "src/utils.ts"},
			diff: "@@\n+export const formatCurrency = (n: number): string => {\n",
			want: "Update formatCurrency",
		},
		{
			name: "js-function-default",
			op:   OpItem{Op: "modify", Path: "src/index.js"},
			diff: "@@\n+function bootstrap() {\n",
			want: "Update bootstrap",
		},
		{
			name: "python-def",
			op:   OpItem{Op: "modify", Path: "scripts/migrate.py"},
			diff: "@@\n+def run_migration(conn):\n",
			want: "Update run_migration",
		},
		{
			name: "python-async-def",
			op:   OpItem{Op: "modify", Path: "scripts/worker.py"},
			diff: "@@\n+async def consume_events(queue):\n",
			want: "Update consume_events",
		},
		{
			name: "python-class",
			op:   OpItem{Op: "create", Path: "scripts/auth.py"},
			diff: "@@\n+class TokenStore(BaseModel):\n",
			want: "Add TokenStore",
		},
		{
			name: "markdown-h1",
			op:   OpItem{Op: "modify", Path: "docs/intent.md"},
			diff: "@@\n+# Intent Planner Window\n+\n+Forced aging behavior.\n",
			want: "Update Intent Planner Window",
		},
		{
			name: "markdown-h3",
			op:   OpItem{Op: "modify", Path: "docs/intent.md"},
			diff: "@@\n+### Defer limit semantics\n",
			want: "Update Defer limit semantics",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DiffAwareSubject(tc.op, tc.diff)
			if got != tc.want {
				t.Fatalf("subject=%q want %q", got, tc.want)
			}
		})
	}
}

// TestDiffAwareSubject_VerbsByOp pins the create/modify/delete/mode verb
// derivation. Symbol extraction is intentionally skipped (no diff body)
// so the result equals the legacy basename verb form.
func TestDiffAwareSubject_VerbsByOp(t *testing.T) {
	cases := []struct {
		name string
		op   OpItem
		want string
	}{
		{name: "create", op: OpItem{Op: "create", Path: "src/foo.go"}, want: "Add foo.go"},
		{name: "modify", op: OpItem{Op: "modify", Path: "src/foo.go"}, want: "Update foo.go"},
		{name: "delete", op: OpItem{Op: "delete", Path: "src/foo.go"}, want: "Remove foo.go"},
		{name: "mode", op: OpItem{Op: "mode", Path: "src/foo.go"}, want: "Update foo.go"},
		{name: "unknown", op: OpItem{Op: "wat", Path: "src/foo.go"}, want: "Update foo.go"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DiffAwareSubject(tc.op, "")
			if got != tc.want {
				t.Fatalf("subject=%q want %q", got, tc.want)
			}
		})
	}
}

// TestDiffAwareSubject_DeleteOpKeepsVerbWithDiff: even when a diff is
// available, a delete op should still produce "Remove <basename>" because
// the post-image is the empty file and any extracted symbol would be
// misleading. The added-lines extractor only sees `-` lines for deletes,
// so symbol extraction returns "".
func TestDiffAwareSubject_DeleteOpKeepsVerbWithDiff(t *testing.T) {
	op := OpItem{Op: "delete", Path: "src/legacy.go"}
	diff := "@@\n-func Legacy() {}\n"
	if got := DiffAwareSubject(op, diff); got != "Remove legacy.go" {
		t.Fatalf("subject=%q want %q", got, "Remove legacy.go")
	}
}

// TestDiffAwareSubject_RenameUsesLegacy: rename ops bypass the diff-aware
// path entirely because the symbol semantics do not match the rename
// verb. We let singleOpSubject handle the "Rename old to new" form.
func TestDiffAwareSubject_RenameUsesLegacy(t *testing.T) {
	op := OpItem{Op: "rename", Path: "src/new.go", OldPath: "src/old.go"}
	diff := "@@\n+func Whatever() {}\n"
	if got := DiffAwareSubject(op, diff); got != "Rename old.go to new.go" {
		t.Fatalf("subject=%q want %q", got, "Rename old.go to new.go")
	}
}

// TestDiffAwareSubject_UnknownExtensionFallsBack: paths with no recognized
// extension fall back to the basename verb form regardless of diff.
func TestDiffAwareSubject_UnknownExtensionFallsBack(t *testing.T) {
	op := OpItem{Op: "modify", Path: "data/log.csv"}
	diff := "@@\n+id,name\n+1,foo\n"
	if got := DiffAwareSubject(op, diff); got != "Update log.csv" {
		t.Fatalf("subject=%q want %q", got, "Update log.csv")
	}
}

// TestDiffAwareSubject_CreateNoDiff: a create op with no diff content
// should still produce "Add <basename>" (this is the common path when
// a create event has only an after-blob and we choose not to render).
func TestDiffAwareSubject_CreateNoDiff(t *testing.T) {
	op := OpItem{Op: "create", Path: "scripts/init.py"}
	if got := DiffAwareSubject(op, ""); got != "Add init.py" {
		t.Fatalf("subject=%q want %q", got, "Add init.py")
	}
}

// TestDiffAwareSubject_NoMatchableSymbol: a recognized language file with
// a diff that lacks any matchable symbol header (just data lines) falls
// back to the basename verb form.
func TestDiffAwareSubject_NoMatchableSymbol(t *testing.T) {
	op := OpItem{Op: "modify", Path: "src/data.go"}
	diff := "@@\n+\tx := 1\n+\ty := 2\n"
	if got := DiffAwareSubject(op, diff); got != "Update data.go" {
		t.Fatalf("subject=%q want %q", got, "Update data.go")
	}
}

// TestDiffAwareSubject_LineCapBoundsScan ensures we do not chase symbols
// past subjectFallbackLineCap lines. The header is placed past the cap;
// extraction must miss it and return the basename form.
func TestDiffAwareSubject_LineCapBoundsScan(t *testing.T) {
	op := OpItem{Op: "modify", Path: "src/late.go"}
	var b []byte
	b = append(b, "@@\n"...)
	for i := 0; i < subjectFallbackLineCap+10; i++ {
		b = append(b, "+ // padding\n"...)
	}
	b = append(b, "+func Lurking() {}\n"...)
	if got := DiffAwareSubject(op, string(b)); got != "Update late.go" {
		t.Fatalf("subject=%q want %q (line cap should bound the scan)", got, "Update late.go")
	}
}

// TestDiffAwareSubject_DeterministicAcrossCalls confirms the helper is
// pure: identical input must produce identical output every call.
func TestDiffAwareSubject_DeterministicAcrossCalls(t *testing.T) {
	op := OpItem{Op: "modify", Path: "src/foo.go"}
	diff := "@@\n+func Compute(x int) int { return x * 2 }\n"
	first := DiffAwareSubject(op, diff)
	for i := 0; i < 5; i++ {
		got := DiffAwareSubject(op, diff)
		if got != first {
			t.Fatalf("iteration %d: got %q first %q (must be deterministic)", i, got, first)
		}
	}
	if first != "Update Compute" {
		t.Fatalf("first=%q want %q", first, "Update Compute")
	}
}
