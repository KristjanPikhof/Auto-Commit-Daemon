package git

import "testing"

func TestLiteralPathspecPreservesLegalWhitespace(t *testing.T) {
	t.Parallel()
	path := " leading and trailing.txt \n"
	if got, want := LiteralPathspec(path), ":(literal)"+path; got != want {
		t.Fatalf("LiteralPathspec()=%q want %q", got, want)
	}
}
