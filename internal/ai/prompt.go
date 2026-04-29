// prompt.go — commit-context shape, diff truncation, and message sanitization
// shared by every Provider in this package. See plan §10.1-10.2.
//
// CommitContext carries the per-event facts a provider needs to produce a
// commit message; the deterministic provider only consults Op + Path +
// OldPath + MultiOp, while openai-compat additionally serializes DiffText,
// Branch, RepoRoot, and Commits into the request payload.
//
// SanitizeMessage ports the legacy Python `sanitize_message`. Behavioural
// notes vs the legacy:
//   - subject is stripped of leading bullet/whitespace markers, trimmed,
//     and any trailing periods removed (legacy: `rstrip(".")` once);
//   - the subject cap is 72 chars (task brief — the legacy used 50 because
//     snapshot-worker followed the kernel-style hard cap; v1 widens this
//     to fit modern commit-log conventions while still truncating with `…`);
//   - body bullets are normalized (single `-` prefix, hanging indent for
//     wrap continuations) and re-wrapped at 72 chars.
//
// The truncation helper preserves the diff header(s) when chopping so a
// downstream model still sees which file the diff describes; the budget
// is the literal byte cap from the brief (4000 chars).
package ai

import (
	"regexp"
	"strings"
	"time"
)

// CommitContext is the input every Provider consumes. Fields are immutable
// once handed to Generate; providers must not mutate the struct.
type CommitContext struct {
	Path     string    // primary path (event-level path)
	Op       string    // "create" | "modify" | "delete" | "rename" | "mode"
	OldPath  string    // for rename
	DiffText string    // unified diff; caller is expected to cap via Truncate
	RepoRoot string    // absolute path to the worktree root
	Branch   string    // canonical branch ref (e.g. refs/heads/main)
	Commits  []string  // recent parent commit subjects for additional context
	MultiOp  []OpItem  // present when one event carries > 1 ops
	Now      time.Time // injected clock for deterministic tests
}

// OpItem is one entry of CommitContext.MultiOp. Mirrors the per-op subset
// the provider needs to enumerate the bullet body.
type OpItem struct {
	Path    string
	Op      string
	OldPath string
}

// Result is what a Provider returns. Subject is the first commit-message
// line; Body is everything after the blank line (may be empty). Source
// records which provider satisfied the request — Compose stamps the
// effective source so callers can tell whether the primary or fallback
// fired.
type Result struct {
	Subject string
	Body    string
	Source  string
}

// SubjectCap is the maximum subject length any sanitized commit message
// will reach. Anything longer is truncated at a word boundary with `…`.
const SubjectCap = 72

// BodyWrap is the line width used when re-wrapping bullet bodies.
const BodyWrap = 72

// DiffCap is the byte cap callers should apply after RedactDiffSecrets before
// handing a unified diff to a network-bound provider.
const DiffCap = 4000

var (
	// reBulletPrefix strips a leading `-` / `*` plus whitespace from a
	// subject candidate (e.g. when the model returned a bulleted line).
	reBulletPrefix = regexp.MustCompile(`^[\-\*\s]+`)
	// reBulletStart matches a body line that starts a new bullet.
	reBulletStart = regexp.MustCompile(`^[\-\*]\s+`)
	// reControl strips ASCII control chars (except tab/newline) so a
	// hostile model response cannot inject terminal escape sequences
	// into the commit log.
	reControl = regexp.MustCompile(`[\x00-\x08\x0B-\x1F\x7F]`)
)

// Truncate caps a unified diff to `max` bytes. Mirrors the legacy 4000-char
// cutoff; tries to preserve the header block at the top so a downstream
// model still sees the file paths it is reasoning about.
//
// When the diff is shorter than `max` the original is returned unchanged.
// When truncation kicks in we keep the full leading header lines (every
// run of `diff --git`, `index`, `---`, `+++`, `@@` lines from the start)
// up to `max/2` chars, then append as much of the trailing diff body as
// remaining budget allows, joined by a `... <truncated> ...` sentinel.
func Truncate(diff string, max int) string {
	if max <= 0 || len(diff) <= max {
		return diff
	}
	const sentinel = "\n... <truncated> ...\n"
	headerBudget := max / 2
	tailBudget := max - headerBudget - len(sentinel)
	if tailBudget < 0 {
		// Pathological tiny budget — fall back to a hard prefix cut.
		return diff[:max]
	}

	// Walk header lines: anything that looks like git diff metadata stays
	// in the header until we exceed headerBudget.
	var header strings.Builder
	rem := diff
	for {
		nl := strings.IndexByte(rem, '\n')
		if nl < 0 {
			break
		}
		line := rem[:nl+1]
		if !isDiffHeaderLine(line) {
			break
		}
		if header.Len()+len(line) > headerBudget {
			break
		}
		header.WriteString(line)
		rem = rem[nl+1:]
	}

	// Trailing body: take the last tailBudget bytes from the original
	// diff (so the most recent change context is preserved). Skip past
	// the next newline so we don't start mid-line.
	tail := diff
	if len(tail) > tailBudget {
		tail = tail[len(tail)-tailBudget:]
		if i := strings.IndexByte(tail, '\n'); i >= 0 && i+1 < len(tail) {
			tail = tail[i+1:]
		}
	}

	var out strings.Builder
	out.Grow(max)
	out.WriteString(header.String())
	out.WriteString(sentinel)
	out.WriteString(tail)
	return out.String()
}

// isDiffHeaderLine is true for the canonical `git diff` header prefixes.
func isDiffHeaderLine(line string) bool {
	switch {
	case strings.HasPrefix(line, "diff --git"),
		strings.HasPrefix(line, "index "),
		strings.HasPrefix(line, "--- "),
		strings.HasPrefix(line, "+++ "),
		strings.HasPrefix(line, "@@ "),
		strings.HasPrefix(line, "new file mode"),
		strings.HasPrefix(line, "deleted file mode"),
		strings.HasPrefix(line, "old mode"),
		strings.HasPrefix(line, "new mode"),
		strings.HasPrefix(line, "rename from"),
		strings.HasPrefix(line, "rename to"),
		strings.HasPrefix(line, "similarity index"),
		strings.HasPrefix(line, "Binary files "):
		return true
	}
	return false
}

// SanitizeMessage normalizes a model- or command-produced commit message.
//
// Behaviour is ported from the legacy Python `sanitize_message`:
//  1. control chars are stripped;
//  2. blank lines are dropped, then the first surviving line becomes the
//     subject after bullet/period scrubbing and the SubjectCap word-
//     boundary truncation;
//  3. remaining lines are folded into bullets — a leading `-`/`*` opens
//     a new bullet, anything else is appended to the running bullet;
//  4. each bullet is re-wrapped at BodyWrap with a `- ` prefix and
//     two-space hanging indent.
//
// An empty input yields the safe placeholder `"Update files"` so callers
// never have to handle "" on top of fallback logic.
func SanitizeMessage(text string) string {
	text = reControl.ReplaceAllString(text, "")
	rawLines := strings.Split(text, "\n")
	var lines []string
	for _, l := range rawLines {
		l = strings.TrimRight(l, " \t\r")
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, l)
	}
	if len(lines) == 0 {
		return "Update files"
	}

	subject := reBulletPrefix.ReplaceAllString(lines[0], "")
	subject = strings.TrimSpace(subject)
	subject = strings.TrimRight(subject, ".")
	if subject == "" {
		subject = "Update files"
	}
	subject = trimSubject(subject, SubjectCap)

	var bullets []string
	var current string
	for _, line := range lines[1:] {
		stripped := strings.TrimSpace(line)
		if stripped == "" {
			continue
		}
		if reBulletStart.MatchString(stripped) {
			if current != "" {
				bullets = append(bullets, current)
			}
			current = strings.TrimSpace(reBulletPrefix.ReplaceAllString(stripped, ""))
		} else if current != "" {
			current = strings.TrimSpace(current + " " + stripped)
		} else {
			current = stripped
		}
	}
	if current != "" {
		bullets = append(bullets, current)
	}

	if len(bullets) == 0 {
		return subject
	}

	var wrapped []string
	for _, b := range bullets {
		wrapped = append(wrapped, wrapBullet(b, BodyWrap)...)
	}
	return subject + "\n\n" + strings.Join(wrapped, "\n")
}

// trimSubject mirrors the legacy `_trim_subject`: collapse to `limit`
// chars on a word boundary, fall back to a hard cut + ellipsis when no
// boundary lies in the second half of the budget.
func trimSubject(subject string, limit int) string {
	subject = strings.TrimSpace(subject)
	if len([]rune(subject)) <= limit {
		return subject
	}
	runes := []rune(subject)
	head := runes[:limit-1]
	boundary := -1
	for _, ch := range []rune{' ', '/', '.'} {
		if i := lastIndexRune(head, ch); i > boundary {
			boundary = i
		}
	}
	if boundary >= limit/2 {
		return strings.TrimRight(string(head[:boundary]), " /.") + "…"
	}
	return strings.TrimRight(string(head), " ") + "…"
}

func lastIndexRune(rs []rune, r rune) int {
	for i := len(rs) - 1; i >= 0; i-- {
		if rs[i] == r {
			return i
		}
	}
	return -1
}

// wrapBullet renders one bullet body as a sequence of width-capped lines:
// the first line carries `- `, continuation lines a two-space indent. We
// never break mid-token; if a single token exceeds the budget it lands on
// its own line, slightly over-wide but readable. Mirrors the legacy
// textwrap.wrap(initial_indent="- ", subsequent_indent="  ").
func wrapBullet(body string, width int) []string {
	const initial = "- "
	const subsequent = "  "
	words := strings.Fields(body)
	if len(words) == 0 {
		return []string{initial}
	}
	var out []string
	prefix := initial
	cur := prefix + words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			out = append(out, cur)
			prefix = subsequent
			cur = prefix + w
			continue
		}
		cur = cur + " " + w
	}
	out = append(out, cur)
	return out
}
