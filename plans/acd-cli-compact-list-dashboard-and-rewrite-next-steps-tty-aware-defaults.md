---
epic_id: c095b960-c9bc-48c4-a4ef-d1bb740e18bb
schema_version: 1
exported_at: 2026-05-27T16:34:22.089Z
status: in_progress
---

# ACD CLI: compact list dashboard and rewrite next-steps (TTY-aware defaults)

## Summary

| Metric | Count |
|--------|-------|
| Tasks | 7 |
| Subtasks | 15 |
| Dependencies | 6 |
| External nodes | 0 |
| Warnings | 0 |

### Task status breakdown

| Status | Count |
|--------|-------|
| todo | 1 |
| done | 6 |

### Subtask status breakdown

| Status | Count |
|--------|-------|
| todo | 15 |

## Description

Goal: Ship accepted brainstorm UX for acd list and rewrite-commits plan-only flow.

In scope:
- acd list: TTY defaults to live watch with compact table (last-two-segment REPO e.g. Development/Auto-Commit-Daemon, short STATUS, no CLIENTS column).
- acd list: non-TTY and --once emit one-shot compact; --verbose restores homeShort path, CLIENTS, LAST_COMMIT, status notes.
- rewrite-commits: plan-only ends with Plan saved plus Next block (show-plan, apply-plan dry-run, apply-plan --yes).
- Docs and CHANGELOG for breaking interactive list default.

Out of scope: JSON schema changes, daemon behavior, planner logic.

Success: list_test and rewrite_commits_test pass; cleanenv make lint and make test; operator can run acd list (watch compact), acd list --once --verbose (old snapshot), rewrite --plan-only and see apply commands.

Risks: last-two-segment path collisions across repos; scripts piping acd list must use non-TTY or --once.

Verification gates: go test ./internal/cli/... -run 'List|Rewrite' -race -count=1; cleanenv make lint; README and docs/rewrite-commits.md updated.

## Task index

| # | Title | Status | Subtasks |
|---|-------|--------|----------|
| 1 | [\[Verify\] Run pre-PR gates for CLI UX epic](#task-00fb3254-312b-4b4e-8be8-66712a686f5d) | todo | 0 |
| 2 | [\[CLI\] Extend list tests for new defaults and flags](#task-4f352a81-fd1a-4bae-bc24-79fd32c7ffd5) | done | 2 |
| 3 | [\[Docs\] Update operator docs for list and rewrite flows](#task-6a04eafe-78b4-4a20-a8c6-7be0983e803d) | done | 3 |
| 4 | [\[CLI\] Add rewrite-commits plan-only Next steps footer](#task-7668298f-0743-4e1b-8501-54185c72db67) | done | 3 |
| 5 | [\[CLI\] Implement compact and verbose acd list table renderers](#task-7d6a1715-9c5f-43c4-a561-60d335522d39) | done | 4 |
| 6 | [\[CLI\] Test rewrite-commits Next footer output](#task-c3bf5116-5168-4c45-a4b7-9586135f7409) | done | 0 |
| 7 | [\[CLI\] Default acd list to watch on TTY with --once guard](#task-c462604e-4ff8-479d-b368-aa1e3b96f5b7) | done | 3 |

## Tasks

### [Verify] Run pre-PR gates for CLI UX epic

**ID:** `00fb3254-312b-4b4e-8be8-66712a686f5d`  
**Status:** todo  

Target: whole repo CLI scope. Acceptance: cleanenv make lint; cleanenv make test; cleanenv go test ./internal/cli/... -race -count=3. Owner: verify-lane. Blocked by docs.

**Blocked by:**
- `6a04eafe-78b4-4a20-a8c6-7be0983e803d`

### [CLI] Extend list tests for new defaults and flags

**ID:** `4f352a81-fd1a-4bae-bc24-79fd32c7ffd5`  
**Status:** done  

Target: internal/cli/list_test.go. Acceptance: tests cover compact default output, verbose wide output, --once one-shot, missing row format, waiting without fix in note. Verify: go test ./internal/cli/... -run List -race -count=3. Owner: cli-lane. Blocked by list-watch.
Compact REPO column expects last-two path segments (Development/Auto-Commit-Daemon), not single basename.

**Blocked by:**
- `c462604e-4ff8-479d-b368-aa1e3b96f5b7`

**Blocks:**
- `6a04eafe-78b4-4a20-a8c6-7be0983e803d`

#### Subtasks

- [ ] **Update existing list tests for compact default** — `af36c89f-fccb-4d2e-bfcc-a506a04c9413` (todo)
  Fix TestList_HumanOneShotOutput and related tests to expect compact headers or pass verbose flag where full layout required.
- [ ] **Add tests for verbose once and last-two-segment REPO** — `ddb770bf-5321-463a-9bd4-2ec93097a3f7` (todo)
  Add cases for --verbose paths, last-two-segment collision suffix, and non-watch once snapshot.
### [Docs] Update operator docs for list and rewrite flows

**ID:** `6a04eafe-78b4-4a20-a8c6-7be0983e803d`  
**Status:** done  

Target: README.md, CHANGELOG.md, docs/rewrite-commits.md, CLAUDE.md if CLI contract changed. Acceptance: quick start three-step rewrite; list documents watch default --once --verbose; breaking change noted. Owner: docs-lane. Blocked by list-tests and rewrite-tests.
Document compact REPO as last-two path segments; verbose as homeShort full path.

**Blocked by:**
- `c3bf5116-5168-4c45-a4b7-9586135f7409`
- `4f352a81-fd1a-4bae-bc24-79fd32c7ffd5`

**Blocks:**
- `00fb3254-312b-4b4e-8be8-66712a686f5d`

#### Subtasks

- [ ] **Document breaking acd list default** — `79e918cc-2c2b-4d5c-9b80-9f20f1ba534d` (todo)
  CHANGELOG entry for watch-by-default and compact table.
- [ ] **Update README list and rewrite examples** — `8c99106b-94cc-437f-bf46-c7b7b86def3e` (todo)
  Swap acd list examples; add rewrite plan-only next steps pointer.
- [ ] **Add rewrite-commits.md quick start section** — `9acdef44-1606-44f2-8800-39bf43db3442` (todo)
  Top of docs/rewrite-commits.md with plan dry-run apply; trim duplication pointer to intent-commit-rewrite-flow.
### [CLI] Add rewrite-commits plan-only Next steps footer

**ID:** `7668298f-0743-4e1b-8501-54185c72db67`  
**Status:** done  
**Owner:** rewrite-footer-agent  

Target: internal/cli/rewrite_commits.go. Acceptance: valid --plan-only prints Plan saved git history unchanged and Next block with show-plan apply-plan dry-run and yes using plan-out path or plan id. Declined interactive apply still says no rewrite performed. Owner: cli-lane. Parallel with list-core.

**Blocks:**
- `c3bf5116-5168-4c45-a4b7-9586135f7409`

#### Subtasks

- [ ] **Fix plan-only vs declined-apply messaging** — `433b9827-ce02-43fa-a750-e15ee5170a14` (todo)
  Plan-only uses Plan saved; user declined apply keeps distinct message. Signal: tests distinguish strings.
- [ ] **Add printRewritePlanNextSteps helper** — `9fd9760e-9d16-4b16-8014-403e27414e2b` (todo)
  Central helper formats Next lines from plan ref file or id. Signal: helper covered by test.
- [ ] **Wire footer on generate and edit plan-only** — `acfea34a-cb96-46ed-9df7-841324752e19` (todo)
  Call helper from generate path and editSavedRewritePlan when planOnly after valid plan. Signal: rewrite_commits_test captures stdout.
### [CLI] Implement compact and verbose acd list table renderers

**ID:** `7d6a1715-9c5f-43c4-a561-60d335522d39`  
**Status:** done  
**Owner:** list-core-agent  

Target: internal/cli/list.go, internal/cli/helpers.go. Read: renderListTable, blockedListStatusNote, homeShort. Do not touch daemon loop. Acceptance: compact shows last-two-segment REPO (e.g. Development/Auto-Commit-Daemon), DAEMON, PEND, BLK, HEAD, short STATUS; verbose shows homeShort, CLIENTS, PENDING, BLOCKED, LAST_COMMIT, STATUS with note; missing rows avoid five dash columns. Verify: go test ./internal/cli/... -run List -race -count=1. Owner: cli-lane. Parallel with rewrite-footer.
Design update: compact REPO uses last two path segments (Development/Auto-Commit-Daemon), canonical compact REPO format. Verbose still uses homeShort full path.

**Blocks:**
- `c462604e-4ff8-479d-b368-aa1e3b96f5b7`

#### Subtasks

- [ ] **Render verbose list table columns** — `0ea9e48d-0cdd-4f79-8054-2f9668a8e8c1` (todo)
  Implement renderListTableVerbose matching improved current layout with shortened blocked note text. Signal: TestList_Human_TwoRepos and pause tests pass with --verbose.
- [ ] **Render compact list table columns** — `312f21d8-92b6-4158-9119-5f5d58fa3cc4` (todo)
  Implement renderListTableCompact: REPO column uses pathLastTwoLabel (e.g. Development/Auto-Commit-Daemon), plus DAEMON, PEND, BLK, HEAD, short STATUS tokens. Signal: TestList compact assertions pass.
- [ ] **Add repo last-two-segment display helper** — `a60b688a-02f2-4b90-835f-6cd20f0b97fd` (todo)
  Add pathLastTwoLabel helper: join parentDir/base from repo path (homeShort first). On collision in one snapshot, suffix with repo_hash tail. Signal: list test covers two paths sharing same last-two segments.
- [ ] **Shorten blocked status notes** — `c2a69e5f-4042-4ec3-be38-96d007b774c0` (todo)
  Replace embedded acd fix command paragraphs with diagnose and fix --dry-run pointers. Signal: blocked row fits typical terminal width in verbose mode.
### [CLI] Test rewrite-commits Next footer output

**ID:** `c3bf5116-5168-4c45-a4b7-9586135f7409`  
**Status:** done  

Target: internal/cli/rewrite_commits_test.go. Acceptance: plan-only stdout contains Next and apply-plan commands. Verify: go test ./internal/cli/... -run Rewrite -race -count=1. Owner: cli-lane. Blocked by rewrite-footer.

**Blocked by:**
- `7668298f-0743-4e1b-8501-54185c72db67`

**Blocks:**
- `6a04eafe-78b4-4a20-a8c6-7be0983e803d`

### [CLI] Default acd list to watch on TTY with --once guard

**ID:** `c462604e-4ff8-479d-b368-aa1e3b96f5b7`  
**Status:** done  

Target: internal/cli/list.go newListCmd. Acceptance: TTY without flags runs watch compact; pipe uses once compact; --once forces static on TTY; --watch remains alias; --json still rejects watch. Verify: list tests for TTY simulation. Owner: cli-lane. Blocked by list-core.

**Blocked by:**
- `7d6a1715-9c5f-43c4-a561-60d335522d39`

**Blocks:**
- `4f352a81-fd1a-4bae-bc24-79fd32c7ffd5`

#### Subtasks

- [ ] **Detect TTY and choose watch vs once** — `5bba9cc4-16dc-436f-a45c-c4729581fca3` (todo)
  Use isatty on stdout; default watch only when TTY and not --once. Signal: test with fake terminal or extracted chooser function.
- [ ] **Apply compact layout in watch frames** — `bf1a1f36-4427-4f0d-ad9e-467fcca837ab` (todo)
  renderListWatchFrame uses compact unless verbose. Signal: watch test asserts compact header columns.
- [ ] **Add --once and --verbose flags** — `c737f804-bbf3-46b5-a4fd-34531e5a44c6` (todo)
  Wire verbose into render path; update cobra Long Example help text. Signal: acd list --help mentions dashboard default and --once.

## Dependencies

| Source | Depends on | Type |
|--------|------------|------|
| `6a04eafe-78b4-4a20-a8c6-7be0983e803d` (task) | `c3bf5116-5168-4c45-a4b7-9586135f7409` (task) | internal |
| `c3bf5116-5168-4c45-a4b7-9586135f7409` (task) | `7668298f-0743-4e1b-8501-54185c72db67` (task) | internal |
| `00fb3254-312b-4b4e-8be8-66712a686f5d` (task) | `6a04eafe-78b4-4a20-a8c6-7be0983e803d` (task) | internal |
| `6a04eafe-78b4-4a20-a8c6-7be0983e803d` (task) | `4f352a81-fd1a-4bae-bc24-79fd32c7ffd5` (task) | internal |
| `4f352a81-fd1a-4bae-bc24-79fd32c7ffd5` (task) | `c462604e-4ff8-479d-b368-aa1e3b96f5b7` (task) | internal |
| `c462604e-4ff8-479d-b368-aa1e3b96f5b7` (task) | `7d6a1715-9c5f-43c4-a561-60d335522d39` (task) | internal |

---

*Exported from Trekoon on 2026-05-27T16:34:22.089Z. This is a snapshot — the database is the source of truth.*
