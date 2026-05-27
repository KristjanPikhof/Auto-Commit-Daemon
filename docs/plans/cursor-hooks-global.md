---
epic_id: 4a7de6d8-6fa2-41fd-96c0-02aeac28da3a
schema_version: 1
exported_at: 2026-05-27T10:56:22.671Z
status: todo
---

# ACD: add global Cursor hooks harness

## Summary

| Metric | Count |
|--------|-------|
| Tasks | 9 |
| Subtasks | 15 |
| Dependencies | 12 |
| External nodes | 0 |
| Warnings | 0 |

### Task status breakdown

| Status | Count |
|--------|-------|
| todo | 9 |

### Subtask status breakdown

| Status | Count |
|--------|-------|
| todo | 15 |

## Description

User-global hooks.json only. Wire Cursor agent hooks to acd lifecycle.
Scope: user-global ~/.cursor/hooks.json only (match claude-code opencode pi). Not repo-local .cursor/hooks.json (unlike codex .codex). Design: helper script ~/.cursor/hooks/acd-lifecycle.sh; hooks.json commands ./hooks/acd-lifecycle.sh {start|wake|flush|stop}. Events: sessionStart start; postToolUse+afterFileEdit start+wake; stop flush --logical; sessionEnd stop. Session id: conversation_id. Repo: first git root in workspace_roots else cwd from tool hooks. watch-pid 0. Out of scope: project hooks, setup --apply merge, Tab hooks optional later.

## Task index

| # | Title | Status | Subtasks |
|---|-------|--------|----------|
| 1 | [\[Test\] Integration cursor e2e](#task-051f7973-b5e4-4c26-936c-21f234757d20) | todo | 1 |
| 2 | [\[Spec\] Define Cursor harness contract](#task-130fcb0c-eac8-4fd3-963d-e291a41d496d) | todo | 3 |
| 3 | [\[Docs\] README and CHANGELOG](#task-1384b780-d539-4e1b-8a3e-7fae00c96d4b) | todo | 1 |
| 4 | [\[CLI\] Cursor stdin extraction](#task-1efc14fd-0365-4bd2-bda6-5e21db2e872a) | todo | 2 |
| 5 | [\[CLI\] Wire acd setup cursor](#task-4805aa77-a2e3-4837-833d-04b06dcc6f7b) | todo | 1 |
| 6 | [\[Templates\] hooks.json README uninstall](#task-60008df5-acdf-4302-81a4-eefb3b278670) | todo | 2 |
| 7 | [\[Adapter\] Register cursor detection](#task-860a03f1-49e1-4d28-8008-c7138fbc1f3d) | todo | 2 |
| 8 | [\[CLI\] Doctor checks for cursor](#task-daaf7038-a57d-4982-9b5d-cd538918d4e8) | todo | 1 |
| 9 | [\[Harness\] Ship acd-lifecycle.sh](#task-dff780a4-9233-4959-b546-9dc46e4b97a7) | todo | 2 |

## Tasks

### [Test] Integration cursor e2e

**ID:** `051f7973-b5e4-4c26-936c-21f234757d20`  
**Status:** todo  

Target adapter_e2e_test.go subtest cursor. Owner test-lane.

**Blocked by:**
- `dff780a4-9233-4959-b546-9dc46e4b97a7`
- `1efc14fd-0365-4bd2-bda6-5e21db2e872a`
- `daaf7038-a57d-4982-9b5d-cd538918d4e8`

**Blocks:**
- `1384b780-d539-4e1b-8a3e-7fae00c96d4b`

#### Subtasks

- [ ] **runCursorE2E** — `d4b4683d-6a99-4d4d-847b-3047e225456c` (todo)
  TestAdapterE2E cursor subtest.
### [Spec] Define Cursor harness contract

**ID:** `130fcb0c-eac8-4fd3-963d-e291a41d496d`  
**Status:** todo  

Write event map and stdin rules in templates/cursor/README.md. Global path only. Owner spec-lane.

**Blocks:**
- `860a03f1-49e1-4d28-8008-c7138fbc1f3d`
- `1efc14fd-0365-4bd2-bda6-5e21db2e872a`
- `dff780a4-9233-4959-b546-9dc46e4b97a7`

#### Subtasks

- [ ] **Stdin precedence** — `1214d69d-623a-4c33-9839-edbcc69fe659` (todo)
  conversation_id session. workspace_roots git root then cwd.
- [ ] **Map Cursor events** — `cbe436e7-1fe6-46ee-b528-d12c1c2daaa0` (todo)
  sessionStart start. postToolUse afterFileEdit start+wake. stop flush. sessionEnd stop.
- [ ] **Global install only** — `f1801359-1ff0-45f8-9be3-5b869b626304` (todo)
  ~/.cursor/hooks.json canonical. Out of scope repo .cursor hooks.json.
### [Docs] README and CHANGELOG

**ID:** `1384b780-d539-4e1b-8a3e-7fae00c96d4b`  
**Status:** todo  

Document global install and merge warning. Owner docs-lane.

**Blocked by:**
- `051f7973-b5e4-4c26-936c-21f234757d20`

#### Subtasks

- [ ] **README CHANGELOG** — `c76f7105-64e3-457d-afa6-bd34a1331602` (todo)
  Harness table and setup command.
### [CLI] Cursor stdin extraction

**ID:** `1efc14fd-0365-4bd2-bda6-5e21db2e872a`  
**Status:** todo  

Extend hook-stdin-extract or add helper for conversation_id and repo. Target internal/cli/hookhelper.go. Verify go test ./internal/cli -run Hook. Owner cli-lane.

**Blocked by:**
- `130fcb0c-eac8-4fd3-963d-e291a41d496d`

**Blocks:**
- `dff780a4-9233-4959-b546-9dc46e4b97a7`
- `051f7973-b5e4-4c26-936c-21f234757d20`

#### Subtasks

- [ ] **Implement extraction** — `72154ffa-c331-4301-bb95-de95102dfe32` (todo)
  Sample Cursor stdin tests.
- [ ] **Edge cases** — `ccd0452f-e1e4-4754-9da4-485530d78ee8` (todo)
  Empty roots and non-git cwd.
### [CLI] Wire acd setup cursor

**ID:** `4805aa77-a2e3-4837-833d-04b06dcc6f7b`  
**Status:** todo  

Target setup.go embed.go. Owner cli-lane.

**Blocked by:**
- `860a03f1-49e1-4d28-8008-c7138fbc1f3d`
- `60008df5-acdf-4302-81a4-eefb3b278670`

**Blocks:**
- `daaf7038-a57d-4982-9b5d-cd538918d4e8`

#### Subtasks

- [ ] **setup cursor** — `97f248ff-d82e-4fef-ae51-c1643a32d01c` (todo)
  harnessSnippets embed.go.
### [Templates] hooks.json README uninstall

**ID:** `60008df5-acdf-4302-81a4-eefb3b278670`  
**Status:** todo  

Target templates/cursor/. Validate with acd setup cursor --raw. Owner harness-lane.

**Blocked by:**
- `dff780a4-9233-4959-b546-9dc46e4b97a7`

**Blocks:**
- `4805aa77-a2e3-4837-833d-04b06dcc6f7b`

#### Subtasks

- [ ] **hooks.json** — `c453a614-3b95-49e5-b8b4-0a60f90fe14c` (todo)
  _acd_managed version 1 lifecycle commands.
- [ ] **README uninstall** — `e05cd681-949c-4fd4-ae90-3e26021aadc5` (todo)
  Merge warning copy script to ~/.cursor/hooks.
### [Adapter] Register cursor detection

**ID:** `860a03f1-49e1-4d28-8008-c7138fbc1f3d`  
**Status:** todo  

Target internal/adapter/known.go path ~/.cursor/hooks.json only. Owner adapter-lane.

**Blocked by:**
- `130fcb0c-eac8-4fd3-963d-e291a41d496d`

**Blocks:**
- `4805aa77-a2e3-4837-833d-04b06dcc6f7b`

#### Subtasks

- [ ] **Tests** — `6a71470b-c703-4998-b30f-1ac97bade7ec` (todo)
  go test internal adapter.
- [ ] **knownHarnesses** — `f35ce8ae-3ca8-4489-a31c-87c19b1f668d` (todo)
  ~/.cursor/hooks.json only.
### [CLI] Doctor checks for cursor

**ID:** `daaf7038-a57d-4982-9b5d-cd538918d4e8`  
**Status:** todo  

Target doctor.go. Owner cli-lane.

**Blocked by:**
- `4805aa77-a2e3-4837-833d-04b06dcc6f7b`

**Blocks:**
- `051f7973-b5e4-4c26-936c-21f234757d20`

#### Subtasks

- [ ] **Doctor cursor** — `ddf6567b-c338-4219-8858-94e3f50bf2b0` (todo)
  JSON template drift like codex.
### [Harness] Ship acd-lifecycle.sh

**ID:** `dff780a4-9233-4959-b546-9dc46e4b97a7`  
**Status:** todo  

Target templates/cursor/hooks/acd-lifecycle.sh. Subcommands start wake flush stop. Owner harness-lane.

**Blocked by:**
- `1efc14fd-0365-4bd2-bda6-5e21db2e872a`
- `130fcb0c-eac8-4fd3-963d-e291a41d496d`

**Blocks:**
- `60008df5-acdf-4302-81a4-eefb3b278670`
- `051f7973-b5e4-4c26-936c-21f234757d20`

#### Subtasks

- [ ] **Embed script** — `663bee27-49c7-4419-9e3c-ff1bebff9730` (todo)
  templates embed cursor hooks dir.
- [ ] **Lifecycle script** — `8ae2d201-c4b0-464e-97a0-d7b54161ae19` (todo)
  start wake flush stop subcommands.

## Dependencies

| Source | Depends on | Type |
|--------|------------|------|
| `60008df5-acdf-4302-81a4-eefb3b278670` (task) | `dff780a4-9233-4959-b546-9dc46e4b97a7` (task) | internal |
| `860a03f1-49e1-4d28-8008-c7138fbc1f3d` (task) | `130fcb0c-eac8-4fd3-963d-e291a41d496d` (task) | internal |
| `1efc14fd-0365-4bd2-bda6-5e21db2e872a` (task) | `130fcb0c-eac8-4fd3-963d-e291a41d496d` (task) | internal |
| `051f7973-b5e4-4c26-936c-21f234757d20` (task) | `dff780a4-9233-4959-b546-9dc46e4b97a7` (task) | internal |
| `dff780a4-9233-4959-b546-9dc46e4b97a7` (task) | `1efc14fd-0365-4bd2-bda6-5e21db2e872a` (task) | internal |
| `4805aa77-a2e3-4837-833d-04b06dcc6f7b` (task) | `860a03f1-49e1-4d28-8008-c7138fbc1f3d` (task) | internal |
| `1384b780-d539-4e1b-8a3e-7fae00c96d4b` (task) | `051f7973-b5e4-4c26-936c-21f234757d20` (task) | internal |
| `051f7973-b5e4-4c26-936c-21f234757d20` (task) | `1efc14fd-0365-4bd2-bda6-5e21db2e872a` (task) | internal |
| `4805aa77-a2e3-4837-833d-04b06dcc6f7b` (task) | `60008df5-acdf-4302-81a4-eefb3b278670` (task) | internal |
| `daaf7038-a57d-4982-9b5d-cd538918d4e8` (task) | `4805aa77-a2e3-4837-833d-04b06dcc6f7b` (task) | internal |
| `051f7973-b5e4-4c26-936c-21f234757d20` (task) | `daaf7038-a57d-4982-9b5d-cd538918d4e8` (task) | internal |
| `dff780a4-9233-4959-b546-9dc46e4b97a7` (task) | `130fcb0c-eac8-4fd3-963d-e291a41d496d` (task) | internal |

---

*Exported from Trekoon on 2026-05-27T10:56:22.671Z. This is a snapshot — the database is the source of truth.*
