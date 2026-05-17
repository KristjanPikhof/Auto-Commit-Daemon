# Status Machine Reference

Trekoon enforces a status transition graph. Only these transitions are valid:

| From | Allowed targets |
|---|---|
| `todo` | `in_progress`, `blocked` |
| `in_progress` | `done`, `blocked` |
| `blocked` | `in_progress`, `todo` |
| `done` | `in_progress` |

Invalid transitions (e.g. `todo → done`) return error code
`status_transition_invalid`. Always transition through `in_progress` to reach
`done`.

**Exception:** `task done` auto-transitions through `in_progress` when the task
is in `todo` or `blocked` status, so you can call `task done` from any
non-done status.

Recommended statuses for consistent workflows: `todo`, `in_progress`, `done`.
Use `blocked` with an appended reason when work is stuck.
