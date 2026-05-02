# Changelog

## Unreleased

### Breaking changes

- `ACD_AI_SEND_DIFF` removed. Diff egress to AI providers is now off by
  default and requires the new `ACD_AI_DIFF_EGRESS=1` opt-in even when a
  network provider (`openai-compat`, `subprocess:<name>`) is selected.
  Setting `ACD_AI_SEND_DIFF` triggers a one-shot deprecation warn-log at
  daemon startup. See `docs/ai-providers.md` for the full migration
  guide.
