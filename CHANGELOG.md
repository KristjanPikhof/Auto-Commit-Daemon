# Changelog

## Unreleased

### Breaking changes

- `ACD_AI_SEND_DIFF` removed. Diff egress now follows `ACD_AI_PROVIDER` selection. Network providers receive diffs by default; deterministic provider behavior unchanged. Setting `ACD_AI_SEND_DIFF` triggers a one-shot deprecation warn-log.
