# v19 to v20 checkpoint cutover

ACD upgrades all registered repositories in one transaction through
`acd setup`. There is no legacy engine mode, dual-read period, staged
production rollout, or automatic post-commit downgrade.

## Preflight

Setup resolves every enabled worktree, checks SQLite, dry-runs exact-chain
proof, resolves all required refs and objects, verifies disk capacity, rejects
unsafe Git operations, and creates one immutable plan digest over repositories,
services, integrations, and configuration.

Missing disabled worktrees may proceed only when their database has no
unpublished or terminal work. One unresolved or ambiguous repository blocks
the complete cutover.

## Apply

1. Start a temporary protection bridge for every enabled worktree.
2. Stop old owners through canonical ownership checks.
3. Acquire repository locks in sorted common-directory order.
4. Create and verify WAL-consistent v19 database backups.
5. Preserve provable unpublished chains as checkpoint or recovery history.
6. Create complete migration checkpoints, including bridge-observed edits.
7. apply schema v20 to every repository and registry v2 globally.
8. Install the managed binary, supervisor service, and owned integrations.
9. Run the isolated checkpoint/publish/restore self-test.
10. Hold new workers until schema, version, ownership, recovery, and current
    coverage are proven.
11. Import any final bridge checkpoint, prove no observation gap, and commit.

Historical published events are not fabricated into checkpoints. The cutover
imports current full worktree states, exact unpublished chains, required
recovery snapshots, and a bounded recent published-history projection.

Existing settings are preserved exactly. Fresh installations use deterministic
Intent/Fast.

## Rollback and recovery

Before global commit, setup stops held v20 workers, restores every preimage and
v19 database, CAS-deletes only refs it created at their exact expected targets,
and restores the prior service desired state. A bridge ref with otherwise
unrepresented work is retained with a local recovery manifest.

After commit, new changes may exist only in v20 checkpoints, so downgrade is
forbidden. Recovery proceeds forward with `acd doctor` and
`acd support repair`.
