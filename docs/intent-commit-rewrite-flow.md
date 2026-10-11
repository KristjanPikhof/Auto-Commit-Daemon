# Explicit history rewrite flow

History rewrite is separate from continuous protection and from finishing new
work. The user selects a contiguous linear range, saves a plan, previews its
apply, and confirms it explicitly.

~~~text
selected original commits -> pinned source history
                          -> Local or Intent plan
                          -> saved immutable goals and trees
                          -> exact apply preview and checks
                          -> backup + one publication transaction
~~~

Local preserves original commit membership and order, including empty and
net-neutral sequences. Intent uses exact recorded path versions to plan
completed goals. `--messages-only` fixes the original membership even under
Intent. Output goals preserve author boundaries, rename groups, dependencies,
and exact final tree equality. Neutral provenance does not force fabricated
commits.

A saved plan records source HEAD, selected commits, complete goal trees,
configuration, style, and private evidence refs. Showing or applying it makes
no provider call. Editing supported messages appends a plan revision; changing
purposeful goals requires new planning.

Before apply, the worker checks clean initial worktree/index, branch identity,
source and goal pins, approved configuration, and every candidate tree. It
builds the entire chain privately. The publication transaction verifies expected
HEAD and index and retains a backup ref. New-branch apply creates only the
approved absent target branch and preserves the source branch.

The worker owns accepted apply work independently of the client. Later saves
continue to receive checkpoints without entering the selected history. On
restart, prepared proof can complete only when the exact source or already
applied target is proven; ambiguity requires attention. There is no automatic
history repair or provider-driven restart loop.

See [rewrite commands and examples](rewrite-commits.md) and the
[finish contract](finish-contract.md#explicit-history-rewrite-and-legacy-preservation).
