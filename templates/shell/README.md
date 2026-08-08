# Shell integration

Install or update transactionally:

~~~bash
acd setup --integrations=shell
~~~

ACD merges a bounded, versioned block into the supported user shell file. The
integration can accelerate repository/session discovery, but filesystem
watching and complete polling provide protection without it.

Enable or disable repositories with `acd on` and `acd off`. Use
`acd uninstall --dry-run` to preview removal of only the verified owned block.
