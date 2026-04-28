# Uninstall acd from Pi

1. Remove the `# acd-managed: true` block (and the five `acd-*` hooks it added) from your `.pi/hook/hooks.yaml`.
2. Stop any running daemons:
   ~~~bash
   acd stop --all
   ~~~
3. (Optional) Remove the acd binary and state — see the top-level uninstall guide.
