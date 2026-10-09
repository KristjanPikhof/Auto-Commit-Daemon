package daemon

import (
	"reflect"
	"strings"
	"testing"
)

const intentCobraFixtureRoot = `package cli
import "github.com/spf13/cobra"
func Execute() error { root := newRootCmd(); return root.Execute() }
func newRootCmd() *cobra.Command {
 cmd := &cobra.Command{Use:"acd"}
 cmd.AddCommand(newSupportNamespaceCmd())
 return cmd
}
func withInvocationCapabilities(command *cobra.Command, ignored bool) *cobra.Command {
 command.Annotations = map[string]string{"json":"true"}
 return command
}
`

const intentCobraFixtureFacade = `package cli
import "github.com/spf13/cobra"
func newSupportNamespaceCmd() *cobra.Command {
 cmd := &cobra.Command{Use:"support"}
 cmd.AddCommand(newSupportRecoverCmd())
 return cmd
}
func newSupportRecoverCmd() *cobra.Command {
 command := newFixCmd()
 command.Use = "recover"
 return withInvocationCapabilities(command,true)
}
func runProductFix(force bool) bool { return buildFixPlan(force) }
`

const intentCobraFixtureFix = `package cli
import "github.com/spf13/cobra"
const (fixActionReconcileUnpublishedChain = "reconcile_unpublished_chain")
func newFixCmd() *cobra.Command {
 cmd := &cobra.Command{Use:"fix"}
 cmd.Flags().Bool("force",false,"Preserve unresolved captures")
 cmd.Flags().Bool("dry-run",false,"Preview")
 cmd.Flags().Bool("yes",false,"Apply")
 return cmd
}
func buildFixPlan(force bool) bool { return force }
`

func TestIntentCobraQualifiedReferenceRequiresActualHierarchy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, root, facade, fix, doc string
		valid                        bool
	}{
		{"qualified", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+Use `acd support recover --force --yes`.\n", true},
		{"fenced", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, " ```bash\n+acd support recover --force\n ```\n", true},
		{"missing_root", "", intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"wrong_root", strings.Replace(intentCobraFixtureRoot, `Use:"acd"`, `Use:"other"`, 1), intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"wrong_command", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd repo recover --force`\n", false},
		{"wrong_flag", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --unknown`\n", false},
		{"flag_only", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+Use `--force`.\n", false},
		{"prose", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+Run acd support recover --force.\n", false},
		{"unchanged_doc", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, " Use `acd support recover --force`.\n+Other prose\n", false},
		{"truncated_fence", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, " ```bash\n+acd support recover --force\n", false},
		{"shell_operator", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force && other`\n", false},
		{"dynamic_use", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = chooseCommand()`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"conditional_use", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "recover"; if changed { command.Use = "other" }`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"opaque_mutator", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "recover"; mutate(command)`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"mutating_wrapper", strings.Replace(intentCobraFixtureRoot, `command.Annotations = map[string]string{"json":"true"}`, `command.Use = "other"`, 1), intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"aliased_wrapper", strings.Replace(intentCobraFixtureRoot, `command.Annotations = map[string]string{"json":"true"}`, `alias := command; alias.Use = "other"`, 1), intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"address_alias", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "recover"; alias := &command; (*alias).Use = "other"`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"composite_alias", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "recover"; aliases := []*cobra.Command{command}; aliases[0].Use = "other"`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"multi_use", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "recover"; command.Use, other = "other", "x"`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"shadowed_child", strings.Replace(intentCobraFixtureRoot, `cmd.AddCommand(newSupportNamespaceCmd())`, `newSupportNamespaceCmd := func() *cobra.Command { return &cobra.Command{Use:"other"} }; cmd.AddCommand(newSupportNamespaceCmd())`, 1), intentCobraFixtureFacade, intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"shadowed_base", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, `command := newFixCmd()`, `newFixCmd := func() *cobra.Command { return &cobra.Command{Use:"other"} }; command := newFixCmd()`, 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"other_package", intentCobraFixtureRoot, strings.Replace(intentCobraFixtureFacade, "package cli", "package other", 1), intentCobraFixtureFix, "+`acd support recover --force`\n", false},
		{"incomplete_source", intentCobraFixtureRoot, intentCobraFixtureFacade, intentCobraFixtureFix[:len(intentCobraFixtureFix)-3], "+`acd support recover --force`\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := []intentCobraSource{{path: "cli/root.go", contents: tc.root}, {path: "cli/facade.go", seq: 3, contents: tc.facade}, {path: "cli/fix.go", seq: 2, contents: tc.fix}}
			var owners []intentCobraSource
			for _, invocation := range intentDocumentCLIInvocations(tc.doc) {
				owners = intentCobraQualifiedOwners(intentCobraFactories(sources), invocation)
			}
			if (len(owners) > 0) != tc.valid {
				t.Fatalf("owners=%+v valid=%t", owners, tc.valid)
			}
			if tc.valid {
				seqs := map[int64]bool{}
				for _, owner := range owners {
					if owner.seq > 0 {
						seqs[owner.seq] = true
					}
				}
				if !reflect.DeepEqual(seqs, map[int64]bool{2: true, 3: true}) {
					t.Fatalf("actual unpublished owners=%v", seqs)
				}
			}
		})
	}
}
