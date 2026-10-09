package daemon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
)

type intentCobraSource struct {
	path, oid, contents string
	seq                 int64
}

type intentCobraFactory struct {
	source          intentCobraSource
	name, use, base string
	flags, children []string
}

type intentCLIInvocation struct {
	command, flags []string
}

func (invocation intentCLIInvocation) key() string {
	return strings.Join(invocation.command, " ") + " | " + strings.Join(invocation.flags, " ")
}

// Only changed inline or complete fenced commands are contracts. Prose and a
// standalone flag cannot identify the command that owns an option.
func intentDocumentCLIInvocations(diff string) []intentCLIInvocation {
	// Existing witnesses already retain only signed inline commands or a
	// self-contained, closed fenced command. Empty/truncated witnesses fail.
	fragments := map[string]bool{}
	for _, witnesses := range intentDocumentPublicWitnesses(diff) {
		for _, raw := range witnesses {
			lines := strings.Split(raw, "\n")
			if len(lines) == 3 && len(lines[1]) > 0 && (lines[1][0] == '+' || lines[1][0] == '-') {
				fragments[strings.TrimSpace(lines[1][1:])] = true
			} else if len(lines) == 1 && len(raw) > 0 && (raw[0] == '+' || raw[0] == '-') {
				for _, match := range intentDocumentInline.FindAllStringSubmatch(raw[1:], 32) {
					fragments[match[1]] = true
				}
			}
		}
	}
	var ordered []string
	for fragment := range fragments {
		ordered = append(ordered, fragment)
	}
	sort.Strings(ordered)
	var invocations []intentCLIInvocation
	for _, fragment := range ordered {
		invocation := intentCLIInvocation{}
		valid := true
		for _, word := range strings.Fields(fragment) {
			if strings.HasPrefix(word, "--") && intentPublicFlagName.MatchString(strings.TrimPrefix(word, "--")) {
				invocation.flags = append(invocation.flags, word)
			} else if len(invocation.flags) == 0 && intentPublicFlagName.MatchString(word) {
				invocation.command = append(invocation.command, word)
			} else {
				valid = false
				break
			}
		}
		if valid && len(invocation.command) >= 2 && len(invocation.command) <= 4 && len(invocation.flags) > 0 && len(invocation.flags) <= 8 && len(invocations) < 32 {
			sort.Strings(invocation.flags)
			invocations = append(invocations, invocation)
		}
	}
	return invocations
}

func intentCobraFactories(sources []intentCobraSource) map[string]intentCobraFactory {
	type parsed struct {
		source intentCobraSource
		file   *ast.File
		alias  string
	}
	var files []parsed
	wrappers := map[string]bool{}
	declarations := map[string]int{}
	scope := ""
	for _, source := range sources {
		if len(source.contents) > intentSourceReferenceScanCap {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), source.path, source.contents, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		alias := ""
		for _, imported := range file.Imports {
			if imported.Path.Value == `"github.com/spf13/cobra"` {
				alias = "cobra"
				if imported.Name != nil {
					alias = imported.Name.Name
				}
			}
		}
		if alias == "" || alias == "." || alias == "_" {
			continue
		}
		currentScope := path.Dir(source.path) + "/" + file.Name.Name
		if scope != "" && scope != currentScope {
			continue
		}
		scope = currentScope
		files = append(files, parsed{source, file, alias})
		for _, declaration := range file.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil {
				declarations[function.Name.Name]++
				wrappers[function.Name.Name] = declarations[function.Name.Name] == 1 && intentCobraIdentityWrapper(function, alias)
			}
		}
	}
	factories := map[string]intentCobraFactory{}
	duplicates := map[string]bool{}
	for _, parsed := range files {
		for _, declaration := range parsed.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Body == nil || function.Type.Results == nil || len(function.Type.Results.List) != 1 || !intentCobraType(function.Type.Results.List[0].Type, parsed.alias) || len(function.Body.List) == 0 || len(function.Type.Params.List) != 0 {
				continue
			}
			locals := intentCobraLocalBindings(function)
			safeWrappers := make(map[string]bool)
			for name, safe := range wrappers {
				safeWrappers[name] = safe && !locals[name]
			}
			returned, ok := function.Body.List[len(function.Body.List)-1].(*ast.ReturnStmt)
			if !ok || len(returned.Results) != 1 {
				continue
			}
			result := returned.Results[0]
			if call, ok := result.(*ast.CallExpr); ok {
				name, direct := call.Fun.(*ast.Ident)
				if !direct || !safeWrappers[name.Name] || len(call.Args) == 0 {
					continue
				}
				result = call.Args[0]
			}
			command, ok := result.(*ast.Ident)
			if !ok {
				continue
			}
			factory := intentCobraFactory{source: parsed.source, name: function.Name.Name}
			valid := intentCobraCommandBindingStable(function, command.Name, safeWrappers)
			for _, statement := range function.Body.List {
				if assignment, ok := statement.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
					if variable, ok := assignment.Lhs[0].(*ast.Ident); ok && variable.Name == command.Name {
						factory.use, factory.base = intentCobraBinding(assignment.Rhs[0], parsed.alias)
						valid = valid && (factory.use != "" || factory.base != "") && !locals[factory.base]
					}
					if selector, ok := assignment.Lhs[0].(*ast.SelectorExpr); ok && selector.Sel.Name == "Use" {
						if variable, ok := selector.X.(*ast.Ident); ok && variable.Name == command.Name {
							factory.use = intentCobraString(assignment.Rhs[0])
							valid = valid && factory.use != ""
						}
					}
				}
				expression, ok := statement.(*ast.ExprStmt)
				if !ok {
					continue
				}
				call, ok := expression.X.(*ast.CallExpr)
				if !ok {
					continue
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				if variable, ok := selector.X.(*ast.Ident); ok && variable.Name == command.Name && selector.Sel.Name == "AddCommand" {
					for _, argument := range call.Args {
						if child, ok := argument.(*ast.CallExpr); ok && len(child.Args) == 0 {
							if name, ok := child.Fun.(*ast.Ident); ok {
								if locals[name.Name] {
									valid = false
								} else {
									factory.children = append(factory.children, name.Name)
								}
							}
						}
					}
				}
				flags, ok := selector.X.(*ast.CallExpr)
				if !ok || len(flags.Args) != 0 {
					continue
				}
				getter, ok := flags.Fun.(*ast.SelectorExpr)
				if !ok || (getter.Sel.Name != "Flags" && getter.Sel.Name != "PersistentFlags") {
					continue
				}
				variable, ok := getter.X.(*ast.Ident)
				if !ok || variable.Name != command.Name {
					continue
				}
				if selector.Sel.Name == "Bool" || selector.Sel.Name == "BoolP" || selector.Sel.Name == "String" || selector.Sel.Name == "StringP" {
					if len(call.Args) > 0 {
						if flag := intentCobraString(call.Args[0]); flag != "" {
							factory.flags = append(factory.flags, "--"+flag)
						}
					}
				}
			}
			if !valid || len(factories) >= 128 {
				continue
			}

			if _, exists := factories[factory.name]; exists {
				duplicates[factory.name] = true
			}
			factories[factory.name] = factory
		}
	}
	for name := range duplicates {
		delete(factories, name)
	}
	return factories
}

func intentCobraBinding(expression ast.Expr, alias string) (string, string) {
	if call, ok := expression.(*ast.CallExpr); ok && len(call.Args) == 0 {
		if name, ok := call.Fun.(*ast.Ident); ok {
			return "", name.Name
		}
	}
	pointer, ok := expression.(*ast.UnaryExpr)
	if !ok || pointer.Op != token.AND {
		return "", ""
	}
	literal, ok := pointer.X.(*ast.CompositeLit)
	if !ok || !intentCobraType(&ast.StarExpr{X: literal.Type}, alias) {
		return "", ""
	}
	for _, element := range literal.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok {
			if name, ok := pair.Key.(*ast.Ident); ok && name.Name == "Use" {
				return intentCobraString(pair.Value), ""
			}
		}
	}
	return "", ""
}

func intentCobraString(expression ast.Expr) string {
	if literal, ok := expression.(*ast.BasicLit); ok && literal.Kind == token.STRING {
		value, _ := strconv.Unquote(literal.Value)
		return value
	}
	return ""
}

func intentCobraType(expression ast.Expr, alias string) bool {
	pointer, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Command" {
		return false
	}
	name, ok := selector.X.(*ast.Ident)
	return ok && name.Name == alias
}

func intentCobraIdentityWrapper(function *ast.FuncDecl, alias string) bool {
	if function.Recv != nil || function.Body == nil || function.Type.Params == nil || len(function.Type.Params.List) == 0 || len(function.Type.Params.List[0].Names) != 1 || !intentCobraType(function.Type.Params.List[0].Type, alias) || function.Type.Results == nil || len(function.Type.Results.List) != 1 || !intentCobraType(function.Type.Results.List[0].Type, alias) {
		return false
	}
	parameter := function.Type.Params.List[0].Names[0].Name
	valid, returns := true, 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			valid = false
			return false
		}
		switch value := node.(type) {
		case *ast.ReturnStmt:
			returns++
			if len(value.Results) != 1 {
				valid = false
				break
			}
			name, ok := value.Results[0].(*ast.Ident)
			valid = valid && ok && name.Name == parameter
		case *ast.AssignStmt:
			for _, left := range value.Lhs {
				if name, ok := left.(*ast.Ident); ok && name.Name == parameter {
					valid = false
				}
			}
			for _, right := range value.Rhs {
				if name, ok := right.(*ast.Ident); ok && name.Name == parameter {
					valid = false
				}
			}
		case *ast.UnaryExpr:
			if name, ok := value.X.(*ast.Ident); ok && name.Name == parameter && value.Op == token.AND {
				valid = false
			}
		case *ast.ValueSpec:
			for _, right := range value.Values {
				if name, ok := right.(*ast.Ident); ok && name.Name == parameter {
					valid = false
				}
			}
		case *ast.CompositeLit:
			valid = valid && !intentCobraContainsName(value, parameter)
		case *ast.SelectorExpr:
			if name, ok := value.X.(*ast.Ident); ok && name.Name == parameter && value.Sel.Name != "Annotations" {
				valid = false
			}
		case *ast.CallExpr:
			for _, argument := range value.Args {
				if name, ok := argument.(*ast.Ident); ok && name.Name == parameter {
					valid = false
				}
			}
		}
		return true
	})
	return valid && returns > 0
}

func intentCobraQualifiedOwners(factories map[string]intentCobraFactory, invocation intentCLIInvocation) []intentCobraSource {
	var owners []intentCobraSource
	matches := 0
	var walk func(string, int, []intentCobraSource) bool
	walk = func(name string, depth int, path []intentCobraSource) bool {
		factory, found := factories[name]
		if !found || depth >= len(invocation.command) {
			return false
		}
		flags := append([]string(nil), factory.flags...)
		lineage := []intentCobraSource{factory.source}
		base := factory.base
		for count := 0; base != "" && count < 4; count++ {
			parent, found := factories[base]
			if !found {
				return false
			}
			lineage = append(lineage, parent.source)
			flags = append(flags, parent.flags...)
			if factory.use == "" {
				factory.use = parent.use
			}
			base = parent.base
		}
		use := strings.Fields(factory.use)
		if base != "" || len(use) == 0 || use[0] != invocation.command[depth] {
			return false
		}
		path = append(path, lineage...)
		if depth+1 < len(invocation.command) {
			found := false
			for _, child := range factory.children {
				found = walk(child, depth+1, path) || found
			}
			return found
		}
		for _, wanted := range invocation.flags {
			found := false
			for _, flag := range flags {
				found = found || flag == wanted
			}
			if !found {
				return false
			}
		}
		owners = path
		matches++
		return true
	}
	for name := range factories {
		walk(name, 0, nil)
	}
	if matches != 1 {
		return nil
	}
	return owners
}

// A conditional rename, pointer alias or opaque mutator cannot establish a
// stable public command path. Callback-local variables are separate scopes.
func intentCobraCommandBindingStable(function *ast.FuncDecl, command string, wrappers map[string]bool) bool {
	allowedUses := map[*ast.AssignStmt]bool{}
	for _, statement := range function.Body.List {
		if assignment, ok := statement.(*ast.AssignStmt); ok && assignment.Tok == token.ASSIGN && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 && intentCobraString(assignment.Rhs[0]) != "" {
			allowedUses[assignment] = true
		}
	}
	valid, bindings := true, 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		switch value := node.(type) {
		case *ast.CompositeLit:
			valid = valid && !intentCobraContainsName(value, command)
		case *ast.UnaryExpr:
			if name, ok := value.X.(*ast.Ident); ok && name.Name == command && value.Op == token.AND {
				valid = false
			}
		case *ast.ValueSpec:
			for _, right := range value.Values {
				if name, ok := right.(*ast.Ident); ok && name.Name == command {
					valid = false
				}
			}
		case *ast.ReturnStmt:
			if len(value.Results) != 1 {
				valid = false
				break
			}
			result := value.Results[0]
			if call, ok := result.(*ast.CallExpr); ok {
				callee, direct := call.Fun.(*ast.Ident)
				if !direct || !wrappers[callee.Name] || len(call.Args) == 0 {
					valid = false
					break
				}
				result = call.Args[0]
			}
			name, same := result.(*ast.Ident)
			valid = valid && same && name.Name == command
		case *ast.AssignStmt:
			for _, left := range value.Lhs {
				if name, ok := left.(*ast.Ident); ok && name.Name == command {
					bindings++
				}
				if selector, ok := left.(*ast.SelectorExpr); ok && selector.Sel.Name == "Use" {
					if name, ok := selector.X.(*ast.Ident); ok && name.Name == command && !allowedUses[value] {
						valid = false
					}
				}
			}
			for _, right := range value.Rhs {
				if name, ok := right.(*ast.Ident); ok && name.Name == command {
					valid = false
				}
			}
		case *ast.CallExpr:
			for _, argument := range value.Args {
				if name, ok := argument.(*ast.Ident); ok && name.Name == command {
					callee, direct := value.Fun.(*ast.Ident)
					if !direct || !wrappers[callee.Name] {
						valid = false
					}
				}
			}
			if selector, ok := value.Fun.(*ast.SelectorExpr); ok {
				if name, ok := selector.X.(*ast.Ident); ok && name.Name == command && (selector.Sel.Name == "RemoveCommand" || selector.Sel.Name == "ResetFlags" || selector.Sel.Name == "ResetCommands") {
					valid = false
				}
			}
		}
		return true
	})
	return valid && bindings == 1
}

func intentCobraLocalBindings(function *ast.FuncDecl) map[string]bool {
	locals := make(map[string]bool)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		switch value := node.(type) {
		case *ast.AssignStmt:
			for _, left := range value.Lhs {
				if name, ok := left.(*ast.Ident); ok {
					locals[name.Name] = true
				}
			}
		case *ast.ValueSpec:
			for _, name := range value.Names {
				locals[name.Name] = true
			}
		}
		return true
	})
	return locals
}

func intentCobraContainsName(expression ast.Expr, name string) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		if identifier, ok := node.(*ast.Ident); ok && identifier.Name == name {
			found = true
		}
		return !found
	})
	return found
}
