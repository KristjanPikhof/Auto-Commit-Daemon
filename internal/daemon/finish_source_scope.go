package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type finishSourceScope struct {
	start, end int
	name       string
}

var swiftSourceRange = regexp.MustCompile(`range=\[.*?:(\d+):(\d+) - line:(\d+):(\d+)\]`)
var swiftSourceSymbol = regexp.MustCompile(`\((class_decl|struct_decl|enum_decl|extension_decl|func_decl|constructor_decl|destructor_decl)\b.*?range=\[[^\]]+\]\s+"([^"]+)"`)
var sourceFunctionPrefix = regexp.MustCompile(`\b(func|init|deinit)\b`)

func finishSourceScopes(ctx context.Context, dir, path string, content []byte) ([]string, error) {
	lines := finishSourceLines(content)
	if len(lines) == 0 {
		return nil, nil
	}
	var symbols, blocks []finishSourceScope
	switch filepath.Ext(path) {
	case ".swift":
		file := filepath.Join(dir, "scope.swift")
		if err := os.WriteFile(file, content, 0600); err != nil {
			return nil, err
		}
		parseCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(parseCtx, "xcrun", "swiftc", "-frontend", "-dump-parse", file)
		var output sourceScopeOutput
		// Swift writes a trailing newline to stderr. Sharing its pipe with the
		// AST can split a declaration mid-line and change edit IDs on retry.
		cmd.Stdout, cmd.Stderr = &output, io.Discard
		if err := cmd.Run(); err != nil {
			if parseCtx.Err() != nil {
				return nil, parseCtx.Err()
			}
			return nil, errors.New("Swift scope parsing failed; a working Swift compiler and valid frozen syntax are required")
		}
		var err error
		symbols, blocks, err = parseSwiftSourceScopes(output.String(), lines)
		if err != nil {
			return nil, err
		}
	case ".go":
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, content, parser.SkipObjectResolution)
		if err != nil {
			return nil, errors.New("Go scope parsing failed; valid frozen syntax is required")
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if node == nil {
				return true
			}
			start, end := set.Position(node.Pos()), set.Position(node.End())
			switch n := node.(type) {
			case *ast.FuncDecl:
				name := n.Name.Name
				if n.Recv != nil && len(n.Recv.List) > 0 {
					var receiver bytes.Buffer
					_ = format.Node(&receiver, set, n.Recv.List[0].Type)
					name = receiver.String() + "." + name
				}
				symbols = append(symbols, finishSourceScope{start.Line, end.Line, "func:" + name})
			case *ast.TypeSpec:
				symbols = append(symbols, finishSourceScope{start.Line, end.Line, "type:" + n.Name.Name})
			case *ast.BlockStmt:
				if start.Line < end.Line {
					prefix := strings.TrimSpace(string(lines[start.Line-1][:start.Column-1]))
					blocks = append(blocks, finishSourceScope{start.Line, end.Line, prefix})
				}
			}
			return true
		})
	}
	return sourceScopeKeys(ctx, lines, symbols, blocks)
}

func sourceScopeKeys(ctx context.Context, lines [][]byte, symbols, blocks []finishSourceScope) ([]string, error) {
	bodies := make(map[[2]int]bool, len(symbols))
	for _, symbol := range symbols {
		bodies[[2]int{symbol.start, symbol.end}] = true
	}
	var nested []finishSourceScope
	for _, block := range blocks {
		if !bodies[[2]int{block.start, block.end}] && !sourceFunctionPrefix.MatchString(block.name) {
			nested = append(nested, block)
		}
	}
	keys := make([]string, len(lines))
	for i := range lines {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var enclosing []finishSourceScope
		for _, symbol := range symbols {
			if symbol.start <= i+1 && i+1 <= symbol.end {
				enclosing = append(enclosing, symbol)
			}
		}
		sort.SliceStable(enclosing, func(a, b int) bool {
			if enclosing[a].start == enclosing[b].start {
				return enclosing[a].end > enclosing[b].end
			}
			return enclosing[a].start < enclosing[b].start
		})
		var names []string
		for _, symbol := range enclosing {
			names = append(names, symbol.name)
		}
		for _, block := range nested {
			if block.start <= i+1 && i+1 <= block.end {
				names = append(names, "block:"+block.name)
			}
		}
		encoded, _ := json.Marshal(names)
		keys[i] = string(encoded)
	}
	return keys, nil
}

func parseSwiftSourceScopes(dump string, lines [][]byte) ([]finishSourceScope, []finishSourceScope, error) {
	var symbols, blocks []finishSourceScope
	for _, line := range strings.Split(dump, "\n") {
		span := swiftSourceRange.FindStringSubmatch(line)
		if span == nil {
			continue
		}
		start, _ := strconv.Atoi(span[1])
		column, _ := strconv.Atoi(span[2])
		end, _ := strconv.Atoi(span[3])
		if start < 1 || end < start || end > len(lines) || column < 1 || column-1 > len(lines[start-1]) {
			return nil, nil, fmt.Errorf("Swift parser returned invalid source positions")
		}
		if symbol := swiftSourceSymbol.FindStringSubmatch(line); symbol != nil {
			name, _, _ := strings.Cut(symbol[2], "(")
			symbols = append(symbols, finishSourceScope{start, end, symbol[1] + ":" + name})
		}
		if strings.Contains(line, "(brace_stmt ") && end > start {
			prefix := strings.TrimSpace(string(lines[start-1][:column-1]))
			blocks = append(blocks, finishSourceScope{start, end, prefix})
		}
	}
	return symbols, blocks, nil
}

// Never retain an unbounded compiler dump or include it in a provider error.
type sourceScopeOutput struct{ data bytes.Buffer }

func (out *sourceScopeOutput) String() string { return out.data.String() }

func (out *sourceScopeOutput) Write(data []byte) (int, error) {
	if out.data.Len()+len(data) > 8<<20 {
		return 0, errors.New("source scope output exceeds 8 MiB")
	}
	return out.data.Write(data)
}
