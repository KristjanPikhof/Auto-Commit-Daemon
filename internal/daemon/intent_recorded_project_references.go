package daemon

import (
	"path"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Project context comes from an immutable recorded blob. Only registered file
// objects can supply path witnesses; comments and quoted prose cannot do so.
func intentProjectReferenceContext(sourcePath, recordedContents string, offeredPaths []string) string {
	if path.Ext(sourcePath) != ".pbxproj" || strings.ContainsRune(recordedContents, 0) || !utf8.ValidString(recordedContents) {
		return ""
	}
	recordedContents = recordedContents[:min(len(recordedContents), intentSourceReferenceScanCap)]
	targets, basenames := make(map[string]bool), make(map[string]int)
	for _, offered := range offeredPaths[:min(len(offeredPaths), 256)] {
		target := path.Clean(offered)
		if target != sourcePath && !targets[target] {
			targets[target] = true
			basenames[path.Base(target)]++
		}
	}
	references := func(name string) bool {
		name = path.Clean(name)
		return targets[name] || targets[path.Clean(path.Join(path.Dir(sourcePath), name))] ||
			(name == path.Base(name) && basenames[name] == 1)
	}
	tokens := intentProjectTokens(recordedContents)
	type object struct {
		objects, registered, ambiguous bool
		arrayDepth                     int
		isa, filename, rawPath, tree   string
	}
	var stack []object
	arrayDepth := 0
	seen := make(map[string]bool)
	var result strings.Builder
	for index, token := range tokens {
		switch token.raw {
		case "(":
			arrayDepth++
		case ")":
			if arrayDepth > 0 {
				arrayDepth--
			}
		case "{":
			frame := object{arrayDepth: arrayDepth}
			if index >= 2 && tokens[index-1].raw == "=" && !tokens[index-2].quoted {
				frame.objects = tokens[index-2].raw == "objects"
				frame.registered = len(stack) > 0 && stack[len(stack)-1].objects &&
					arrayDepth == stack[len(stack)-1].arrayDepth && intentProjectObjectID(tokens[index-2].raw)
			}
			stack = append(stack, frame)
		case "}":
			if len(stack) == 0 {
				continue
			}
			frame := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			localTree := frame.tree == "" || frame.tree == "<group>" || frame.tree == "SOURCE_ROOT" || frame.tree == "PROJECT_DIR"
			if !frame.registered || frame.ambiguous || arrayDepth != frame.arrayDepth || frame.isa != "PBXFileReference" ||
				!localTree || frame.rawPath == "" || !references(frame.filename) {
				continue
			}
			witness := " path = " + frame.rawPath + ";\n"
			if !seen[witness] && result.Len()+len(witness) <= intentSourceReferenceContextCap {
				result.WriteString(witness)
				seen[witness] = true
			}
		default:
			if len(stack) == 0 || token.quoted || (token.raw != "isa" && token.raw != "path" && token.raw != "sourceTree") || index+3 >= len(tokens) {
				continue
			}
			frame := &stack[len(stack)-1]
			if !frame.registered || arrayDepth != frame.arrayDepth || tokens[index+1].raw != "=" ||
				tokens[index+3].raw != ";" || (index > 0 && tokens[index-1].raw != "{" && tokens[index-1].raw != ";") {
				continue
			}
			value := tokens[index+2]
			if value.value == "" {
				continue
			}
			if token.raw == "isa" {
				frame.ambiguous = frame.ambiguous || frame.isa != ""
				frame.isa = value.value
			} else if token.raw == "path" {
				frame.ambiguous = frame.ambiguous || frame.rawPath != ""
				frame.filename, frame.rawPath = value.value, value.raw
			} else {
				frame.ambiguous = frame.ambiguous || frame.tree != ""
				frame.tree = value.value
			}
		}
	}
	return result.String()
}

type intentProjectToken struct {
	raw, value string
	quoted     bool
}

func intentProjectTokens(contents string) []intentProjectToken {
	var tokens []intentProjectToken
	for index := 0; index < len(contents); {
		if strings.ContainsRune(" \t\r\n", rune(contents[index])) {
			index++
			continue
		}
		if strings.HasPrefix(contents[index:], "//") {
			end := strings.IndexByte(contents[index:], '\n')
			if end < 0 {
				break
			}
			index += end + 1
			continue
		}
		if strings.HasPrefix(contents[index:], "/*") {
			end := strings.Index(contents[index+2:], "*/")
			if end < 0 {
				break
			}
			index += end + 4
			continue
		}
		start := index
		if contents[index] == '"' {
			index++
			for index < len(contents) && contents[index] != '"' {
				if contents[index] == '\\' {
					index++
				}
				index++
			}
			if index >= len(contents) {
				break
			}
			index++
			raw := contents[start:index]
			value, err := strconv.Unquote(raw)
			if err != nil {
				value = ""
			}
			tokens = append(tokens, intentProjectToken{raw: raw, value: value, quoted: true})
			continue
		}
		if strings.ContainsRune("{}()=;,", rune(contents[index])) {
			index++
			tokens = append(tokens, intentProjectToken{raw: contents[start:index]})
			continue
		}
		for index < len(contents) && !strings.ContainsRune(" \t\r\n{}()=;,\"", rune(contents[index])) &&
			!strings.HasPrefix(contents[index:], "/*") && !strings.HasPrefix(contents[index:], "//") {
			index++
		}
		raw := contents[start:index]
		tokens = append(tokens, intentProjectToken{raw: raw, value: raw})
	}
	return tokens
}

func intentProjectObjectID(value string) bool {
	if len(value) != 24 {
		return false
	}
	for _, char := range value {
		if !strings.ContainsRune("0123456789abcdefABCDEF", char) {
			return false
		}
	}
	return true
}
