package finish

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var projectReferenceBlock = regexp.MustCompile(`\{[^{}]*\bisa\s*=\s*PBXFileReference\s*;[^{}]*\}`)
var projectRootReference = regexp.MustCompile(`\bsourceTree\s*=\s*SOURCE_ROOT\s*;`)
var projectReferencePath = regexp.MustCompile(`\bpath\s*=\s*("(?:\\.|[^"\\])*"|[^;]+)\s*;`)
var projectInputList = regexp.MustCompile(`"\$\(SRCROOT\)/([^"\r\n]+\.xcfilelist)"`)

// WithProjectSourceDependencies orders newly created SOURCE_ROOT files before
// their explicit Xcode build references. Existing files and rename groups keep
// their recorded rules; this does not infer dependencies from proximity.
func WithProjectSourceDependencies(evidence Evidence, diffs map[string]string) Evidence {
	created := map[string][]string{}
	for _, change := range evidence.Changes {
		file := change.After.Mode == "100644" || change.After.Mode == "100755"
		if change.Before.Mode == "" && file && change.AtomicGroup == "" &&
			evidence.Base[change.Path].Mode == "" && evidence.Target[change.Path].Mode != "" {
			created[change.Path] = append(created[change.Path], change.ID)
		}
	}
	evidence.Changes = slices.Clone(evidence.Changes)
	for i, change := range evidence.Changes {
		project := strings.HasSuffix(change.Path, ".xcodeproj/project.pbxproj")
		if !project && !strings.HasSuffix(change.Path, ".xcfilelist") {
			continue
		}
		var after strings.Builder
		for _, line := range strings.Split(diffs[change.ID], "\n") {
			if strings.HasPrefix(line, "+++") || len(line) == 0 || (line[0] != '+' && line[0] != ' ') {
				continue
			}
			after.WriteString(line[1:])
			after.WriteByte('\n')
		}
		require := func(path string) {
			ids := created[path]
			if len(ids) == 1 && ids[0] != change.ID && !slices.Contains(change.DependsOn, ids[0]) {
				change.DependsOn = append(slices.Clone(change.DependsOn), ids[0])
			}
		}
		if !project {
			for _, line := range strings.Split(after.String(), "\n") {
				if path, ok := strings.CutPrefix(strings.TrimSpace(line), "$(SRCROOT)/"); ok {
					require(path)
				}
			}
		}
		for _, match := range projectInputList.FindAllStringSubmatch(after.String(), -1) {
			require(match[1])
		}
		for _, block := range projectReferenceBlock.FindAllString(after.String(), -1) {
			match := projectReferencePath.FindStringSubmatch(block)
			if !projectRootReference.MatchString(block) || len(match) != 2 {
				continue
			}
			path := strings.TrimSpace(match[1])
			if strings.HasPrefix(path, `"`) {
				var err error
				path, err = strconv.Unquote(path)
				if err != nil {
					continue
				}
			}
			require(path)
		}
		evidence.Changes[i] = change
	}
	return evidence
}
