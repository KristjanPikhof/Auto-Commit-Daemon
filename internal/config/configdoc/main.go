package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

const defaultOutput = "docs/configuration-reference.md"

func main() {
	check := flag.Bool("check", false, "fail when the generated reference differs from the checked-in file")
	write := flag.Bool("write", false, "write the generated reference to the checked-in file")
	output := flag.String("output", defaultOutput, "reference document path")
	flag.Parse()

	if *check && *write {
		fail("--check and --write cannot be combined")
	}
	if err := run(*output, *check, *write, os.Stdout); err != nil {
		fail("%v", err)
	}
}

func run(output string, check, write bool, stdout io.Writer) error {
	generated := config.RenderReference()
	if check {
		current, err := os.ReadFile(output)
		if err != nil {
			return fmt.Errorf("read %s: %w", output, err)
		}
		if !bytes.Equal(current, generated) {
			return fmt.Errorf("%s is stale; run `go run ./internal/config/configdoc --write`", output)
		}
		return nil
	}
	if write {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
		if err := os.WriteFile(output, generated, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", output, err)
		}
		return nil
	}
	if _, err := stdout.Write(generated); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "configdoc: "+format+"\n", args...)
	os.Exit(1)
}
