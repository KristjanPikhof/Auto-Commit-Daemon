package cli

import (
	isatty "github.com/mattn/go-isatty"
	"io"
	"os"
)

var (
	settingsInputTTY = func(r io.Reader) bool {
		f, ok := r.(*os.File)
		return ok && (isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()))
	}
	settingsOutputTTY = func(w io.Writer) bool {
		f, ok := w.(interface{ Fd() uintptr })
		return ok && (isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()))
	}
)
