package settingsui

import (
	"io"

	tea "charm.land/bubbletea/v2"
)

type Options struct { Input io.Reader; Output io.Writer; Accessible bool; NoColor bool }

func Run(backend Backend,opts Options) error {
	if opts.Accessible { _,err:=RunAccessible(contextBackground(),map[string]string{},opts.Input,opts.Output);return err }
	m:=New(backend);m.noColor=opts.NoColor
	var programOpts []tea.ProgramOption
	if opts.Input!=nil{programOpts=append(programOpts,tea.WithInput(opts.Input))}
	if opts.Output!=nil{programOpts=append(programOpts,tea.WithOutput(opts.Output))}
	_,err:=tea.NewProgram(m,programOpts...).Run();return err
}
