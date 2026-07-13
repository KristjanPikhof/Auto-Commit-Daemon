package settingsui

import (
	"context"
	"fmt"
	"io"
	"strings"

	"charm.land/huh/v2"
)

type AccessibleValues struct { Values map[string]string; Action string; Confirm bool }

func AccessibleForm(draft map[string]string, input io.Reader, output io.Writer) (*huh.Form, *AccessibleValues) {
	values:=&AccessibleValues{Values:map[string]string{}}
	var fields []huh.Field
	for _,desc:=range Fields(){
		if desc.Sensitive { continue }
		value:=safeText(draft[desc.Key]); values.Values[desc.Key]=value
		fields=append(fields,huh.NewInput().Key(desc.Key).Title(desc.Label+" ("+desc.Apply+")").Value(&values.Values[desc.Key]))
	}
	fields=append(fields,huh.NewSelect[string]().Key("action").Title("Next action").Options(huh.NewOption("Save draft only","save"),huh.NewOption("Run strict synthetic test","test"),huh.NewOption("Apply tested draft at next safe boundary","apply")).Value(&values.Action))
	fields=append(fields,huh.NewConfirm().Key("confirm").Title("Confirm selected action? Provider tests may make one paid synthetic request.").Value(&values.Confirm))
	form:=huh.NewForm(huh.NewGroup(fields...)).WithAccessible(true).WithInput(input).WithOutput(output).WithShowHelp(true)
	return form,values
}

func RunAccessible(ctx context.Context,draft map[string]string,input io.Reader,output io.Writer)(AccessibleValues,error){form,values:=AccessibleForm(draft,input,output);if err:=form.RunWithContext(ctx);err!=nil{return AccessibleValues{},err};if !values.Confirm{return AccessibleValues{},fmt.Errorf("action not confirmed")};values.Action=safeText(strings.ToLower(values.Action));values.Values=sanitizedDraft(values.Values);return *values,nil}
