package steps

import (
	"context"
	"go-etl/core"
	"strings"
)

type UppercaseStep struct {
	name string
}

func (f *UppercaseStep) Name() string     { return f.name }
func (f *UppercaseStep) Inputs() []string { return nil }
func (f *UppercaseStep) Run(ctx context.Context, inputs map[string]*core.Data) (*core.Data, error) {
	for _, d := range inputs {
		outputs := make([]any, len(d.Value))
		for i, v := range d.Value {
			if str, ok := v.(string); ok {
				outputs[i] = strings.ToUpper(str)
			} else {
				outputs[i] = strings.ToUpper(str)
			}
		}

		return &core.Data{Value: outputs}, nil
	}
	return nil, nil
}

func newUppercaseStep(name string, _ map[string]any) (core.Step, error) {
	return &UppercaseStep{name: name}, nil
}
