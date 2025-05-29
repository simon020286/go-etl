package steps

import (
	"context"
	"fmt"
	"go-etl/core"
	"strings"
)

type UppercaseStep struct {
	name  string
	value core.InterpolateValue[string]
}

func (f *UppercaseStep) Name() string { return f.name }

func (f *UppercaseStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	value, err := f.value.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("value", f.value.Raw)
	}

	return core.CreateDefaultResultData(strings.ToUpper(value)), nil
}

func newUppercaseStep(name string, config map[string]any) (core.Step, error) {
	value, ok := config["value"]
	if !ok {
		return nil, core.ErrMissingConfig("value")
	}

	return &UppercaseStep{name: name, value: core.InterpolateValue[string]{Raw: fmt.Sprintf("%s", value)}}, nil
}
