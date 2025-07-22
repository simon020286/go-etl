package steps

import (
	"context"
	"errors"
	"go-etl/core"
	"go-etl/pipeline"
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

func init() {
	pipeline.RegisterStepType("file", func(name string, config map[string]any) (core.Step, error) {
		path, ok := config["path"].(string)
		if !ok {
			return nil, errors.New("missing 'path' in file step")
		}
		return &FileStep{name: name, path: core.InterpolateValue[string]{Raw: path}}, nil
	})
}
