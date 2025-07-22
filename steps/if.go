package steps

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
)

type IfStep struct {
	name      string
	condition core.InterpolateValue[bool]
}

func (f *IfStep) Name() string { return f.name }

func (f *IfStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	condition, err := f.condition.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("condition", f.condition.Raw)
	}

	if condition {
		return core.CreateResultData("true", nil), nil
	}

	return core.CreateResultData("false", nil), nil
}

func init() {
	pipeline.RegisterStepType("if", func(name string, config map[string]any) (core.Step, error) {
		condition, ok := config["condition"]
		if !ok {
			return nil, core.ErrMissingConfig("condition")
		}

		return &IfStep{name: name, condition: core.InterpolateValue[bool]{Raw: condition}}, nil
	})
}
