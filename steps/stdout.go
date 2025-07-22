package steps

import (
	"context"
	"fmt"
	"go-etl/core"
	"go-etl/pipeline"
)

type StdoutStep struct {
	name  string
	value core.InterpolateValue[string]
}

func (s *StdoutStep) Name() string { return s.name }

func (s *StdoutStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	value, err := s.value.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("value", s.value.Raw)
	}
	fmt.Printf("%v\n", value)
	return core.CreateDefaultResultData(value), nil
}

func init() {
	pipeline.RegisterStepType("stdout", func(name string, config map[string]any) (core.Step, error) {
		value, ok := config["value"].(string)
		if !ok {
			return nil, fmt.Errorf("missing 'value' in stdout step")
		}
		return &StdoutStep{name: name, value: core.InterpolateValue[string]{Raw: value}}, nil
	})
}
