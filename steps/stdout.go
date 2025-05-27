package steps

import (
	"context"
	"fmt"
	"go-etl/core"
)

type StdoutStep struct {
	name  string
	value core.InterpolateValue[string]
}

func (s *StdoutStep) Name() string { return s.name }

func (s *StdoutStep) Run(ctx context.Context, state *core.PipelineState) (*core.Data, error) {
	value, err := s.value.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("value", s.value.Raw)
	}
	fmt.Printf("%v\n", value)
	return &core.Data{Value: value}, nil
}

func newStdoutStep(name string, config map[string]any) (core.Step, error) {
	value, ok := config["value"].(string)
	if !ok {
		return nil, fmt.Errorf("missing 'value' in stdout step")
	}
	return &StdoutStep{name: name, value: core.InterpolateValue[string]{Raw: value}}, nil
}
