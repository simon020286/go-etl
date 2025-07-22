package steps

import (
	"context"
	"errors"
	"go-etl/core"
	"go-etl/pipeline"
	"time"
)

type DelayStep struct {
	name  string
	delay core.InterpolateValue[int]
}

func (d *DelayStep) Name() string { return d.name }

func (d *DelayStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	delay, err := d.delay.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("delay", d.delay.Raw)
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
	return core.CreateDefaultResultData(nil), nil
}

func init() {
	pipeline.RegisterStepType("delay", func(name string, config map[string]any) (core.Step, error) {
		ms, ok := config["ms"]
		if !ok {
			return nil, errors.New("missing 'ms' in delay step")
		}
		rawInputs, _ := config["inputs"].([]any)
		inputs := make([]string, len(rawInputs))
		for i, v := range rawInputs {
			inputs[i], ok = v.(string)
			if !ok {
				return nil, errors.New("invalid input name")
			}
		}
		return &DelayStep{name: name, delay: core.InterpolateValue[int]{Raw: ms}}, nil
	})
}
