package steps

import (
	"context"
	"errors"
	"go-etl/core"
	"time"
)

type DelayStep struct {
	name   string
	inputs []string
	delay  time.Duration
}

func (d *DelayStep) Name() string     { return d.name }
func (d *DelayStep) Inputs() []string { return d.inputs }
func (d *DelayStep) Run(ctx context.Context, inputs map[string]*core.Data) (*core.Data, error) {
	time.Sleep(d.delay)
	for _, d := range inputs {
		return &core.Data{Value: d.Value}, nil
	}
	return nil, nil
}

func newDelayStep(name string, config map[string]any) (core.Step, error) {
	ms, ok := config["ms"].(int)
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
	return &DelayStep{name: name, inputs: inputs, delay: time.Millisecond * time.Duration(ms)}, nil
}
