package steps

import (
	"context"
	"fmt"
	"go-etl/core"
)

type StdoutStep struct {
	name string
}

func (s *StdoutStep) Name() string     { return s.name }
func (s *StdoutStep) Inputs() []string { return nil }
func (s *StdoutStep) Run(ctx context.Context, inputs map[string]*core.Data) (*core.Data, error) {
	for name, d := range inputs {
		for _, v := range d.Value {
			fmt.Printf("[%s] => %v\n", name, v)
		}
		return &core.Data{Value: d.Value}, nil
	}
	return nil, nil
}

func newStdoutStep(name string, config map[string]any) (core.Step, error) {
	return &StdoutStep{name: name}, nil
}
