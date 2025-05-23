package core

import "context"

type Data struct {
	Value []any
}

type Step interface {
	Name() string
	Inputs() []string
	Run(ctx context.Context, inputs map[string]*Data) (*Data, error)
}

type StepFactory func(name string, config map[string]any) (Step, error)
