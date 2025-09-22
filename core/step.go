package core

import (
	"context"
)

type Step interface {
	Name() string
	Run(ctx context.Context, state *PipelineState) (map[string]*Data, error)
}

type StepFactory func(name string, config map[string]any) (Step, error)
