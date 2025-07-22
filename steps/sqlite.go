package steps

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
)

type Sqlite struct {
	name string
}

func (s *Sqlite) Name() string { return s.name }

func (s *Sqlite) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// TODO: implement step logic
	return core.CreateDefaultResultData(nil), nil
}

func init() {
	pipeline.RegisterStepType("sqlite", func(name string, config map[string]any) (core.Step, error) {
		return &Sqlite{name: name}, nil
	})
}
