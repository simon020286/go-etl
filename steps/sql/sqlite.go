package sql

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
)

type SQLiteStep struct {
	name       string
	connection string
	query      string
}

func (s *SQLiteStep) Name() string { return s.name }

func (s *SQLiteStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	return nil, nil
}

func init() {
	pipeline.RegisterStepType("sqlite", func(name string, config map[string]any) (core.Step, error) {

		return &SQLiteStep{name: name, connection: "", query: "s"}, nil
	})
}
