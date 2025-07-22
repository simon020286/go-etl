package {{.Package}}

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
)

type {{.StructName}} struct {
	name string
}

func (s *{{.StructName}}) Name() string { return s.name }

func (s *{{.StructName}}) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// TODO: implement step logic
	return core.CreateDefaultResultData(nil), nil
}

func init() {
	pipeline.RegisterStepType("{{.ID}}", func(name string, config map[string]any) (core.Step, error) {
		return &{{.StructName}}{name: name}, nil
	})
}
