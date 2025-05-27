package steps

import (
	"context"
	"errors"
	"go-etl/core"
	"os"
)

type FileStep struct {
	name string
	path core.InterpolateValue[string]
}

func (f *FileStep) Name() string { return f.name }

func (f *FileStep) Run(ctx context.Context, state *core.PipelineState) (*core.Data, error) {
	path, err := f.path.Resolve(state)
	if err != nil {
		return nil, core.ErrInterpolate("path", f.path.Raw)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &core.Data{Value: string(b)}, nil
}

func newFileStep(name string, config map[string]any) (core.Step, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, errors.New("missing 'path' in file step")
	}
	return &FileStep{name: name, path: core.InterpolateValue[string]{Raw: path}}, nil
}
