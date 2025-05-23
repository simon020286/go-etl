package steps

import (
	"context"
	"errors"
	"go-etl/core"
	"os"
)

type FileStep struct {
	name string
	path string
}

func (f *FileStep) Name() string     { return f.name }
func (f *FileStep) Inputs() []string { return nil }
func (f *FileStep) Run(ctx context.Context, _ map[string]*core.Data) (*core.Data, error) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		return nil, err
	}
	return &core.Data{Value: []any{string(b)}}, nil
}

func newFileStep(name string, config map[string]any) (core.Step, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, errors.New("missing 'path' in file step")
	}
	return &FileStep{name: name, path: path}, nil
}
