package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"go-etl/core"
)

type JsonStep struct {
	name string
	data core.InterpolateValue[string]
}

func (s *JsonStep) Name() string { return s.name }

func (s *JsonStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	dataString, err := s.data.Resolve(state)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve data: %w", err)
	}
	// Unmarshal the JSON data into a map
	var jsonData map[string]any
	if err := json.Unmarshal([]byte(dataString), &jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
	}

	return core.CreateDefaultResultData(jsonData), nil
}

func newJSONStep(name string, config map[string]any) (core.Step, error) {
	data, ok := config["data"].(string)
	if !ok {
		return nil, core.ErrMissingConfig("data")
	}

	return &JsonStep{
		name: name,
		data: core.InterpolateValue[string]{Raw: data},
	}, nil
}
