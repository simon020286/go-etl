package steps

import (
	"context"
	"fmt"
	"go-etl/core"
	"go-etl/pipeline"

	"gopkg.in/yaml.v3"
)

type ForeachStep struct {
	name     string
	list     core.InterpolateValue[[]any]
	subSteps []pipeline.StepConfig
}

func (f *ForeachStep) Name() string { return f.name }

func (f *ForeachStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	list, err := f.list.Resolve(state)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve list in foreach step: %v", err)
	}

	pipelineConfig := pipeline.PipelineConfig{
		Steps: f.subSteps,
	}

	pipeline, err := pipeline.LoadPipeline(pipelineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load substeps pipeline: %v", err)
	}

	for i, item := range list {
		subState := &core.PipelineState{Results: make(map[string]map[string]*core.Data)}
		subState.Set("foreach", map[string]*core.Data{
			"item":  {Value: item},
			"index": {Value: i},
		})

		pipeline.SetState(subState)

		err = pipeline.Run(ctx, state.Logger)
		if err != nil {
			return nil, fmt.Errorf("foreach substep failed: %v", err)
		}
	}
	return map[string]*core.Data{"default": {Value: fmt.Sprintf("processed %d items", len(list))}}, nil
}

func init() {
	pipeline.RegisterStepType("foreach", func(name string, config map[string]any) (core.Step, error) {
		stepsCfg, ok := config["steps"].([]any)
		if !ok {
			return nil, fmt.Errorf("foreach requires 'steps' array")
		}

		list, ok := config["list"]
		if !ok {
			return nil, fmt.Errorf("foreach requires 'list' to iterate over")
		}

		var subSteps []pipeline.StepConfig

		for _, raw := range stepsCfg {
			m, ok := raw.((map[string]any))
			if !ok {
				return nil, fmt.Errorf("invalid substep format")
			}

			subStepString, err := yaml.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal substep config: %v", err)
			}

			var subStep pipeline.StepConfig
			if err := yaml.Unmarshal(subStepString, &subStep); err != nil {
				return nil, fmt.Errorf("failed to unmarshal substep config: %v", err)
			}

			subSteps = append(subSteps, subStep)
		}

		return &ForeachStep{name: name, list: core.InterpolateValue[[]any]{Raw: list}, subSteps: subSteps}, nil
	})
}
