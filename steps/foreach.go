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
		// for _, step := range f.subSteps {
		// 	_, err := step.Run(ctx, subState)
		// 	if err != nil {
		// 		return nil, fmt.Errorf("foreach substep failed: %v", err)
		// 	}
		// }
	}
	return map[string]*core.Data{"default": {Value: fmt.Sprintf("processed %d items", len(list))}}, nil
}

func newForeachStep(name string, config map[string]any) (core.Step, error) {
	// itemVar, ok := config["item_var"].(string)
	// if !ok {
	// 	return nil, fmt.Errorf("foreach requires 'item_var'")
	// }
	stepsCfg, ok := config["steps"].([]any)
	if !ok {
		return nil, fmt.Errorf("foreach requires 'steps' array")
	}

	list, ok := config["list"]
	if !ok {
		return nil, fmt.Errorf("foreach requires 'list' to iterate over")
	}

	var subSteps []pipeline.StepConfig
	// pipelineConfig := pipeline.PipelineConfig{
	// 	Steps: make([]pipeline.StepConfig, 0, len(stepsCfg)),
	// }

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
		// name, _ := m["name"].(string)
		// typeStr, _ := m["type"].(string)
		// // cfg := make(map[string]any)

		// // for k, v := range m {
		// // 	switch k {
		// // 	case "name", "type":
		// // 		continue
		// // 	default:
		// // 		cfg[k] = v
		// // 	}
		// // }
		// factory, ok := pipeline.GetStepFactory(typeStr)
		// if !ok {
		// 	return nil, fmt.Errorf("unknown substep type: %s", typeStr)
		// }

		// cfg, _ := m["config"].(map[string]interface{})

		// // if err != nil {
		// // 	return nil, fmt.Errorf("failed to parse substep config: %v", err)
		// // }

		// step, err := factory(name, cfg)
		// if err != nil {
		// 	return nil, err
		// }
		// subSteps = append(subSteps, step)
	}

	// p, err := pipeline.LoadPipeline(pipelineConfig)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to load substeps pipeline: %v", err)
	// }
	// err = p.Run(ctx, state.Logger)
	return &ForeachStep{name: name, list: core.InterpolateValue[[]any]{Raw: list}, subSteps: subSteps}, nil
}
