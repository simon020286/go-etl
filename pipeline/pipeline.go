package pipeline

import (
	"context"
	"fmt"
	"go-etl/core"
	"log/slog"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type Pipeline struct {
	steps    map[string]core.Step
	graph    map[string][]string
	inputs   map[string][]string
	OnChange func(event core.ChangeEvent)
}

func LoadPipelineFromFile(filePath string) (*Pipeline, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open pipeline file: %w", err)
	}
	defer file.Close()

	var config PipelineConfig
	dec := yaml.NewDecoder(file)
	if err := dec.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode pipeline config: %w", err)
	}

	return LoadPipeline(config)
}

func LoadPipeline(config PipelineConfig) (*Pipeline, error) {
	stepsMap := make(map[string]core.Step)
	graph := make(map[string][]string)
	inputs := make(map[string][]string)

	for _, sc := range config.Steps {
		factory, ok := stepRegistry[sc.Type]
		if !ok {
			return nil, fmt.Errorf("unknown step type: %s", sc.Type)
		}
		step, err := factory(sc.Name, sc.Config)
		if err != nil {
			return nil, err
		}
		stepsMap[sc.Name] = step
		inputs[sc.Name] = sc.Inputs
		for _, input := range sc.Inputs {
			graph[input] = append(graph[input], sc.Name)
		}
	}

	return &Pipeline{steps: stepsMap, graph: graph, inputs: inputs}, nil
}

func (p *Pipeline) Run(ctx context.Context, logger *slog.Logger) error {
	state := &core.PipelineState{Results: make(map[string]map[string]*core.Data)}
	done := make(map[string]chan struct{})
	var wg sync.WaitGroup
	mu := sync.Mutex{}

	// Create done channels
	for _, step := range p.steps {
		done[step.Name()] = make(chan struct{})
	}

	exec := func(step core.Step) {
		defer wg.Done()
		// Wait for all inputs
		for _, input := range p.inputs[step.Name()] {
			parts := strings.Split(input, ":")
			stepName := parts[0]
			outputName := "default"
			if len(parts) == 2 {
				outputName = parts[1]
			}
			<-done[stepName]

			mu.Lock()
			if _, ok := state.Get(stepName, outputName); !ok {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
		logger.Debug("Running step", slog.String("step", step.Name()))

		if p.OnChange != nil {
			p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeStart, StepName: step.Name()})
		}

		outputs, err := step.Run(ctx, state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Step %s failed: %v\n", step.Name(), err)
			return
		}
		state.Set(step.Name(), outputs)
		close(done[step.Name()])
		logger.Debug("Step completed", slog.String("step", step.Name()), slog.Any("output", outputs))
		if p.OnChange != nil {
			p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeEnd, StepName: step.Name(), Data: outputs})
		}
	}

	for _, step := range p.steps {
		wg.Add(1)
		go exec(step)
	}

	wg.Wait()
	return nil
}
