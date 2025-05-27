package pipeline

import (
	"context"
	"fmt"
	"go-etl/core"
	"log/slog"
	"os"
	"sync"
)

type Pipeline struct {
	steps    map[string]core.Step
	graph    map[string][]string
	inputs   map[string][]string
	OnChange func(step string, data *core.Data)
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
	state := &core.PipelineState{Results: make(map[string]*core.Data)}

	var wg sync.WaitGroup
	done := make(map[string]chan struct{})
	for name := range p.steps {
		done[name] = make(chan struct{})
	}

	exec := func(name string) {
		defer wg.Done()
		for _, dep := range p.inputs[name] {
			<-done[dep]
		}

		logger.Debug("Running step", slog.String("step", name))
		out, err := p.steps[name].Run(ctx, state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Step %s failed: %v\n", name, err)
		}

		logger.Debug("Step completed", slog.String("step", name), slog.Any("output", out))

		state.Set(name, out)
		close(done[name])
		if p.OnChange != nil {
			p.OnChange(name, out)
		}
	}

	for name := range p.steps {
		wg.Add(1)
		go exec(name)
	}
	wg.Wait()
	return nil
}
