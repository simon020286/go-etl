package pipeline

import (
	"context"
	"fmt"
	"go-etl/core"
	"os"
	"sync"
)

type Pipeline struct {
	steps  map[string]core.Step
	graph  map[string][]string
	inputs map[string][]string
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

func (p *Pipeline) Run(ctx context.Context) error {
	results := make(map[string]*core.Data)
	var mu sync.Mutex
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

		inputData := make(map[string]*core.Data)
		mu.Lock()
		for _, dep := range p.inputs[name] {
			inputData[dep] = results[dep]
		}
		mu.Unlock()

		out, err := p.steps[name].Run(ctx, inputData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Step %s failed: %v\n", name, err)
		}

		mu.Lock()
		results[name] = out
		mu.Unlock()
		close(done[name])
	}

	for name, _ := range p.steps {
		wg.Add(1)
		go exec(name)
	}
	wg.Wait()
	return nil
}
