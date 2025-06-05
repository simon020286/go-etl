package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"go-etl/core"

	"gopkg.in/yaml.v3"
)

type Pipeline struct {
	steps    map[string]core.Step
	triggers map[string]core.Trigger
	inputs   map[string][]string
	state    *core.PipelineState
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
	triggersMap := make(map[string]core.Trigger)
	inputs := make(map[string][]string)

	for _, sc := range config.Steps {
		factoryType, factory, ok := GetFactory(sc.Type)
		if !ok {
			return nil, fmt.Errorf("unknown step type: %s", sc.Type)
		}
		step, err := factory(sc.Name, sc.Config)
		if err != nil {
			return nil, err
		}
		if factoryType == "trigger" {
			triggersMap[sc.Name] = step.(core.Trigger)
		} else {
			stepsMap[sc.Name] = step
		}

		slog.Info("Load", slog.String("step", sc.Name), slog.String("type", factoryType))

		inputs[sc.Name] = sc.Inputs
	}

	return &Pipeline{steps: stepsMap, triggers: triggersMap, inputs: inputs}, nil
}

func (p *Pipeline) Run(ctx context.Context, logger *slog.Logger) error {
	if p.state == nil {
		p.state = &core.PipelineState{Results: make(map[string]map[string]*core.Data), Logger: logger}
	}

	core.StartWebServer()

	if len(p.triggers) > 0 {
		logger.Info("Found", slog.Int("triggers", len(p.triggers)))
		p.RunFromTriggers()
		return nil
	}

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
			if _, ok := p.state.Get(stepName, outputName); !ok {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
		logger.Debug("Running step", slog.String("step", step.Name()))

		if p.OnChange != nil {
			p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeStart, StepName: step.Name()})
		}

		outputs, err := step.Run(ctx, p.state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Step %s failed: %v\n", step.Name(), err)
			return
		}
		p.state.Set(step.Name(), outputs)
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

func (p *Pipeline) SetState(state *core.PipelineState) {
	p.state = state
}

func (p *Pipeline) RunFromTriggers() {
	// wg := sync.WaitGroup{}
	// wg.Add(1)
	for _, trigger := range p.triggers {
		slog.Info("Trigger", "name", trigger.Name())
		trigger.SetOnTrigger(func() {
			newP := Pipeline{
				steps: p.steps,
			}

			go func() {
				_ = newP.Run(context.Background(), p.state.Logger)
				slog.Info("Pipe line ended", slog.String("trigger", trigger.Name()))
			}()
		})
	}
	slog.Info("Waiting for triggers")
	select {}
	// wg.Wait()
}
