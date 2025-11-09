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
	steps         map[string]core.Step
	triggers      map[string]core.Trigger
	inputs        map[string][]string
	state         *core.PipelineState
	OnChange      func(event core.ChangeEvent)
	OnTriggerFire func(triggerName string, data map[string]*core.Data) (*core.PipelineState, error) // NEW: callback to create new execution
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

func LoadPipelineFromYAML(yamlContent string) (*Pipeline, error) {
	var config PipelineConfig
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	if err != nil {
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
		p.state = &core.PipelineState{
			Results: make(map[string]map[string]*core.Data),
			Logger:  logger,
			// ExecutionID set externally by PipelineStateManager
		}
	}

	// Identify root triggers (without inputs)
	rootTriggers := p.getRootTriggers()

	if len(rootTriggers) > 0 {
		// CONTINUOUS mode: root triggers create multiple executions
		logger.Info("Running in continuous mode", slog.Int("root_triggers", len(rootTriggers)))
		return p.runContinuous(ctx, logger, rootTriggers)
	}

	// ONE-SHOT mode: normal execution (triggers included as steps)
	logger.Info("Running in one-shot mode", slog.Int("steps", len(p.steps)), slog.Int("mid_triggers", len(p.triggers)))
	return p.runOnce(ctx, logger)
}

// runOnce executes the pipeline once, treating triggers as normal steps
// excludeTriggers: list of trigger names to exclude from execution (e.g., root triggers already running)
func (p *Pipeline) runOnce(ctx context.Context, logger *slog.Logger, excludeTriggers ...string) error {
	done := make(map[string]chan struct{})
	var wg sync.WaitGroup
	mu := sync.Mutex{}

	// Build exclusion set for fast lookup
	excludeSet := make(map[string]bool)
	for _, name := range excludeTriggers {
		excludeSet[name] = true
	}

	// IMPORTANT: Consider triggers as normal steps
	allSteps := make(map[string]core.Step)
	for name, step := range p.steps {
		allSteps[name] = step
	}
	// Add triggers as steps (excluding root triggers that are already running)
	for name, trigger := range p.triggers {
		if !excludeSet[name] {
			allSteps[name] = trigger.(core.Step) // Trigger implements Step interface
		}
	}

	// Create done channels for all (steps + triggers)
	for name := range allSteps {
		done[name] = make(chan struct{})
	}

	// Pre-populate channels for existing state (e.g., trigger data from continuous mode)
	for stepName := range p.state.Results {
		if _, exists := done[stepName]; !exists {
			ch := make(chan struct{})
			close(ch)
			done[stepName] = ch
		}
	}

	// Step executor (same as existing code)
	exec := func(step core.Step) {
		defer wg.Done()

		// Wait for dependencies
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
				logger.Warn("Step dependency not satisfied",
					slog.String("step", step.Name()),
					slog.String("dependency", stepName))
				return
			}
			mu.Unlock()
		}

		logger.Debug("Running step", slog.String("step", step.Name()))

		if p.OnChange != nil {
			p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeStart, StepName: step.Name()})
		}

		// EXECUTE STEP (triggers block here until fire if mid-pipeline)
		outputs, err := step.Run(ctx, p.state)
		if err != nil {
			logger.Error("Step failed", slog.String("step", step.Name()), slog.Any("error", err))
			return
		}

		p.state.Set(step.Name(), outputs)
		close(done[step.Name()])

		logger.Debug("Step completed", slog.String("step", step.Name()), slog.Any("output", outputs))

		if p.OnChange != nil {
			p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeEnd, StepName: step.Name(), Data: outputs})
		}
	}

	// Launch all steps (including mid-pipeline triggers) concurrently
	for _, step := range allSteps {
		wg.Add(1)
		go exec(step)
	}

	wg.Wait()
	return nil
}

// runContinuous handles continuous mode for root triggers
func (p *Pipeline) runContinuous(ctx context.Context, logger *slog.Logger, rootTriggers []core.Trigger) error {
	// NOTE: This method is called only when there are root triggers (without input)
	// Each trigger fire creates a NEW EXECUTION in the DB via OnTriggerFire callback

	// Collect root trigger names to exclude from runOnce execution
	rootTriggerNames := make([]string, 0, len(rootTriggers))
	for _, trigger := range rootTriggers {
		rootTriggerNames = append(rootTriggerNames, trigger.Name())
	}

	for _, trigger := range rootTriggers {
		t := trigger
		logger.Info("Setting up continuous trigger", slog.String("trigger", t.Name()))

		t.SetOnTrigger(func(data map[string]*core.Data) {
			logger.Info("Trigger fired", slog.String("trigger", t.Name()))

			var newState *core.PipelineState

			// Use callback to create new execution if available
			if p.OnTriggerFire != nil {
				state, err := p.OnTriggerFire(t.Name(), data)
				if err != nil {
					logger.Error("Failed to create execution for trigger",
						slog.String("trigger", t.Name()),
						slog.Any("error", err))
					return
				}
				newState = state
			} else {
				// Fallback: create state without execution ID (for backward compatibility)
				newState = &core.PipelineState{
					Results: map[string]map[string]*core.Data{
						t.Name(): data,
					},
					Logger: logger,
				}
			}

			// Save original state
			origState := p.state
			p.state = newState

			go func() {
				// Exclude root triggers from execution (they're already running continuously)
				err := p.runOnce(ctx, logger, rootTriggerNames...)
				if err != nil {
					logger.Error("Trigger execution failed",
						slog.String("trigger", t.Name()),
						slog.Any("error", err))
				}

				// Restore original state
				p.state = origState
			}()
		})
	}

	logger.Info("Waiting for triggers", slog.Int("count", len(rootTriggers)))
	<-ctx.Done()
	logger.Info("Pipeline cancelled, stopping triggers")

	for _, trigger := range rootTriggers {
		if err := trigger.Stop(); err != nil {
			logger.Error("Failed to stop trigger",
				slog.String("trigger", trigger.Name()),
				slog.Any("error", err))
		}
	}

	return nil
}

func (p *Pipeline) SetState(state *core.PipelineState) {
	p.state = state
}

// isRootTrigger verifies if a trigger is at the beginning of the pipeline (no dependencies)
func (p *Pipeline) isRootTrigger(triggerName string) bool {
	inputs := p.inputs[triggerName]
	return len(inputs) == 0
}

// getRootTriggers returns all triggers without dependencies
func (p *Pipeline) getRootTriggers() []core.Trigger {
	roots := []core.Trigger{}
	for name, trigger := range p.triggers {
		if p.isRootTrigger(name) {
			roots = append(roots, trigger)
		}
	}
	return roots
}


// GetTriggers returns the triggers map
func (p *Pipeline) GetTriggers() map[string]core.Trigger {
	return p.triggers
}
