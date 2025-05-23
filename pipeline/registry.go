package pipeline

import "go-etl/core"

type StepFactory func(name string, config map[string]any) (core.Step, error)

var stepRegistry = make(map[string]StepFactory)

func RegisterStepType(stepType string, factory StepFactory) {
	stepRegistry[stepType] = factory
}
