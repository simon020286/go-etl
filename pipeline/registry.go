package pipeline

import (
	"go-etl/core"
)

type StepFactory func(name string, config map[string]any) (core.Step, error)

var stepRegistry = make(map[string]StepFactory)
var triggerRegistry = make(map[string]StepFactory)

func RegisterStepType(stepType string, factory StepFactory) {
	stepRegistry[stepType] = factory
}

func RegisterTriggerType(stepType string, factory StepFactory) {
	triggerRegistry[stepType] = factory
}

// func RegisterTriggerType(triggerType string, factory core.TriggerFactory) {
// 	stepRegistry[triggerType] = factory
// }

func GetStepFactory(stepType string) (StepFactory, bool) {
	factory, exists := stepRegistry[stepType]
	return factory, exists
}

func GetFactory(stepType string) (string, StepFactory, bool) {
	factoryType := "step"
	factory, exists := stepRegistry[stepType]
	if !exists {
		factory, exists = triggerRegistry[stepType]
		if exists {
			factoryType = "trigger"
		} else {
			return "", nil, false
		}
	}
	return factoryType, factory, exists
}
