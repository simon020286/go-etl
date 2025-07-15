package steps

import "go-etl/pipeline"

func init() {
	pipeline.RegisterStepType("file", newFileStep)
	pipeline.RegisterStepType("stdout", newStdoutStep)
	pipeline.RegisterStepType("delay", newDelayStep)
	pipeline.RegisterStepType("uppercase", newUppercaseStep)
	pipeline.RegisterStepType("map", newMapStep)
	pipeline.RegisterStepType("if", newIfStep)
	pipeline.RegisterStepType("foreach", newForeachStep)
	pipeline.RegisterStepType("http client", newHTTPClientStep)
	pipeline.RegisterStepType("plugin", newExecPluginStep)

	pipeline.RegisterTriggerType("webhook", newWebhookStep)
}
