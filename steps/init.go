package steps

import "go-etl/pipeline"

func init() {
	pipeline.RegisterStepType("file", newFileStep)
	pipeline.RegisterStepType("stdout", newStdoutStep)
	pipeline.RegisterStepType("delay", newDelayStep)
	pipeline.RegisterStepType("uppercase", newUppercaseStep)
}
