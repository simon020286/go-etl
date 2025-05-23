package models

type PipelineConfig struct {
	Steps []StepConfig `yaml:"steps"`
}
