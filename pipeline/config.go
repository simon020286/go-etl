package pipeline

type PipelineConfig struct {
	Steps []StepConfig `yaml:"steps"`
}

type StepConfig struct {
	Name   string                 `yaml:"name"`
	Type   string                 `yaml:"type"`
	Inputs []string               `yaml:"inputs"`
	Config map[string]interface{} `yaml:"config"`
}
