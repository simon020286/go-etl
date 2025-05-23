package models

type StepConfig struct {
	Name   string                 `yaml:"name"`
	Type   string                 `yaml:"type"`
	Inputs []string               `yaml:"inputs"`
	Config map[string]interface{} `yaml:"config"`
}
