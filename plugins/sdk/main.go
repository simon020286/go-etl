package sdk

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func ReadInput[T any]() (T, error) {
	var input T
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return input, fmt.Errorf("read error: %w", err)
	}

	if err := json.Unmarshal(data, &input); err != nil {
		return input, fmt.Errorf("invalid json input: %w", err)
	}

	return input, nil
}

func WriteOutput[T any](output T) {
	outputData, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "json output error: %v\n", err)
		os.Exit(1)
	}

	os.Stdout.Write(outputData)
}

func ReadConfiguration(path string) (Configuration, error) {
	var config Configuration
	data, err := os.ReadFile(path)
	if err != nil {
		return config, fmt.Errorf("read error: %w", err)
	}

	if err := json.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("invalid json configuration: %w", err)
	}

	return config, nil
}

type Configuration struct {
	Name    string           `json:"name"`
	Version string           `json:"version"`
	Inputs  map[string]Input `json:"inputs"`
}

type Input struct {
	Type          string `json:"type"`
	Label         string `json:"label"`
	Default       any    `json:"default,omitempty"`
	Interpolation bool   `json:"interpolation,omitempty"`
	Required      bool   `json:"required,omitempty"`
}
