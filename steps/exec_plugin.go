package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go-etl/core"
	"go-etl/pipeline"
	"maps"
	"os"
	"os/exec"
	"path"

	sdk "go-etl-sdk"
)

type ExecPluginStep struct {
	name          string
	command       string
	otherConfig   map[string]any
	configuration sdk.Configuration
}

func (e *ExecPluginStep) Name() string { return e.name }

func (e *ExecPluginStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	resolvedConfig := make(map[string]any)

	for key, value := range e.configuration.Inputs {
		conf := e.otherConfig[key]
		resolvedConfig[key] = conf
		if value.Interpolation {

			interpolatedValue := core.InterpolateFromType(conf, value.Type)
			v, err := interpolatedValue.Resolve(state)
			if err != nil {
				return nil, core.ErrInterpolate(key, conf)
			}
			resolvedConfig[key] = v
		}
	}

	payload, err := json.Marshal(resolvedConfig)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, e.command)
	cmd.Stdin = bytes.NewReader(payload)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	var result any
	state.Logger.Debug("exec_plugin step output", "name", e.name, "output", out.String())
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		return nil, err
	}

	return core.CreateDefaultResultData(result), nil

}

func init() {
	pipeline.RegisterStepType("plugin", func(name string, config map[string]any) (core.Step, error) {
		commandPath, ok := config["command"].(string)
		if !ok {
			return nil, errors.New("missing 'path' in file step")
		}
		otherConfig := maps.Clone(config)
		delete(otherConfig, "command")

		_, err := os.Stat(commandPath)

		if err != nil {
			return nil, err
		}

		commandDir := path.Dir(commandPath)
		settingsPath := path.Join(commandDir, "plugin.json")
		if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
			return nil, errors.New("plugin settings file does not exist: " + settingsPath)
		}

		configuration, err := sdk.ReadConfiguration(settingsPath)
		if err != nil {
			return nil, errors.New("failed to read plugin settings: " + err.Error())
		}

		for key, value := range configuration.Inputs {
			if value.Required {
				if _, exists := otherConfig[key]; !exists {
					return nil, errors.New("missing required input: " + key)
				}
			}
		}

		return &ExecPluginStep{name: name, command: commandPath, otherConfig: otherConfig, configuration: configuration}, nil
	})
}
