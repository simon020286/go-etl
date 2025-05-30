package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go-etl/core"
	"maps"
	"os"
	"os/exec"
)

type ExecPluginStep struct {
	name        string
	command     string
	otherConfig map[string]any
}

func (e *ExecPluginStep) Name() string { return e.name }

func (e *ExecPluginStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	payload, err := json.Marshal(e.otherConfig)
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

func newExecPluginStep(name string, config map[string]any) (core.Step, error) {
	path, ok := config["command"].(string)
	if !ok {
		return nil, errors.New("missing 'path' in file step")
	}
	otherConfig := maps.Clone(config)
	delete(otherConfig, "command")
	return &ExecPluginStep{name: name, command: path, otherConfig: otherConfig}, nil
}
