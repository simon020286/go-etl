package tests

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
	_ "go-etl/steps"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestDelay(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("delay")
	if !ok {
		t.Errorf("Step type 'delay' not registered")
	}

	stepInstance, err := stepFactory("testDelay", map[string]any{
		"ms": 100,
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testDelay" {
		t.Errorf("Expected step name 'testDelay', got '%s'", stepInstance.Name())
	}

	ctx := context.Background()
	result, err := stepInstance.Run(ctx, nil)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func TestUppercase(t *testing.T) {
	t.SkipNow()
	stepFactory, ok := pipeline.GetStepFactory("uppercase")
	if !ok {
		t.Errorf("Step type 'uppercase' not registered")
	}

	stepInstance, err := stepFactory("testUppercase", map[string]any{
		"value": "'hello world'",
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testUppercase" {
		t.Errorf("Expected step name 'testUppercase', got '%s'", stepInstance.Name())
	}

	ctx := context.Background()
	state := &core.PipelineState{}
	result, err := stepInstance.Run(ctx, state)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else if result["default"].Value.(string) != strings.ToUpper("hello world") {
		t.Errorf("Expected result 'HELLO WORLD', got '%v'", result)
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func TestMap(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("map")
	if !ok {
		t.Errorf("Step type 'map' not registered")
	}

	stepInstance, err := stepFactory("testMap", map[string]any{
		"fields": []interface{}{
			map[string]any{
				"name":  "code",
				"value": "ctx.input1.code",
			},
			map[string]any{
				"name":  "description",
				"value": "ctx.input1.description",
			}},
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testMap" {
		t.Errorf("Expected step name 'testMap', got '%s'", stepInstance.Name())
	}

	ctx := context.Background()
	input1Result := core.CreateDefaultResultData(map[string]interface{}{
		"code":        "code1",
		"description": "description1",
	})

	state := &core.PipelineState{
		Results: map[string]map[string]*core.Data{
			"input1": input1Result,
		},
	}
	result, err := stepInstance.Run(ctx, state)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		for prop, item := range result["default"].Value.(map[string]interface{}) {
			if prop == "code" && item != "code1" {
				t.Errorf("Expected code 'code1', got '%v'", item)
			} else if prop == "description" && item != "description1" {
				t.Errorf("Expected description 'description1', got '%v'", item)
			}
		}
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func TestUpperCasePlugin(t *testing.T) {
	skipRequireBuild(t)
	stepFactory, ok := pipeline.GetStepFactory("plugin")
	if !ok {
		t.Errorf("Step type 'plugin' not registered")
	}

	stepInstance, err := stepFactory("testUpperCasePlugin", map[string]any{
		"command": "./plugins/uppercase_plugin/uppercase_plugin",
		"value":   "ctx.input1",
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testUpperCasePlugin" {
		t.Errorf("Expected step name 'testUpperCasePlugin', got '%s'", stepInstance.Name())
	}

	ctx := context.Background()
	state := &core.PipelineState{
		Logger: slog.Default(),
		Results: map[string]map[string]*core.Data{
			"input1": {
				"default": {Value: "hello world"},
			},
		},
	}
	result, err := stepInstance.Run(ctx, state)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func skipRequireBuild(t *testing.T) {
	if os.Getenv("NOBUILD") == "" {
		t.Skip("Skipping testing in no build environment")
	}
}
