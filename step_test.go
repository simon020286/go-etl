package main

import (
	"context"
	"go-etl/core"
	"go-etl/pipeline"
	_ "go-etl/steps"
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
	stepFactory, ok := pipeline.GetStepFactory("uppercase")
	if !ok {
		t.Errorf("Step type 'uppercase' not registered")
	}

	stepInstance, err := stepFactory("testUppercase", map[string]any{
		"value": "hello world",
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
				"value": "code1",
			},
			map[string]any{
				"name":  "description",
				"value": "description1",
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
	state := &core.PipelineState{}
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
