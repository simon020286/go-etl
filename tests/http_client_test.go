package tests

import (
	"context"
	"go-etl/pipeline"
	_ "go-etl/steps"
	"testing"
)

func TestGet(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("http client")
	if !ok {
		t.Errorf("Step type 'http client' not registered")
	}

	stepInstance, err := stepFactory("testGet", map[string]any{
		"url":      "https://jsonplaceholder.typicode.com/posts/1",
		"method":   "GET",
		"headers":  map[string]string{"Accept": "application/json"},
		"response": "json",
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testGet" {
		t.Errorf("Expected step name 'testGet', got '%s'", stepInstance.Name())
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

func TestPost(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("http client")
	if !ok {
		t.Errorf("Step type 'http client' not registered")
	}

	stepInstance, err := stepFactory("testPost", map[string]any{
		"url":      "https://jsonplaceholder.typicode.com/posts",
		"method":   "POST",
		"headers":  map[string]string{"Content-Type": "application/json"},
		"body":     map[string]any{"title": "foo", "body": "bar", "userId": 1},
		"response": "json",
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testPost" {
		t.Errorf("Expected step name 'testPost', got '%s'", stepInstance.Name())
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
