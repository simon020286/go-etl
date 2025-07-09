package core

import (
	"log/slog"
	"sync"
)

// PipelineState holds results of executed steps
type PipelineState struct {
	Results map[string]map[string]*Data
	mu      sync.RWMutex
	Logger  *slog.Logger
}

func (ps *PipelineState) Get(stepName, outputName string) (*Data, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	stepOutputs, ok := ps.Results[stepName]
	if !ok {
		return nil, false
	}
	data, ok := stepOutputs[outputName]
	return data, ok
}

func (ps *PipelineState) Set(name string, data map[string]*Data) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.Results[name] = data
}
