package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"
)

type Data struct {
	Value any
}

type Step interface {
	Name() string
	Run(ctx context.Context, state *PipelineState) (*Data, error)
}

type StepFactory func(name string, config map[string]any) (Step, error)

// InterpolateValue is a generic type for values that support interpolation
type InterpolateValue[T any] struct {
	Raw any
}

func (iv *InterpolateValue[T]) Resolve(state *PipelineState) (T, error) {
	if v, ok := iv.Raw.(T); ok {
		// Ensure that if T is string, we don't return here (we want to interpolate strings)
		_, isString := any(v).(string)
		if !isString {
			return v, nil
		}
	}

	ctx := make(map[string]any)
	state.mu.RLock()
	for k, v := range state.Results {
		ctx[k] = v.Value
	}
	state.mu.RUnlock()
	var t T
	tmpl, err := template.New("interpolate").Parse(iv.Raw.(string))
	if err != nil {
		return t, err
	}
	// var buf []byte
	output := new(strings.Builder)
	err = tmpl.Execute(output, ctx)
	if err != nil {
		return t, err
	}
	out := output.String()
	switch any(t).(type) {
	case int:
		var parsed int
		_, err := fmt.Sscanf(out, "%d", &parsed)
		return any(parsed).(T), err
	case string:
		return any(out).(T), nil
	case bool:
		var parsed bool
		_, err := fmt.Sscanf(out, "%t", &parsed)
		return any(parsed).(T), err
	default:
		return t, fmt.Errorf("unsupported type")
	}
}

// PipelineState holds results of executed steps
type PipelineState struct {
	Results map[string]*Data
	mu      sync.RWMutex
}

func (ps *PipelineState) Get(name string) (*Data, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	res, ok := ps.Results[name]
	return res, ok
}

func (ps *PipelineState) Set(name string, data *Data) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.Results[name] = data
}
