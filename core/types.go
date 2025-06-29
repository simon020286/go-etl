package core

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/dop251/goja"
)

type Data struct {
	Value any
}

func (d *Data) String() string {
	return fmt.Sprintf("%v", d.Value)
}

func CreateDefaultResultData(value any) map[string]*Data {
	return CreateResultData("default", value)
}

func CreateResultData(name string, value any) map[string]*Data {
	return map[string]*Data{
		name: {
			Value: value,
		},
	}
}

// InterpolateValue is a generic type for values that support interpolation
type InterpolateValue[T any] struct {
	Raw        any
	TargetType string // Optional, can be used to specify the type of the value
}

// func (iv *InterpolateValue[T]) Resolve(state *PipelineState) (T, error) {
// 	if v, ok := iv.Raw.(T); ok {
// 		// Ensure that if T is string, we don't return here (we want to interpolate strings)
// 		_, isString := any(v).(string)
// 		if !isString {
// 			return v, nil
// 		}
// 	}

// 	ctx := make(map[string]any)
// 	state.mu.RLock()
// 	for stepName, outputs := range state.Results {
// 		for outName, data := range outputs {
// 			if outName == "default" {
// 				ctx[stepName] = data.Value
// 			} else {
// 				// ctx[stepName+"."+outName] = data.Value
// 				if ctx[stepName] == nil {
// 					ctx[stepName] = make(map[string]any)
// 				}
// 				stepCtx, _ := ctx[stepName].(map[string]any)
// 				stepCtx[outName] = data.Value
// 			}
// 		}
// 	}
// 	state.mu.RUnlock()
// 	var t T
// 	tmpl, err := template.New("interpolate").
// 		Funcs(template.FuncMap{
// 			"toJson": func(v any) string {
// 				jsonStr, _ := json.Marshal(v)
// 				return string(jsonStr)
// 			},
// 		}).
// 		Parse(iv.Raw.(string))

// 	if err != nil {
// 		return t, err
// 	}
// 	// var buf []byte
// 	output := new(strings.Builder)
// 	err = tmpl.Execute(output, ctx)
// 	if err != nil {
// 		return t, err
// 	}
// 	out := output.String()
// 	switch any(t).(type) {
// 	case int:
// 		// var parsed int
// 		// _, err := fmt.Sscanf(out, "%d", &parsed)
// 		// return any(parsed).(T), err
// 		v, err := strconv.Atoi(out)
// 		return any(v).(T), err
// 	case string:
// 		return any(out).(T), nil
// 	case bool:
// 		// var parsed bool
// 		// _, err := fmt.Sscanf(out, "%t", &parsed)
// 		// return any(parsed).(T), err
// 		v, err := strconv.ParseBool(out)
// 		return any(v).(T), err
// 	default:
// 		switch iv.TargetType {
// 		case "int":
// 			// var parsed int
// 			// _, err := fmt.Sscanf(out, "%d", &parsed)
// 			// return any(parsed).(T), err
// 			v, err := strconv.Atoi(out)
// 			return any(v).(T), err
// 		case "bool":
// 			// var parsed bool
// 			// _, err := fmt.Sscanf(out, "%t", &parsed)
// 			// return any(parsed).(T), err
// 			v, err := strconv.ParseBool(out)
// 			return any(v).(T), err
// 		case "string":
// 			return any(out).(T), nil
// 		default:
// 			var jsonData T
// 			err := json.Unmarshal([]byte(out), &jsonData)
// 			return jsonData, err
// 		}
// 	}
// }

func (iv *InterpolateValue[T]) Resolve(state *PipelineState) (T, error) {
	if v, ok := iv.Raw.(T); ok {
		// Ensure that if T is string, we don't return here (we want to interpolate strings)
		_, isString := any(v).(string)
		if !isString {
			return v, nil
		}
	}

	runtime := goja.New()
	ctx := make(map[string]any)
	state.mu.RLock()

	for stepName, outputs := range state.Results {
		for outName, data := range outputs {
			if outName == "default" {
				ctx[stepName] = data.Value
			} else {
				// ctx[stepName+"."+outName] = data.Value
				if ctx[stepName] == nil {
					ctx[stepName] = make(map[string]any)
				}
				stepCtx, _ := ctx[stepName].(map[string]any)
				stepCtx[outName] = data.Value
			}
		}
	}
	state.mu.RUnlock()

	var t T
	err := runtime.Set("ctx", ctx)
	if err != nil {
		return t, err
	}

	result, err := runtime.RunString(iv.Raw.(string))
	if err != nil {
		return t, err
	}

	switch any(t).(type) {
	case int:
		return any(result.ToInteger()).(T), nil
	case float64:
		return any(result.ToFloat()).(T), nil
	case string:
		return any(result.String()).(T), nil
	case bool:
		return any(result.ToBoolean()).(T), nil
	default:
		return any(result.Export()).(T), nil
	}
}

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

type ChangeEvent struct {
	StepName string
	Type     ChangeEventType
	Data     map[string]*Data
}

type ChangeEventType string

const (
	ChangeEventTypeStart ChangeEventType = "start"
	ChangeEventTypeEnd   ChangeEventType = "end"
)
