package core

import "github.com/dop251/goja"

// InterpolateValue is a generic type for values that support interpolation
type InterpolateValue[T any] struct {
	Raw        any
	TargetType string // Optional, can be used to specify the type of the value
}

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

func InterpolateFromType(raw any, targetType string) InterpolateValue[any] {
	return InterpolateValue[any]{Raw: raw, TargetType: targetType}
}
