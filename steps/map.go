package steps

import (
	"context"
	"fmt"
	"go-etl/core"
)

type MapStep struct {
	name   string
	fields map[string]core.InterpolateValue[any]
}

func (m *MapStep) Name() string { return m.name }

func (m *MapStep) Run(ctx context.Context, state *core.PipelineState) (*core.Data, error) {
	fields := make(map[string]any)
	for key, iv := range m.fields {
		resolved, err := iv.Resolve(state)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve field %s: %w", key, err)
		}
		fields[key] = resolved
	}

	return &core.Data{Value: fields}, nil
}

func newMapStep(name string, config map[string]any) (core.Step, error) {
	fields, ok := config["fields"]
	if !ok {
		return nil, core.ErrMissingConfig("fields")
	}
	fieldMap, ok := fields.([]interface{})
	if !ok {
		return nil, fmt.Errorf("fields must be a list of maps, got %T", fields)
	}

	fmt.Printf("Creating MapStep with name: %s, fields: %v\n", name, fieldMap)

	mapStepFields := make(map[string]core.InterpolateValue[any])

	for _, field := range fieldMap {
		nameValue, ok := field.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("each field must be a map, got %T", field)
		}
		name, ok := nameValue["name"].(string)
		if !ok {
			return nil, fmt.Errorf("field map must contain a 'name' key with a string value, got %T", nameValue["name"])
		}
		value, ok := nameValue["value"]
		if !ok {
			return nil, fmt.Errorf("field map must contain a 'value' key, got %v", nameValue)
		}
		typeValue, ok := nameValue["type"].(string)
		if !ok {
			typeValue = "string" // Default type if not specified
		}

		switch typeValue {
		case "int":
			mapStepFields[name] = core.InterpolateValue[any]{Raw: value, TargetType: "int"}
		case "bool":
			mapStepFields[name] = core.InterpolateValue[any]{Raw: value, TargetType: "bool"}
		default:
			mapStepFields[name] = core.InterpolateValue[any]{Raw: value, TargetType: "string"}
		}
	}
	return &MapStep{name: name, fields: mapStepFields}, nil
}
