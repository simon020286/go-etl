package core

import "fmt"

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
