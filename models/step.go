package models

import "context"

type Data struct {
	Value any
}

// Step defines the ETL step interface
type Step interface {
	Name() string
	Inputs() []string
	Run(ctx context.Context, inputs map[string]*Data) (*Data, error)
}
