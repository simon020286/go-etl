package core

import "fmt"

type MissingConfigError struct {
	Key string
}

func (e *MissingConfigError) Error() string {
	return "missing required configuration key: " + e.Key
}

func ErrMissingConfig(key string) error {
	return &MissingConfigError{Key: key}
}

type InterpolateError struct {
	Key   string
	Value any
}

func (e *InterpolateError) Error() string {
	return fmt.Sprintf("failed to interpolate value for key '%s': %v", e.Key, e.Value)
}

func ErrInterpolate(key string, value any) error {
	return &InterpolateError{Key: key, Value: value}
}
