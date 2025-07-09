package core

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
