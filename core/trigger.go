package core

type Trigger interface {
	Name() string
	SetOnTrigger(func()) error
}

// type TriggerFactory func(name string, config map[string]any) (Trigger, error)
