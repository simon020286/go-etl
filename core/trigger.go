package core

type Trigger interface {
	Name() string
	Trigger() error
}

// type TriggerFactory func(name string, config map[string]any) (Trigger, error)
