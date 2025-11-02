# Trigger Implementation Pattern

This document explains the standard pattern for implementing triggers in the ETL system.

## Overview

Triggers are special step types that start pipeline executions in response to external events (HTTP requests, timers, messages, etc.). They implement both the `Step` interface and the `Trigger` interface.

## The Standard Pattern

### 1. Struct Definition

```go
type MyTriggerStep struct {
    name     string
    // Configuration fields (parsed in init)
    config   string

    // Runtime fields (initialized in SetOnTrigger)
    channel  chan MyEvent
    stopChan chan struct{}
}
```

**Key principle**: Separate configuration (set in `init()`) from runtime resources (set in `SetOnTrigger()`).

### 2. Core Methods

#### Name()
Simple getter for the trigger name.

```go
func (s *MyTriggerStep) Name() string {
    return s.name
}
```

#### Run()
For triggers, this method is typically not used (only called for regular steps). Return a placeholder result.

```go
func (s *MyTriggerStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
    return core.CreateDefaultResultData("Trigger fired"), nil
}
```

#### SetOnTrigger() - **THE IMPORTANT ONE**

This is where the trigger setup happens. It should:

1. **Initialize runtime resources** (channels, timers, HTTP handlers, connections, etc.)
2. **Start a goroutine** that waits for events
3. **Call the callback** with trigger data when events occur
4. **Return immediately** after starting the goroutine

```go
func (s *MyTriggerStep) SetOnTrigger(callback func(data map[string]*core.Data)) error {
    // 1. Setup: Create runtime resources HERE, not in init()
    s.channel = make(chan MyEvent)
    s.stopChan = make(chan struct{})

    // Example: Register HTTP endpoint, start timer, open connection, etc.
    // This runs when the pipeline actually starts, not when it's loaded
    setupExternalResources(s)

    // 2. Start event loop goroutine
    go func() {
        for {
            select {
            case event := <-s.channel:
                // 3. Create trigger data
                data := map[string]*core.Data{
                    "default":   {Value: event.Value},
                    "timestamp": {Value: event.Timestamp},
                    // Add other relevant data
                }

                // 4. Call callback to trigger pipeline execution
                callback(data)

            case <-s.stopChan:
                // 5. Cleanup on stop
                cleanup()
                return
            }
        }
    }()

    // 6. Return immediately (goroutine continues)
    return nil
}
```

### 3. Registration (init)

The `init()` function should:

1. **Parse and validate configuration** only
2. **NOT create runtime resources** (channels, connections, timers)
3. **NOT register external handlers** (HTTP endpoints, etc.)
4. **Return a struct** with configuration only

```go
func init() {
    pipeline.RegisterTriggerType("my_trigger", func(name string, config map[string]any) (core.Step, error) {
        // 1. Parse configuration
        myConfig, ok := config["my_config"].(string)
        if !ok {
            return nil, fmt.Errorf("my_trigger requires 'my_config' configuration")
        }

        // 2. Validate configuration (optional but recommended)
        if err := validateConfig(myConfig); err != nil {
            return nil, fmt.Errorf("invalid config: %w", err)
        }

        // 3. Return struct with configuration ONLY
        // NO channels, NO timers, NO HTTP registration here!
        return &MyTriggerStep{
            name:   name,
            config: myConfig,
        }, nil
    })
}
```

## Why This Pattern?

### ❌ Old Pattern (Anti-pattern)
```go
func init() {
    // BAD: Creating runtime resources during pipeline load
    channel := make(chan Event)
    http.HandleFunc("/webhook", handler) // Registered even if pipeline never runs!

    return &WebhookStep{channel: channel}
}
```

**Problems:**
- Memory leak if pipeline loaded but never executed
- Can't load multiple pipelines with same endpoint
- Resources allocated even if not needed
- Inconsistent with other triggers

### ✅ New Pattern (Correct)
```go
func init() {
    // GOOD: Only parse and validate config
    return &WebhookStep{
        name: name,
        path: path, // Store config for later
    }
}

func (s *WebhookStep) SetOnTrigger(callback func(...)) error {
    // GOOD: Setup resources when pipeline actually runs
    s.channel = make(chan Event)
    http.HandleFunc("/webhook/"+s.path, handler)

    go func() {
        for event := range s.channel {
            callback(createData(event))
        }
    }()
    return nil
}
```

**Benefits:**
- No resource leak
- Resources created only when needed
- Consistent pattern across all triggers
- Can load multiple pipelines safely

## Trigger Data Format

Always provide a `"default"` output for ease of access, plus any additional named outputs:

```go
data := map[string]*core.Data{
    "default":   {Value: mainValue},     // Required: easy access via ctx.trigger_name
    "timestamp": {Value: timestamp},     // Optional: additional context
    "metadata":  {Value: metadata},      // Optional: more context
}
```

Users can then access:
- `ctx.trigger_name` or `ctx.trigger_name.default` - main value
- `ctx.trigger_name.timestamp` - additional fields
- `ctx.trigger_name.metadata` - more fields

## Cleanup (Optional)

If your trigger needs cleanup (closing connections, stopping timers), implement a `Stop()` method:

```go
func (s *MyTriggerStep) Stop() {
    if s.stopChan != nil {
        close(s.stopChan)
    }
}
```

Call this from the stop channel case in your event loop.

## Examples

### Cron Trigger (Timer-based)
- **Config in init**: Schedule expression
- **Setup in SetOnTrigger**: Create and start `time.Ticker`
- **Cleanup**: Stop ticker on shutdown

### Webhook Trigger (HTTP-based)
- **Config in init**: Path and method
- **Setup in SetOnTrigger**: Register HTTP handler
- **Cleanup**: None needed (HTTP server manages routes)

### MQTT Trigger (Connection-based)
- **Config in init**: Broker URL, topic, credentials
- **Setup in SetOnTrigger**: Connect to broker, subscribe to topic
- **Cleanup**: Disconnect from broker

## Testing

When testing triggers, mock the callback:

```go
func TestMyTrigger(t *testing.T) {
    called := false
    var receivedData map[string]*core.Data

    trigger := &MyTriggerStep{name: "test", config: "value"}

    err := trigger.SetOnTrigger(func(data map[string]*core.Data) {
        called = true
        receivedData = data
    })

    // Simulate trigger event
    simulateEvent()

    // Wait and verify
    time.Sleep(100 * time.Millisecond)
    assert.True(t, called)
    assert.NotNil(t, receivedData["default"])
}
```

## Summary Checklist

When implementing a new trigger:

- [ ] Struct has configuration fields only (no channels/timers in init)
- [ ] `Name()` returns the trigger name
- [ ] `Run()` returns placeholder result
- [ ] `SetOnTrigger()` creates runtime resources
- [ ] `SetOnTrigger()` starts goroutine with event loop
- [ ] Event loop calls callback with trigger data
- [ ] Trigger data includes `"default"` output
- [ ] `init()` only parses and validates config
- [ ] `init()` does NOT create channels or runtime resources
- [ ] Optional: `Stop()` method for cleanup
- [ ] Tests verify callback is called with correct data
