# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Building the Application
```bash
go build -o etl
```

### Running the Application
```bash
# Run with a pipeline file
./etl -file pipeline.yml

# Start web server mode
./etl -web

# Set log level (debug, info, warn, error)
./etl -file pipeline.yml -log info
```

### Testing
```bash
# Run all tests
go test ./tests/

# Run tests with plugin builds enabled
NOBUILD=1 go test ./tests/

# Run specific test
go test ./tests/ -run TestDelay
```

### Dependencies
```bash
go mod tidy
```

## Architecture Overview

This is a modular ETL (Extract-Transform-Load) pipeline engine built in Go with a plugin architecture.

### Core Components

- **main.go**: Entry point with CLI flag parsing for file mode (`-file`) or web mode (`-web`)
- **core/**: Core abstractions and types
  - `Step` interface: Defines pipeline steps with `Run(ctx, state) (outputs, error)`
  - `Trigger` interface: Defines triggers like webhooks that start pipelines
  - `PipelineState`: Thread-safe state management for step outputs using `Results map[string]map[string]*Data`
  - `Data`: Wrapper for step output values
- **pipeline/**: Pipeline orchestration and configuration
  - `Pipeline` struct: Manages steps, triggers, inputs, and execution flow
  - `LoadPipelineFromFile()`: Loads YAML pipeline configurations
  - Registry system for step/trigger factories via `RegisterStepType()` and `RegisterTriggerType()`
- **steps/**: Built-in step implementations (delay, stdout, uppercase, if, foreach, webhook, file, map, etc.)
- **plugins/**: External plugin system with SDK for extending functionality
- **web/**: Web server for pipeline management interface
- **tests/**: Test suite with examples of step testing patterns

### Pipeline Execution Model

- Steps declare dependencies via `inputs` array referencing other step outputs
- Concurrent execution with dependency resolution using goroutines and channels
- Step outputs stored in shared `PipelineState` accessible as `ctx.stepname.outputname`
- JavaScript templating via Goja for dynamic configuration values

### Step Registration Pattern

Steps self-register in `init()` functions using:
```go
pipeline.RegisterStepType("stepname", factory)
pipeline.RegisterTriggerType("triggername", factory)
```

### Plugin System

- External plugins communicate via stdin/stdout JSON protocol
- Plugin SDK in `plugins/sdk/` provides Go interface
- Plugins built as separate binaries and invoked by `exec_plugin` step type

### Configuration Format

Pipeline configurations use YAML with JavaScript templating:
```yaml
steps:
  - name: stepname
    type: steptype
    inputs: ["dependency1", "dependency2:outputname"]
    config:
      key: "ctx.previousstep.output"
```

### Testing Approach

- Unit tests in `tests/` directory test individual step factories and execution
- Tests use `pipeline.GetStepFactory()` to obtain step instances
- Some tests require built plugins and check `NOBUILD` environment variable