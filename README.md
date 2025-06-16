# ETL Pipeline Engine

> A flexible and modular ETL (Extract-Transform-Load) engine written in Go.

This project was created for personal and educational purposes to learn Go (Golang), so development may be intermittent.

### Getting Started

#### 1. Build

```bash
go build -o etl
```

#### 2. Start
```bash
etl -file pipeline.yml
# or
etl -web
```

### Available Steps

| Type        | Description                                     |
| ----------- | ----------------------------------------------- |
| `stdout`    | Prints a value to the console                   |
| `uppercase` | Converts a string to uppercase                  |
| `delay`     | Waits a number of milliseconds                  |
| `if`        | Conditional step with true/false branches       |
| `foreach`   | Iterates over array input, spawns sub-pipelines |
| `webhook`   | Trigger step that starts pipelines via HTTP     |


#### Webhook trigger
```yaml
name: StepName
type: webhook
config:
    method: GET|POST (default GET)
    path: (default step name)
```

#### File
```yaml
name: StepName
type: file
config:
    path: file path
```

Documentation for the other steps will be available soon.