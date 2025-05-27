package main

import (
	"context"
	"go-etl/pipeline"
	_ "go-etl/steps"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // o slog.LevelInfo
	}))

	f, err := os.Open("pipeline.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var config pipeline.PipelineConfig
	dec := yaml.NewDecoder(f)
	if err := dec.Decode(&config); err != nil {
		panic(err)
	}

	pl, err := pipeline.LoadPipeline(config)
	if err != nil {
		panic(err)
	}

	if err := pl.Run(context.Background(), logger); err != nil {
		panic(err)
	}
}
