package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type PluginInput struct {
	Value string `json:"value"`
}

type PluginOutput struct {
	Value string `json:"value"`
}

func main() {
	inputData, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}

	var input PluginInput
	if err := json.Unmarshal(inputData, &input); err != nil {
		fmt.Fprintf(os.Stderr, "invalid json input: %v\n", err)
		os.Exit(1)
	}

	upper := strings.ToUpper(input.Value)
	output := PluginOutput{Value: upper}

	outputData, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "json output error: %v\n", err)
		os.Exit(1)
	}

	os.Stdout.Write(outputData)
}
