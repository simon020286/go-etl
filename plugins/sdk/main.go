package sdk

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func ReadInput[T any]() (T, error) {
	var input T
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return input, fmt.Errorf("read error: %w", err)
	}

	if err := json.Unmarshal(data, &input); err != nil {
		return input, fmt.Errorf("invalid json input: %w", err)
	}

	return input, nil
}

func WriteOutput[T any](output T) {
	outputData, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "json output error: %v\n", err)
		os.Exit(1)
	}

	os.Stdout.Write(outputData)
}
