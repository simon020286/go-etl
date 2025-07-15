package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go-etl/core"
	"io"
	"net/http"
)

type HTTPClientStep struct {
	name     string
	url      string
	method   string
	headers  map[string]string
	body     any
	response string
}

type HTTPClientResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       any
}

func (h *HTTPClientStep) Name() string { return h.name }

func (h *HTTPClientStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	client := &http.Client{}

	var bodyData io.Reader = nil
	contentType, ok := h.headers["Content-Type"]
	if ok && contentType == "application/json" {
		if h.body != nil {
			bodyDataBytes, err := json.Marshal(h.body)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal body: %w", err)
			}
			bodyData = io.NopCloser(bytes.NewReader(bodyDataBytes))
		}
	} else {
		// For non-JSON bodies, we can use a different approach if needed
		bodyData = nil // No body for non-JSON requests
		if h.body != nil {
			return nil, fmt.Errorf("body is not supported for non-JSON requests")
		}
	}

	// bodyData, err := json.Marshal(h.body)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to marshal body: %w", err)
	// }
	req, err := http.NewRequest(h.method, h.url, bodyData)
	if err != nil {
		return nil, err
	}

	// Set headers
	for key, value := range h.headers {
		req.Header.Set(key, value)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", res.StatusCode)
	}

	responseData := &HTTPClientResponse{
		StatusCode: res.StatusCode,
		Headers:    make(map[string]string),
	}

	for key, values := range res.Header {
		responseData.Headers[key] = values[0] // Use the first value for simplicity
	}

	switch h.response {
	case "json":
		var bodyData map[string]any
		if err := json.NewDecoder(res.Body).Decode(&bodyData); err != nil {
			return nil, fmt.Errorf("failed to decode JSON response: %w", err)
		}
		responseData.Body = bodyData
	case "text":
		var bodyData string
		if err := json.NewDecoder(res.Body).Decode(&bodyData); err != nil {
			return nil, fmt.Errorf("failed to decode text response: %w", err)
		}
		responseData.Body = bodyData
	}

	return core.CreateDefaultResultData(responseData), nil
}

func newHTTPClientStep(name string, config map[string]any) (core.Step, error) {
	url, ok := config["url"].(string)
	if !ok {
		return nil, core.ErrMissingConfig("missing or invalid 'url' in HTTP client step")
	}

	method, ok := config["method"].(string)
	if !ok {
		return nil, core.ErrMissingConfig("missing or invalid 'method' in HTTP client step")
	}

	headers, ok := config["headers"].(map[string]string)
	if !ok {
		headers = make(map[string]string) // Default to empty headers if not provided
	}

	response, ok := config["response"].(string)
	if !ok {
		response = "json"
	}

	body := config["body"] // Body can be optional

	return &HTTPClientStep{
		name:     name,
		url:      url,
		method:   method,
		headers:  headers,
		body:     body,
		response: response,
	}, nil
}
