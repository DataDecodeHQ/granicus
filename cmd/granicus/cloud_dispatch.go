package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// cloudGet performs a GET request to the cloud API and returns the response body.
func cloudGet(path string) ([]byte, error) {
	endpoint := CloudEndpoint()
	apiKey := CloudAPIKey()
	if endpoint == "" {
		return nil, fmt.Errorf("cloud endpoint not configured; run: granicus login")
	}

	req, err := http.NewRequest("GET", endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// cloudPost performs a POST request with JSON body to the cloud API.
func cloudPost(path string, payload any) ([]byte, error) {
	endpoint := CloudEndpoint()
	apiKey := CloudAPIKey()
	if endpoint == "" {
		return nil, fmt.Errorf("cloud endpoint not configured; run: granicus login")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", endpoint+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// isCloudMode checks if we're in cloud mode without requiring flags.
func isCloudMode() bool {
	cfg, _ := config.ResolveCLIConfig("", "", "")
	return resolveMode(cfg, false) == ModeCloud
}
