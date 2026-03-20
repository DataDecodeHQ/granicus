package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

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

// cloudPostMultipart sends a multipart/form-data POST to the cloud API.
// fieldName is the form field name for the file, filePath is the local file to upload.
// Additional key-value pairs can be passed as fields.
func cloudPostMultipart(path, fieldName, filePath string, fields map[string]string) ([]byte, error) {
	endpoint := CloudEndpoint()
	apiKey := CloudAPIKey()
	if endpoint == "" {
		return nil, fmt.Errorf("cloud endpoint not configured; run: granicus login")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Add file part
	part, err := writer.CreateFormFile(fieldName, filepath.Base(filePath))
	if err != nil {
		return nil, fmt.Errorf("creating form file: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, fmt.Errorf("copying file: %w", err)
	}

	// Add other fields
	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("writing field %s: %w", k, err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing multipart writer: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint+path, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// cloudDelete performs a DELETE request to the cloud API.
func cloudDelete(path string) ([]byte, error) {
	endpoint := CloudEndpoint()
	apiKey := CloudAPIKey()
	if endpoint == "" {
		return nil, fmt.Errorf("cloud endpoint not configured; run: granicus login")
	}

	req, err := http.NewRequest("DELETE", endpoint+path, nil)
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

// isCloudMode checks if we're in cloud mode without requiring flags.
func isCloudMode() bool {
	cfg, _ := config.ResolveCLIConfig("", "", "")
	return resolveMode(cfg, false) == ModeCloud
}
