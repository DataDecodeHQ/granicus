package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// ResolveCredentials returns a file path to the credential file.
// Supports two formats:
//   - File path: "/path/to/creds.json" (returned as-is)
//   - Secret Manager: "secret:<secret-id>" (fetched, written to temp file, path returned)
//
// For Secret Manager references, the GCP project is read from GRANICUS_FIRESTORE_PROJECT.
func ResolveCredentials(creds string) (string, error) {
	if creds == "" {
		return "", nil
	}

	if !strings.HasPrefix(creds, "secret:") {
		return creds, nil
	}

	secretID := strings.TrimPrefix(creds, "secret:")
	project := os.Getenv("GRANICUS_FIRESTORE_PROJECT")
	if project == "" {
		return "", fmt.Errorf("GRANICUS_FIRESTORE_PROJECT required for secret: credential references")
	}

	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("creating secret manager client: %w", err)
	}
	defer client.Close()

	name := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", project, secretID)
	result, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("accessing secret %s: %w", secretID, err)
	}

	tmpDir := os.TempDir()
	credPath := filepath.Join(tmpDir, "granicus-cred-"+secretID+".json")
	if err := os.WriteFile(credPath, result.Payload.Data, 0600); err != nil {
		return "", fmt.Errorf("writing credential file: %w", err)
	}

	slog.Info("credential_resolved", "source", "secret_manager", "secret", secretID)
	return credPath, nil
}

// ResolveConnectionCredentials resolves the credentials for a ConnectionConfig,
// checking the explicit Credentials field first, then falling back to Properties["credentials"].
func ResolveConnectionCredentials(conn *ConnectionConfig) (string, error) {
	creds := conn.Credentials
	if creds == "" {
		creds = conn.Properties["credentials"]
	}
	return ResolveCredentials(creds)
}
