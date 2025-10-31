// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package zerobushttp

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
)

// Config contains the configuration for the ZeroBus HTTP output
type Config struct {
	ZeroBusURI   string            `config:"zerobus_uri" validate:"required"`
	TableName    string            `config:"table_name" validate:"required"`
	PATToken     string            `config:"pat_token"`
	OAuth        *OAuthConfig      `config:"oauth"`
	WorkspaceURL string            `config:"workspace_url" validate:"required"`
	WorkspaceID  string            `config:"workspace_id"`
	Timeout      time.Duration     `config:"timeout"`
	Retry        retryConfig       `config:"retry"`
	TLS          *tlscommon.Config `config:"ssl"`
	Codec        codec.Config      `config:"codec"`
	BatchSize    int               `config:"batch_size"`
	Workers      int               `config:"workers"` // Number of concurrent workers for parallel publishing
	Headers      map[string]string `config:"headers"`
	Queue        config.Namespace  `config:"queue"`
}

// OAuthConfig contains OAuth2 client credentials configuration
type OAuthConfig struct {
	ClientID     string `config:"client_id" validate:"required"`
	ClientSecret string `config:"client_secret" validate:"required"`
}

type retryConfig struct {
	Max     int           `config:"max" validate:"min=0"`
	Backoff time.Duration `config:"backoff" validate:"min=0"`
}

// defaultConfig returns the default configuration for ZeroBus HTTP output
func defaultConfig() Config {
	return Config{
		ZeroBusURI:   "",
		TableName:    "",
		PATToken:     "",
		WorkspaceURL: "",
		Timeout:      30 * time.Second,
		Retry: retryConfig{
			Max:     3,
			Backoff: 1 * time.Second,
		},
		TLS:       nil,
		BatchSize: 1, // Default to one event per request as per ZeroBus API
		Workers:   4, // Default to 4 concurrent workers for parallel publishing
		Headers:   make(map[string]string),
	}
}

// readConfig reads and validates the configuration
func readConfig(cfg *config.C) (*Config, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.ZeroBusURI == "" {
		return errors.New("zerobus_uri is required")
	}

	if c.TableName == "" {
		return errors.New("table_name is required")
	}

	// Validate authentication: exactly one method must be configured
	hasPAT := c.PATToken != ""
	hasOAuth := c.OAuth != nil && c.OAuth.ClientID != "" && c.OAuth.ClientSecret != ""

	if !hasPAT && !hasOAuth {
		return errors.New("authentication is required: either pat_token or oauth (client_id and client_secret) must be configured")
	}

	if hasPAT && hasOAuth {
		return errors.New("only one authentication method can be configured: use either pat_token or oauth, not both")
	}

	// Validate that workspace_id can be determined when OAuth is enabled
	if hasOAuth {
		workspaceID, err := c.getWorkspaceID()
		if err != nil {
			return fmt.Errorf("failed to determine workspace_id for OAuth: %w", err)
		}
		if workspaceID == "" {
			return errors.New("workspace_id is required when using OAuth authentication (either explicitly or via zerobus_uri)")
		}
	}

	if c.WorkspaceURL == "" {
		return errors.New("workspace_url is required")
	}

	// Validate table name format (catalog.schema.table)
	if !isValidTableName(c.TableName) {
		return errors.New("table_name must be in format 'catalog.schema.table'")
	}

	// Validate workspace URL
	if _, err := url.Parse(c.WorkspaceURL); err != nil {
		return fmt.Errorf("invalid workspace_url: %w", err)
	}

	// Clean and validate ZeroBus URI
	cleanURI := strings.TrimPrefix(c.ZeroBusURI, "https://")
	cleanURI = strings.TrimPrefix(cleanURI, "http://")
	if cleanURI == "" {
		return errors.New("zerobus_uri cannot be empty after cleaning protocol")
	}

	// Validate timeout
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	// Validate batch size
	if c.BatchSize <= 0 {
		return errors.New("batch_size must be positive")
	}

	// Validate workers
	if c.Workers <= 0 {
		return errors.New("workers must be positive")
	}

	if c.Workers > 100 {
		return errors.New("workers must not exceed 100")
	}

	return nil
}

// IsOAuthEnabled returns true if OAuth authentication is configured
func (c *Config) IsOAuthEnabled() bool {
	return c.OAuth != nil && c.OAuth.ClientID != "" && c.OAuth.ClientSecret != ""
}

// GetOAuthTokenURL returns the OAuth token endpoint URL
func (c *Config) GetOAuthTokenURL() string {
	// Clean workspace URL and append the OIDC token endpoint
	workspaceURL := strings.TrimSuffix(c.WorkspaceURL, "/")
	return workspaceURL + "/oidc/v1/token"
}

// getWorkspaceID returns the workspace ID, either from explicit configuration
// or extracted from the zerobus_uri hostname
func (c *Config) getWorkspaceID() (string, error) {
	var workspaceID string

	// If explicitly configured, use it
	if c.WorkspaceID != "" {
		workspaceID = c.WorkspaceID
	} else {
		// Extract from zerobus_uri hostname
		// Expected format: <workspace_id>.zerobus.<cloud-details>
		// Example: 1234567890.zerobus.us-east-1.aws.databricks.com
		hostname := c.ZeroBusURI
		// Remove any protocol prefix if present
		hostname = strings.TrimPrefix(hostname, "https://")
		hostname = strings.TrimPrefix(hostname, "http://")

		// Split by dots and check format
		parts := strings.Split(hostname, ".")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid zerobus_uri format: expected <workspace_id>.zerobus.<cloud-details>, got %s", c.ZeroBusURI)
		}

		// Check if second part is "zerobus"
		if parts[1] != "zerobus" {
			return "", fmt.Errorf("invalid zerobus_uri format: expected <workspace_id>.zerobus.<cloud-details>, got %s", c.ZeroBusURI)
		}

		// First part is the workspace ID
		workspaceID = parts[0]
		if workspaceID == "" {
			return "", fmt.Errorf("empty workspace_id extracted from zerobus_uri: %s", c.ZeroBusURI)
		}
	}

	// Validate that workspace_id is a valid number (int64)
	if _, err := strconv.ParseInt(workspaceID, 10, 64); err != nil {
		return "", fmt.Errorf("workspace_id must be a valid number, got '%s': %w", workspaceID, err)
	}

	return workspaceID, nil
}

// GetOAuthResource returns the OAuth resource string for Databricks
func (c *Config) GetOAuthResource() string {
	workspaceID, _ := c.getWorkspaceID() // Safe to ignore error as it's validated during config validation
	return fmt.Sprintf("api://databricks/workspaces/%s/zerobusDirectWriteApi", workspaceID)
}

// buildURL constructs the ZeroBus ingest URL
func (c *Config) buildURL() string {
	// Clean the URI by removing protocol if present
	cleanURI := strings.TrimPrefix(c.ZeroBusURI, "https://")
	cleanURI = strings.TrimPrefix(cleanURI, "http://")

	return fmt.Sprintf("https://%s/ingest-record?table_name=%s",
		cleanURI, url.QueryEscape(c.TableName))
}

// isValidTableName validates that the table name follows catalog.schema.table format
func isValidTableName(tableName string) bool {
	parts := parseTableName(tableName)
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != ""
}

// parseTableName parses a table name into catalog, schema, and table parts
// Handles backtick-quoted identifiers (e.g., `catalog`.schema.`table`)
func parseTableName(tableName string) []string {
	var parts []string
	var current strings.Builder
	inBackticks := false

	for i := 0; i < len(tableName); i++ {
		ch := tableName[i]

		switch ch {
		case '`':
			inBackticks = !inBackticks
		case '.':
			if inBackticks {
				current.WriteByte(ch)
			} else {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}

	// Add the last part
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// GetAuthorizationDetails returns the authorization_details JSON for OAuth
func (c *Config) GetAuthorizationDetails() string {
	parts := parseTableName(c.TableName)
	if len(parts) != 3 {
		// Fallback to empty if parsing fails (shouldn't happen after validation)
		return "[]"
	}

	catalog := parts[0]
	schema := parts[1]
	table := parts[2]

	// Build compact JSON array
	return fmt.Sprintf(`[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"%s"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"%s.%s"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"%s.%s.%s"}]`,
		catalog, catalog, schema, catalog, schema, table)
}
