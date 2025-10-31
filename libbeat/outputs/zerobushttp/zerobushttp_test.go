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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/mapstr"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
			},
			expectError: false,
		},
		{
			name: "missing zerobus_uri",
			config: map[string]interface{}{
				"table_name":    "unity.default.air_quality",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
			},
			expectError: true,
			errorMsg:    "zerobus_uri is required",
		},
		{
			name: "missing table_name",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
			},
			expectError: true,
			errorMsg:    "table_name is required",
		},
		{
			name: "missing authentication",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
			},
			expectError: true,
			errorMsg:    "authentication is required",
		},
		{
			name: "missing workspace_url",
			config: map[string]interface{}{
				"zerobus_uri": "test-workspace.ingest.cloud.databricks.com",
				"table_name":  "unity.default.air_quality",
				"pat_token":   "test-token",
			},
			expectError: true,
			errorMsg:    "workspace_url is required",
		},
		{
			name: "invalid table_name format",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "invalid_table_name",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
			},
			expectError: true,
			errorMsg:    "table_name must be in format 'catalog.schema.table'",
		},
		{
			name: "invalid workspace_url",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"pat_token":     "test-token",
				"workspace_url": "://invalid-url",
			},
			expectError: true,
			errorMsg:    "invalid workspace_url",
		},
		{
			name: "with optional headers",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"headers": map[string]string{
					"X-Custom-Header": "custom-value",
				},
			},
			expectError: false,
		},
		{
			name: "valid OAuth config",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"workspace_id":  "1234567890123456",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-client-secret",
				},
			},
			expectError: false,
		},
		{
			name: "OAuth missing workspace_id",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-client-secret",
				},
			},
			expectError: true,
			errorMsg:    "workspace_id is required when using OAuth authentication",
		},
		{
			name: "OAuth missing client_id",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"workspace_id":  "1234567890123456",
				"oauth": map[string]interface{}{
					"client_secret": "test-client-secret",
				},
			},
			expectError: true,
			errorMsg:    "authentication is required",
		},
		{
			name: "OAuth missing client_secret",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"workspace_id":  "1234567890123456",
				"oauth": map[string]interface{}{
					"client_id": "test-client-id",
				},
			},
			expectError: true,
			errorMsg:    "authentication is required",
		},
		{
			name: "both PAT and OAuth configured",
			config: map[string]interface{}{
				"zerobus_uri":   "test-workspace.ingest.cloud.databricks.com",
				"table_name":    "unity.default.air_quality",
				"pat_token":     "test-token",
				"workspace_url": "https://test-workspace.cloud.databricks.com",
				"workspace_id":  "1234567890123456",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-client-secret",
				},
			},
			expectError: true,
			errorMsg:    "only one authentication method can be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.NewConfigFrom(tt.config)
			if err != nil {
				t.Fatalf("Failed to create config: %v", err)
			}

			config, err := readConfig(cfg)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}

			err = config.Validate()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Fatalf("Expected error message to contain '%s', got: %s", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestURLConstruction(t *testing.T) {
	tests := []struct {
		name        string
		zerobusURI  string
		tableName   string
		expectedURL string
	}{
		{
			name:        "basic URI",
			zerobusURI:  "test-workspace.ingest.cloud.databricks.com",
			tableName:   "unity.default.air_quality",
			expectedURL: "https://test-workspace.ingest.cloud.databricks.com/ingest-record?table_name=unity.default.air_quality",
		},
		{
			name:        "URI with https protocol",
			zerobusURI:  "https://test-workspace.ingest.cloud.databricks.com",
			tableName:   "unity.default.air_quality",
			expectedURL: "https://test-workspace.ingest.cloud.databricks.com/ingest-record?table_name=unity.default.air_quality",
		},
		{
			name:        "URI with http protocol",
			zerobusURI:  "http://test-workspace.ingest.cloud.databricks.com",
			tableName:   "unity.default.air_quality",
			expectedURL: "https://test-workspace.ingest.cloud.databricks.com/ingest-record?table_name=unity.default.air_quality",
		},
		{
			name:        "table name with special characters",
			zerobusURI:  "test-workspace.ingest.cloud.databricks.com",
			tableName:   "unity.default.air-quality_data",
			expectedURL: "https://test-workspace.ingest.cloud.databricks.com/ingest-record?table_name=unity.default.air-quality_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				ZeroBusURI: tt.zerobusURI,
				TableName:  tt.tableName,
			}

			actualURL := config.buildURL()
			if actualURL != tt.expectedURL {
				t.Fatalf("Expected URL %s, got %s", tt.expectedURL, actualURL)
			}
		})
	}
}

func TestTableNameValidation(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		valid     bool
	}{
		{"valid table name", "unity.default.air_quality", true},
		{"valid with underscores", "catalog.schema.table_name", true},
		{"valid with hyphens", "catalog.schema.table-name", true},
		{"valid with backticks", "`catalog`.schema.`table`", true},
		{"valid with all backticks", "`catalog`.`schema`.`table`", true},
		{"valid with dots in backticks", "`cat.alog`.schema.`tab.le`", true},
		{"invalid - missing parts", "unity.air_quality", false},
		{"invalid - too many parts", "unity.default.air.quality", false},
		{"invalid - empty parts", "unity..air_quality", false},
		{"invalid - empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidTableName(tt.tableName)
			if valid != tt.valid {
				t.Fatalf("Expected %v, got %v for table name '%s'", tt.valid, valid, tt.tableName)
			}
		})
	}
}

func TestTableNameParsing(t *testing.T) {
	tests := []struct {
		name           string
		tableName      string
		expectedParts  []string
	}{
		{
			name:          "simple table name",
			tableName:     "catalog.schema.table",
			expectedParts: []string{"catalog", "schema", "table"},
		},
		{
			name:          "with backticks on catalog and table",
			tableName:     "`catalog`.schema.`table`",
			expectedParts: []string{"catalog", "schema", "table"},
		},
		{
			name:          "all parts with backticks",
			tableName:     "`catalog`.`schema`.`table`",
			expectedParts: []string{"catalog", "schema", "table"},
		},
		{
			name:          "dots inside backticks",
			tableName:     "`cat.alog`.schema.`tab.le`",
			expectedParts: []string{"cat.alog", "schema", "tab.le"},
		},
		{
			name:          "special characters in backticks",
			tableName:     "`cat-alog`.`sche-ma`.`tab-le`",
			expectedParts: []string{"cat-alog", "sche-ma", "tab-le"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := parseTableName(tt.tableName)
			if len(parts) != len(tt.expectedParts) {
				t.Fatalf("Expected %d parts, got %d for table name '%s'", len(tt.expectedParts), len(parts), tt.tableName)
			}
			for i, expected := range tt.expectedParts {
				if parts[i] != expected {
					t.Fatalf("Expected part[%d] to be '%s', got '%s' for table name '%s'", i, expected, parts[i], tt.tableName)
				}
			}
		})
	}
}

func TestAuthorizationDetails(t *testing.T) {
	tests := []struct {
		name         string
		tableName    string
		expectedJSON string
	}{
		{
			name:         "simple table name",
			tableName:    "catalog.schema.table",
			expectedJSON: `[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"catalog"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"catalog.schema"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"catalog.schema.table"}]`,
		},
		{
			name:         "with backticks",
			tableName:    "`catalog`.schema.`table`",
			expectedJSON: `[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"catalog"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"catalog.schema"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"catalog.schema.table"}]`,
		},
		{
			name:         "dots in backticks",
			tableName:    "`cat.alog`.schema.`tab.le`",
			expectedJSON: `[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"cat.alog"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"cat.alog.schema"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"cat.alog.schema.tab.le"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				TableName: tt.tableName,
			}
			authDetails := config.GetAuthorizationDetails()
			if authDetails != tt.expectedJSON {
				t.Fatalf("Expected authorization_details:\n%s\nGot:\n%s", tt.expectedJSON, authDetails)
			}
		})
	}
}

func TestOutputCreation(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method
		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		// Verify required headers
		expectedHeaders := map[string]string{
			"Content-Type":                    "application/json",
			"Authorization":                   "Bearer test-token",
			"unity-catalog-endpoint":          "https://test-workspace.cloud.databricks.com",
			"x-databricks-zerobus-table-name": "unity.default.air_quality",
		}

		for key, expectedValue := range expectedHeaders {
			actualValue := r.Header.Get(key)
			if actualValue != expectedValue {
				t.Errorf("Expected header %s to be %s, got %s", key, expectedValue, actualValue)
			}
		}

		// Verify URL path and query
		expectedPath := "/ingest-record"
		if r.URL.Path != expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		expectedTableName := "unity.default.air_quality"
		actualTableName := r.URL.Query().Get("table_name")
		if actualTableName != expectedTableName {
			t.Errorf("Expected table_name %s, got %s", expectedTableName, actualTableName)
		}

		// Return success response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Successfully ingested record."))
	}))
	defer server.Close()

	// Create configuration
	configMap := map[string]interface{}{
		"zerobus_uri":   strings.TrimPrefix(server.URL, "http://"),
		"table_name":    "unity.default.air_quality",
		"pat_token":     "test-token",
		"workspace_url": "https://test-workspace.cloud.databricks.com",
		"timeout":       "10s",
	}

	cfg, err := config.NewConfigFrom(configMap)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	// Create beat info
	beatInfo := beat.Info{
		Beat:    "test-beat",
		Version: "7.0.0",
		Logger:  logp.NewLogger("test"),
	}

	// Create observer
	observer := outputs.NewNilObserver()

	// Create output
	group, err := makeZeroBusHttp(nil, beatInfo, observer, cfg)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if len(group.Clients) != 1 {
		t.Fatalf("Expected 1 client, got %d", len(group.Clients))
	}

	// Test client string representation
	client := group.Clients[0]
	expectedString := "zerobushttp(unity.default.air_quality)"
	if client.String() != expectedString {
		t.Fatalf("Expected client string %s, got %s", expectedString, client.String())
	}
}

func TestEventPublishing(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		var eventData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}

		// Verify event data
		if eventData["message"] != "test message" {
			t.Errorf("Expected message 'test message', got %v", eventData["message"])
		}

		// Return success response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Successfully ingested record."))
	}))
	defer server.Close()

	// Create configuration
	configMap := map[string]interface{}{
		"zerobus_uri":   strings.TrimPrefix(server.URL, "http://"),
		"table_name":    "unity.default.air_quality",
		"pat_token":     "test-token",
		"workspace_url": "https://test-workspace.cloud.databricks.com",
		"timeout":       "10s",
	}

	cfg, err := config.NewConfigFrom(configMap)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	// Create beat info
	beatInfo := beat.Info{
		Beat:    "test-beat",
		Version: "7.0.0",
		Logger:  logp.NewLogger("test"),
	}

	// Create observer
	observer := outputs.NewNilObserver()

	// Create output
	group, err := makeZeroBusHttp(nil, beatInfo, observer, cfg)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	client := group.Clients[0]

	// Create test event
	event := publisher.Event{
		Content: beat.Event{
			Timestamp: time.Now(),
			Fields: mapstr.M{
				"message": "test message",
			},
		},
	}

	// Create batch
	batch := &mockBatch{
		events: []publisher.Event{event},
	}

	// Publish event
	ctx := context.Background()
	err = client.Publish(ctx, batch)
	if err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	// Verify batch was ACKed
	if !batch.acked {
		t.Fatalf("Expected batch to be ACKed")
	}
}

func TestRetryableErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		retryable  bool
	}{
		{"OK", http.StatusOK, false},
		{"Bad Request", http.StatusBadRequest, false},
		{"Unauthorized", http.StatusUnauthorized, false},
		{"Forbidden", http.StatusForbidden, false},
		{"Not Found", http.StatusNotFound, false},
		{"Too Many Requests", http.StatusTooManyRequests, true},
		{"Internal Server Error", http.StatusInternalServerError, true},
		{"Bad Gateway", http.StatusBadGateway, true},
		{"Service Unavailable", http.StatusServiceUnavailable, true},
		{"Gateway Timeout", http.StatusGatewayTimeout, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retryable := isRetryableHTTPError(tt.statusCode)
			if retryable != tt.retryable {
				t.Fatalf("Expected %v, got %v for status code %d", tt.retryable, retryable, tt.statusCode)
			}
		})
	}
}

// mockBatch implements publisher.Batch for testing
type mockBatch struct {
	events []publisher.Event
	acked  bool
}

func (b *mockBatch) Events() []publisher.Event {
	return b.events
}

func (b *mockBatch) ACK() {
	b.acked = true
}

func (b *mockBatch) Drop() {
	// No-op for testing
}

func (b *mockBatch) Retry() {
	// No-op for testing
}

func (b *mockBatch) Cancelled() {
	// No-op for testing
}

func (b *mockBatch) RetryEvents(events []publisher.Event) {
	// No-op for testing
}

func (b *mockBatch) SplitRetry() bool {
	// No-op for testing
	return false
}
