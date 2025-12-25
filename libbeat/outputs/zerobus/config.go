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

package zerobus

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
)

// Config contains the configuration for the Zerobus output
type Config struct {
	ZeroBusURI   string       `config:"zerobus_uri" validate:"required"`
	WorkspaceURL string       `config:"workspace_url" validate:"required"`
	TableName    string       `config:"table_name" validate:"required"`
	OAuth        *OAuthConfig `config:"oauth" validate:"required"`

	// Record format
	RecordType          string `config:"record_type"`
	ProtoDescriptorFile string `config:"proto_descriptor_file"`
	ProtoMessageType    string `config:"proto_message_type"`

	// SDK Options
	SDKOptions SDKOptions `config:"sdk_options"`

	// Concurrency
	Workers int `config:"workers"`

	// Beats integration
	Timeout   time.Duration     `config:"timeout"`
	Retry     retryConfig       `config:"retry"`
	TLS       *tlscommon.Config `config:"ssl"`
	Codec     codec.Config      `config:"codec"`
	BatchSize int               `config:"batch_size"`
	Queue     config.Namespace  `config:"queue"`
}

// OAuthConfig contains OAuth2 client credentials configuration
type OAuthConfig struct {
	ClientID     string `config:"client_id" validate:"required"`
	ClientSecret string `config:"client_secret" validate:"required"`
}

// SDKOptions contains Zerobus SDK configuration options
type SDKOptions struct {
	MaxInflightRequests      uint64 `config:"max_inflight_requests"`
	Recovery                 bool   `config:"recovery"`
	RecoveryTimeoutMs        uint64 `config:"recovery_timeout_ms"`
	RecoveryBackoffMs        uint64 `config:"recovery_backoff_ms"`
	RecoveryRetries          uint32 `config:"recovery_retries"`
	FlushTimeoutMs           uint64 `config:"flush_timeout_ms"`
	ServerLackOfAckTimeoutMs uint64 `config:"server_lack_of_ack_timeout_ms"`
}

type retryConfig struct {
	Max     int           `config:"max" validate:"min=0"`
	Backoff time.Duration `config:"backoff" validate:"min=0"`
}

// defaultConfig returns the default configuration
func defaultConfig() Config {
	return Config{
		RecordType: "json",
		Workers:    4,
		Timeout:    30 * time.Second,
		Retry: retryConfig{
			Max:     3,
			Backoff: 1 * time.Second,
		},
		BatchSize: 2048,
		SDKOptions: SDKOptions{
			MaxInflightRequests:      1000000,
			Recovery:                 true,
			RecoveryTimeoutMs:        15000,
			RecoveryBackoffMs:        2000,
			RecoveryRetries:          4,
			FlushTimeoutMs:           300000,
			ServerLackOfAckTimeoutMs: 60000,
		},
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

	if c.OAuth == nil {
		return errors.New("oauth is required")
	}

	if c.OAuth.ClientID == "" || c.OAuth.ClientSecret == "" {
		return errors.New("oauth client_id and client_secret are required")
	}

	if c.WorkspaceURL == "" {
		return errors.New("workspace_url is required")
	}

	// Validate table name format (catalog.schema.table)
	if !isValidTableName(c.TableName) {
		return errors.New("table_name must be in format 'catalog.schema.table'")
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

	// Validate proto config if proto mode
	if c.RecordType == "proto" {
		if c.ProtoDescriptorFile == "" {
			return errors.New("proto_descriptor_file is required when record_type is 'proto'")
		}
		if c.ProtoMessageType == "" {
			return errors.New("proto_message_type is required when record_type is 'proto'")
		}
	}

	return nil
}

// isValidTableName validates that the table name follows catalog.schema.table format
func isValidTableName(tableName string) bool {
	parts := strings.Split(tableName, ".")
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != ""
}

// buildURL constructs the Zerobus ingest URL
func (c *Config) buildURL() string {
	// Clean the URI by removing protocol if present
	cleanURI := strings.TrimPrefix(c.ZeroBusURI, "https://")
	cleanURI = strings.TrimPrefix(cleanURI, "http://")

	// For SDK, we need the full URI (SDK handles URL construction internally)
	return fmt.Sprintf("https://%s", cleanURI)
}
