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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-agent-libs/config"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]interface{}
		wantErr string
	}{
		{
			name: "valid minimal config",
			cfg: map[string]interface{}{
				"zerobus_uri":   "12345.zerobus.region.cloud.databricks.com",
				"workspace_url": "https://workspace.cloud.databricks.com",
				"table_name":    "catalog.schema.table",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-secret",
				},
			},
			wantErr: "",
		},
		{
			name: "missing zerobus_uri",
			cfg: map[string]interface{}{
				"workspace_url": "https://workspace.cloud.databricks.com",
				"table_name":    "catalog.schema.table",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-secret",
				},
			},
			wantErr: "zerobus_uri is required",
		},
		{
			name: "missing oauth",
			cfg: map[string]interface{}{
				"zerobus_uri":   "12345.zerobus.region.cloud.databricks.com",
				"workspace_url": "https://workspace.cloud.databricks.com",
				"table_name":    "catalog.schema.table",
			},
			wantErr: "oauth is required",
		},
		{
			name: "invalid table name format",
			cfg: map[string]interface{}{
				"zerobus_uri":   "12345.zerobus.region.cloud.databricks.com",
				"workspace_url": "https://workspace.cloud.databricks.com",
				"table_name":    "invalid_table",
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-secret",
				},
			},
			wantErr: "table_name must be in format 'catalog.schema.table'",
		},
		{
			name: "invalid workers count",
			cfg: map[string]interface{}{
				"zerobus_uri":   "12345.zerobus.region.cloud.databricks.com",
				"workspace_url": "https://workspace.cloud.databricks.com",
				"table_name":    "catalog.schema.table",
				"workers":       -1,
				"oauth": map[string]interface{}{
					"client_id":     "test-client-id",
					"client_secret": "test-secret",
				},
			},
			wantErr: "workers must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.NewConfigFrom(tt.cfg)
			require.NoError(t, err)

			c, err := readConfig(cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.NotNil(t, c)
			} else {
				require.Error(t, err)
			}

			if c != nil {
				err = c.Validate()
				if tt.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.wantErr)
				} else {
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()

	assert.Equal(t, "json", c.RecordType)
	assert.Equal(t, 4, c.Workers)
	assert.Equal(t, 30*time.Second, c.Timeout)
	assert.Equal(t, 2048, c.BatchSize)
	assert.Equal(t, uint64(1000000), c.SDKOptions.MaxInflightRequests)
	assert.True(t, c.SDKOptions.Recovery)
}
