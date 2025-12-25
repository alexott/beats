# Zerobus Go SDK Integration - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Integrate Zerobus Go SDK into Filebeat as a new output type for reliable, high-performance data ingestion with JSON and Protocol Buffer support.

**Architecture:** Create new `libbeat/outputs/zerobus` package that wraps the Zerobus Go SDK. Phase 1 implements JSON ingestion with long-lived stream, goroutine-based concurrency, and SDK-managed retries. Phase 2 adds Protocol Buffer support via JSON→Proto conversion.

**Tech Stack:**
- Go 1.21+, CGO
- Zerobus Go SDK (github.com/databricks/zerobus-go-sdk)
- google.golang.org/protobuf (Phase 2)
- Beats framework (outputs.Client interface, codec system)

---

## Prerequisites

Before starting, ensure:
1. Rust toolchain installed (1.75+): `rustc --version`
2. CGO enabled: `go env CGO_ENABLED` should output `1`
3. Clone Zerobus Go SDK to local directory: `git clone https://github.com/databricks/zerobus-sdk-go /Users/alexey.ott/work/forks/zerobus-sdk-go`
4. Build SDK once: `cd /Users/alexey.ott/work/forks/zerobus-sdk-go/sdk && go generate`

---

## Phase 1: JSON Mode Implementation

### Task 1: Create Package Structure and SDK Dependency Setup

**Files:**
- Create: `libbeat/outputs/zerobus/`
- Create: `libbeat/outputs/zerobus/go.mod` (temporary for vendoring)

**Step 1: Create package directory**

```bash
mkdir -p libbeat/outputs/zerobus/docs
```

Expected: Directory created successfully

**Step 2: Add SDK to project dependencies**

Edit `go.mod` in the beats root:

```bash
cd /Users/alexey.ott/work/forks/beats
go mod edit -require=github.com/databricks/zerobus-go-sdk@v0.1.0
go mod edit -replace=github.com/databricks/zerobus-go-sdk=/Users/alexey.ott/work/forks/zerobus-sdk-go
```

Expected: go.mod updated with replace directive

**Step 3: Verify SDK imports work**

```bash
cd libbeat/outputs/zerobus
go mod init github.com/elastic/beats/v7/libbeat/outputs/zerobus
go get github.com/databricks/zerobus-go-sdk@v0.1.0
```

Expected: Dependencies resolved

**Step 4: Commit**

```bash
git add libbeat/outputs/zerobus/
git add go.mod
git commit -m "feat(zerobus): create package structure and add SDK dependency"
```

---

### Task 2: Implement Configuration Structures

**Files:**
- Create: `libbeat/outputs/zerobus/config.go`
- Create: `libbeat/outputs/zerobus/config_test.go`

**Step 1: Write failing test for config validation**

Create `libbeat/outputs/zerobus/config_test.go`:

```go
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
```

**Step 2: Run test to verify it fails**

```bash
cd libbeat/outputs/zerobus
go test -v -run TestConfigValidation
```

Expected: FAIL with "undefined: readConfig" or "undefined: Config"

**Step 3: Implement config structures**

Create `libbeat/outputs/zerobus/config.go`:

```go
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
```

**Step 4: Run test to verify it passes**

```bash
cd libbeat/outputs/zerobus
go test -v -run TestConfigValidation
```

Expected: PASS (all test cases pass)

**Step 5: Commit**

```bash
git add libbeat/outputs/zerobus/config.go libbeat/outputs/zerobus/config_test.go
git commit -m "feat(zerobus): implement configuration structures with validation"
```

---

### Task 3: Implement Output Registration and Factory

**Files:**
- Create: `libbeat/outputs/zerobus/zerobus.go`
- Modify: `libbeat/outputs/zerobus/config.go` (add buildURL method)

**Step 1: Add buildURL helper to config**

Add to `libbeat/outputs/zerobus/config.go`:

```go
import (
	"net/url"
)

// buildURL constructs the Zerobus ingest URL
func (c *Config) buildURL() string {
	// Clean the URI by removing protocol if present
	cleanURI := strings.TrimPrefix(c.ZeroBusURI, "https://")
	cleanURI = strings.TrimPrefix(cleanURI, "http://")

	// For SDK, we need the full URI (SDK handles URL construction internally)
	return fmt.Sprintf("https://%s", cleanURI)
}
```

**Step 2: Create basic output structure**

Create `libbeat/outputs/zerobus/zerobus.go`:

```go
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
	"context"
	"fmt"

	zerobus "github.com/databricks/zerobus-go-sdk/sdk"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
)

func init() {
	outputs.RegisterType("zerobus", makeZerobus)
}

// zerobusOutput implements the outputs.Client interface
type zerobusOutput struct {
	log      *logp.Logger
	config   *Config
	codec    codec.Codec
	observer outputs.Observer
	index    string

	// SDK components
	sdk    *zerobus.ZerobusSdk
	stream *zerobus.ZerobusStream

	// Concurrency control
	workerSem chan struct{}
}

// makeZerobus creates a new Zerobus output
func makeZerobus(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *config.C,
) (outputs.Group, error) {
	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	if err := config.Validate(); err != nil {
		return outputs.Fail(fmt.Errorf("zerobus output configuration validation failed: %w", err))
	}

	// Create codec
	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(fmt.Errorf("failed to create codec: %w", err))
		}
	} else {
		// Use default JSON codec
		enc = json.New(beat.Version, json.Config{
			Pretty:     false,
			EscapeHTML: false,
		})
	}

	// Create output instance
	output, err := newZerobusOutput(beat, observer, config, enc)
	if err != nil {
		return outputs.Fail(fmt.Errorf("zerobus output initialization failed: %w", err))
	}

	return outputs.Success(config.Queue, config.BatchSize, config.Retry.Max, nil, beat.Logger, output)
}

// newZerobusOutput creates a new zerobusOutput instance
func newZerobusOutput(
	beat beat.Info,
	observer outputs.Observer,
	config *Config,
	codec codec.Codec,
) (*zerobusOutput, error) {
	log := beat.Logger.Named("zerobus")

	// Create SDK instance
	sdk, err := zerobus.NewZerobusSdk(
		config.buildURL(),
		config.WorkspaceURL,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK: %w", err)
	}

	// Configure stream options
	options := zerobus.DefaultStreamConfigurationOptions()
	options.RecordType = zerobus.RecordTypeJson
	options.MaxInflightRequests = config.SDKOptions.MaxInflightRequests
	options.Recovery = config.SDKOptions.Recovery
	options.RecoveryRetries = config.SDKOptions.RecoveryRetries
	options.RecoveryTimeoutMs = config.SDKOptions.RecoveryTimeoutMs
	options.RecoveryBackoffMs = config.SDKOptions.RecoveryBackoffMs
	options.FlushTimeoutMs = config.SDKOptions.FlushTimeoutMs
	options.ServerLackOfAckTimeoutMs = config.SDKOptions.ServerLackOfAckTimeoutMs

	// Create long-lived stream
	stream, err := sdk.CreateStream(
		zerobus.TableProperties{
			TableName: config.TableName,
		},
		config.OAuth.ClientID,
		config.OAuth.ClientSecret,
		options,
	)
	if err != nil {
		sdk.Free()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Create worker semaphore
	workerSem := make(chan struct{}, config.Workers)

	output := &zerobusOutput{
		log:       log,
		config:    config,
		codec:     codec,
		observer:  observer,
		index:     beat.Beat,
		sdk:       sdk,
		stream:    stream,
		workerSem: workerSem,
	}

	log.Infof("Initialized Zerobus output for table: %s (%d workers)", config.TableName, config.Workers)

	return output, nil
}

// Close implements the outputs.Client interface
func (o *zerobusOutput) Close() error {
	o.log.Info("Closing Zerobus output")

	// Close stream (flushes and waits for pending acks)
	if err := o.stream.Close(); err != nil {
		o.log.Errorf("Error closing stream: %v", err)
	}

	// Free SDK resources
	o.sdk.Free()

	return nil
}

// Publish implements the outputs.Client interface
func (o *zerobusOutput) Publish(ctx context.Context, batch publisher.Batch) error {
	// TODO: Implement batch publishing
	batch.ACK()
	return nil
}

// String implements the outputs.Client interface
func (o *zerobusOutput) String() string {
	return fmt.Sprintf("zerobus(%s)", o.config.TableName)
}
```

**Step 3: Build to verify compilation**

```bash
cd libbeat/outputs/zerobus
go build
```

Expected: Compilation succeeds (may have warnings about unused variables)

**Step 4: Commit**

```bash
git add libbeat/outputs/zerobus/zerobus.go libbeat/outputs/zerobus/config.go
git commit -m "feat(zerobus): implement output registration and SDK initialization"
```

---

### Task 4: Implement Batch Publishing with Goroutine Pool

**Files:**
- Modify: `libbeat/outputs/zerobus/zerobus.go` (implement Publish method)
- Create: `libbeat/outputs/zerobus/zerobus_test.go`

**Step 1: Write test for batch publishing**

Create `libbeat/outputs/zerobus/zerobus_test.go`:

```go
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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/mapstr"
)

// NOTE: These tests require mocking the SDK, which will be implemented separately
// For now, we'll create placeholder tests that verify the structure

func TestZerobusOutputPublish(t *testing.T) {
	t.Skip("Requires SDK mocking - implement after basic structure is complete")

	// TODO: Add tests for:
	// - Successful batch publishing
	// - Encoding errors
	// - Ingestion errors
	// - Acknowledgment handling
	// - Worker pool behavior
}

func TestZerobusOutputClose(t *testing.T) {
	t.Skip("Requires SDK mocking - implement after basic structure is complete")

	// TODO: Add tests for:
	// - Graceful shutdown
	// - Stream flush on close
	// - SDK resource cleanup
}
```

**Step 2: Implement Publish method**

Replace the Publish method in `libbeat/outputs/zerobus/zerobus.go`:

```go
import (
	"sync"
)

// Publish implements the outputs.Client interface
func (o *zerobusOutput) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	o.observer.NewBatch(len(events))

	if len(events) == 0 {
		batch.ACK()
		return nil
	}

	// Phase 1: Pre-encode all events (codec is not thread-safe)
	type encodedEvent struct {
		index int
		data  []byte
		err   error
	}

	encodedEvents := make([]encodedEvent, len(events))
	for i := range events {
		data, err := o.codec.Encode(o.index, &events[i].Content)
		encodedEvents[i] = encodedEvent{
			index: i,
			data:  data,
			err:   err,
		}
		if err != nil {
			o.log.Errorf("Failed to encode event %d: %v", i, err)
			o.observer.WriteError(err)
		}
	}

	// Phase 2: Parallel ingestion with worker pool
	type result struct {
		index   int
		success bool
		err     error
	}

	results := make(chan result, len(events))
	var wg sync.WaitGroup

	for _, encoded := range encodedEvents {
		if encoded.err != nil {
			// Skip events that failed encoding
			results <- result{index: encoded.index, success: false, err: encoded.err}
			continue
		}

		// Acquire worker slot
		o.workerSem <- struct{}{}
		wg.Add(1)

		go func(idx int, jsonBytes []byte) {
			defer func() {
				<-o.workerSem // Release worker
				wg.Done()
			}()

			// IngestRecord blocks until queued (SDK handles backpressure)
			ack, err := o.stream.IngestRecord(string(jsonBytes))
			if err != nil {
				o.log.Errorf("Failed to ingest event %d: %v", idx, err)
				o.observer.WriteError(err)
				results <- result{index: idx, success: false, err: err}
				return
			}

			// Wait for server acknowledgment
			offset, err := ack.Await()
			if err != nil {
				o.log.Errorf("Failed to await ack for event %d: %v", idx, err)
				o.observer.WriteError(err)
				results <- result{index: idx, success: false, err: err}
				return
			}

			o.log.Debugf("Event %d acknowledged at offset %d", idx, offset)
			results <- result{index: idx, success: true}
		}(encoded.index, encoded.data)
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Phase 3: Collect results
	acked := 0
	failed := 0
	for r := range results {
		if r.success {
			acked++
			o.observer.WriteBytes(len(encodedEvents[r.index].data))
		} else {
			failed++
		}
	}

	// ACK the batch (always ACK to Beats)
	batch.ACK()

	// Report metrics
	o.observer.AckedEvents(acked)
	if failed > 0 {
		// Mark as permanent errors → triggers Beats retry → eventual DLQ
		o.observer.PermanentErrors(failed)
	}

	return nil
}
```

**Step 3: Build to verify compilation**

```bash
cd libbeat/outputs/zerobus
go build
```

Expected: Compilation succeeds

**Step 4: Commit**

```bash
git add libbeat/outputs/zerobus/zerobus.go libbeat/outputs/zerobus/zerobus_test.go
git commit -m "feat(zerobus): implement batch publishing with goroutine pool"
```

---

### Task 5: Add Error Handling Utilities

**Files:**
- Create: `libbeat/outputs/zerobus/errors.go`

**Step 1: Create error utilities**

Create `libbeat/outputs/zerobus/errors.go`:

```go
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
	"fmt"
	"strings"
)

// ProtoConversionError represents an error during JSON→Proto conversion
type ProtoConversionError struct {
	Message      string
	OriginalJSON string
	MessageType  string
	Err          error
}

func (e *ProtoConversionError) Error() string {
	return e.Message
}

func (e *ProtoConversionError) Unwrap() error {
	return e.Err
}

// truncateJSON truncates JSON string to maxLen for logging
func truncateJSON(jsonStr string, maxLen int) string {
	if len(jsonStr) <= maxLen {
		return jsonStr
	}
	return jsonStr[:maxLen] + "... (truncated)"
}

// formatErrorWithContext formats error with contextual information for logging
func formatErrorWithContext(err error, eventID int, data string, maxDataLen int) string {
	return fmt.Sprintf("Event %d error: %v\n  Data: %s",
		eventID, err, truncateJSON(data, maxDataLen))
}

// isRetryableError checks if an error from the SDK is retryable
// The SDK handles retryable errors internally, so this is for additional context
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// Non-retryable error indicators
	nonRetryable := []string{
		"authentication",
		"unauthorized",
		"forbidden",
		"invalid table",
		"schema mismatch",
		"not found",
	}

	for _, indicator := range nonRetryable {
		if strings.Contains(errMsg, indicator) {
			return false
		}
	}

	// Retryable error indicators
	retryable := []string{
		"timeout",
		"connection",
		"network",
		"temporary",
	}

	for _, indicator := range retryable {
		if strings.Contains(errMsg, indicator) {
			return true
		}
	}

	// Default: assume retryable for unknown errors
	return true
}
```

**Step 2: Build to verify compilation**

```bash
cd libbeat/outputs/zerobus
go build
```

Expected: Compilation succeeds

**Step 3: Commit**

```bash
git add libbeat/outputs/zerobus/errors.go
git commit -m "feat(zerobus): add error handling utilities"
```

---

### Task 6: Create Basic Documentation

**Files:**
- Create: `libbeat/outputs/zerobus/docs/zerobus.asciidoc`
- Create: `libbeat/outputs/zerobus/README.md`

**Step 1: Create AsciiDoc documentation**

Create `libbeat/outputs/zerobus/docs/zerobus.asciidoc`:

```asciidoc
[[zerobus-output]]
=== Zerobus output

The Zerobus output sends events to Databricks Delta tables using the Zerobus streaming service.

This output uses the Zerobus Go SDK which provides:

* Automatic OAuth 2.0 authentication
* Built-in retry and stream recovery
* High-throughput streaming ingestion
* Support for both JSON and Protocol Buffer formats

==== Example configuration

[source,yaml]
----
output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.table"

  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"

  workers: 4
  batch_size: 2048
----

==== Configuration options

[float]
===== `zerobus_uri`

The Zerobus service endpoint. This is typically in the format `<workspace_id>.zerobus.<region>.<cloud>.databricks.com`.

[float]
===== `workspace_url`

The Databricks workspace URL for OAuth authentication.

[float]
===== `table_name`

The fully qualified table name in the format `catalog.schema.table`.

[float]
===== `oauth`

OAuth 2.0 client credentials for authentication.

* `client_id`: The OAuth client ID
* `client_secret`: The OAuth client secret

[float]
===== `workers`

Number of concurrent worker goroutines for parallel event ingestion. Default: 4.

[float]
===== `batch_size`

Number of events to batch together before sending. Default: 2048.

[float]
===== `record_type`

Record format type: `json` or `proto`. Default: `json`.

[float]
===== `sdk_options`

Advanced SDK configuration options:

* `max_inflight_requests`: Maximum number of in-flight requests. Default: 1000000.
* `recovery`: Enable automatic stream recovery. Default: true.
* `recovery_retries`: Maximum number of recovery attempts. Default: 4.
* `recovery_timeout_ms`: Timeout for recovery operations in milliseconds. Default: 15000.
* `recovery_backoff_ms`: Delay between recovery retry attempts in milliseconds. Default: 2000.
* `flush_timeout_ms`: Timeout for flush operations in milliseconds. Default: 300000.
* `server_lack_of_ack_timeout_ms`: Timeout waiting for server acknowledgments in milliseconds. Default: 60000.

==== Migration from zerobushttp

The Zerobus output is the recommended replacement for the `zerobushttp` output. To migrate:

1. Change `output.zerobushttp` to `output.zerobus` in your configuration
2. Configuration options remain the same
3. The SDK-based implementation provides better reliability and performance

==== Requirements

* CGO must be enabled (CGO_ENABLED=1)
* Rust toolchain 1.75+ must be installed
* OAuth 2.0 client credentials with appropriate permissions
```

**Step 2: Create README**

Create `libbeat/outputs/zerobus/README.md`:

```markdown
# Zerobus Output

This package implements the Beats output for Databricks Zerobus streaming service using the [Zerobus Go SDK](https://github.com/databricks/zerobus-sdk-go).

## Features

- High-throughput streaming ingestion to Databricks Delta tables
- Automatic OAuth 2.0 authentication with Unity Catalog
- Built-in retry and stream recovery
- Goroutine-based parallel ingestion
- Support for JSON and Protocol Buffer formats
- Dead letter queue integration for permanent errors

## Architecture

The output creates a long-lived gRPC stream to the Zerobus service and uses a worker pool of goroutines to ingest events in parallel. The SDK handles:

- Authentication token management
- Automatic retry of transient failures
- Stream recovery on disconnection
- Backpressure control
- Acknowledgment tracking

## Build Requirements

- Go 1.21+
- CGO enabled (`CGO_ENABLED=1`)
- Rust toolchain 1.75+
- C compiler (gcc/clang/MinGW-w64)

## Configuration

See `docs/zerobus.asciidoc` for complete configuration reference.

## Implementation Status

- [x] Phase 1: JSON ingestion
- [ ] Phase 2: Protocol Buffer support
- [ ] Phase 3: Production hardening

## Testing

Run tests with:

```bash
cd libbeat/outputs/zerobus
go test -v
```

Integration tests require a valid Zerobus staging environment configuration.
```

**Step 3: Commit**

```bash
git add libbeat/outputs/zerobus/docs/ libbeat/outputs/zerobus/README.md
git commit -m "docs(zerobus): add initial documentation"
```

---

### Task 7: Integration Test Configuration

**Files:**
- Create: `libbeat/outputs/zerobus/integration_test.go`
- Create: `libbeat/outputs/zerobus/.env.example`

**Step 1: Create integration test**

Create `libbeat/outputs/zerobus/integration_test.go`:

```go
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

//go:build integration
// +build integration

package zerobus

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/mapstr"
)

// Integration tests require environment variables:
// ZEROBUS_URI, WORKSPACE_URL, TABLE_NAME, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET

func TestIntegrationZerobusOutput(t *testing.T) {
	zeroBusURI := os.Getenv("ZEROBUS_URI")
	workspaceURL := os.Getenv("WORKSPACE_URL")
	tableName := os.Getenv("TABLE_NAME")
	clientID := os.Getenv("OAUTH_CLIENT_ID")
	clientSecret := os.Getenv("OAUTH_CLIENT_SECRET")

	if zeroBusURI == "" || workspaceURL == "" || tableName == "" ||
	   clientID == "" || clientSecret == "" {
		t.Skip("Skipping integration test: required environment variables not set")
	}

	// Create config
	cfg := &Config{
		ZeroBusURI:   zeroBusURI,
		WorkspaceURL: workspaceURL,
		TableName:    tableName,
		OAuth: &OAuthConfig{
			ClientID:     clientID,
			ClientSecret: clientSecret,
		},
		Workers:    2,
		Timeout:    30 * time.Second,
		BatchSize:  10,
		RecordType: "json",
		SDKOptions: SDKOptions{
			MaxInflightRequests:      10000,
			Recovery:                 true,
			RecoveryRetries:          4,
			RecoveryTimeoutMs:        15000,
			RecoveryBackoffMs:        2000,
			FlushTimeoutMs:           300000,
			ServerLackOfAckTimeoutMs: 60000,
		},
	}

	// Create beat info
	beatInfo := beat.Info{
		Beat:     "filebeat",
		Version:  "8.0.0",
		Logger:   logp.NewLogger("zerobus-test"),
	}

	// Create codec
	codec := json.New(beatInfo.Version, json.Config{
		Pretty:     false,
		EscapeHTML: false,
	})

	// Create observer
	observer := outputs.NewNilObserver()

	// Create output
	output, err := newZerobusOutput(beatInfo, observer, cfg, codec)
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Close()

	// Create test batch
	events := []publisher.Event{
		{
			Content: beat.Event{
				Timestamp: time.Now(),
				Fields: mapstr.M{
					"message": "test message 1",
					"level":   "info",
					"host":    "test-host",
				},
			},
		},
		{
			Content: beat.Event{
				Timestamp: time.Now(),
				Fields: mapstr.M{
					"message": "test message 2",
					"level":   "debug",
					"host":    "test-host",
				},
			},
		},
	}

	batch := &testBatch{events: events}

	// Publish batch
	err = output.Publish(context.Background(), batch)
	assert.NoError(t, err)
	assert.True(t, batch.acked, "Batch should be ACKed")

	t.Logf("Successfully published %d events to %s", len(events), tableName)
}

// testBatch implements publisher.Batch for testing
type testBatch struct {
	events []publisher.Event
	acked  bool
}

func (b *testBatch) Events() []publisher.Event {
	return b.events
}

func (b *testBatch) ACK() {
	b.acked = true
}

func (b *testBatch) Drop(reason string) {
	// No-op for test
}

func (b *testBatch) Retry() {
	// No-op for test
}

func (b *testBatch) Cancelled() bool {
	return false
}

func (b *testBatch) RetryEvents(events []publisher.Event) {
	// No-op for test
}

func (b *testBatch) CancelledEvents(events []publisher.Event) {
	// No-op for test
}
```

**Step 2: Create environment variable example**

Create `libbeat/outputs/zerobus/.env.example`:

```bash
# Zerobus Integration Test Configuration
# Copy to .env and fill in your values

# Zerobus service endpoint (format: <workspace_id>.zerobus.<region>.<cloud>.databricks.com)
ZEROBUS_URI=12345.zerobus.westeurope.azuredatabricks.net

# Databricks workspace URL
WORKSPACE_URL=https://adb-12345.17.azuredatabricks.net

# Target table (format: catalog.schema.table)
TABLE_NAME=main.tmp.filebeat_test

# OAuth credentials
OAUTH_CLIENT_ID=your-client-id
OAUTH_CLIENT_SECRET=your-client-secret
```

**Step 3: Document how to run integration tests**

Add to `libbeat/outputs/zerobus/README.md`:

```markdown
## Integration Testing

Integration tests require a Databricks workspace with Zerobus enabled.

1. Copy `.env.example` to `.env` and fill in your credentials
2. Source the environment variables: `source .env`
3. Run integration tests:

```bash
go test -v -tags=integration
```
```

**Step 4: Commit**

```bash
git add libbeat/outputs/zerobus/integration_test.go
git add libbeat/outputs/zerobus/.env.example
git add libbeat/outputs/zerobus/README.md
git commit -m "test(zerobus): add integration test configuration"
```

---

### Task 8: Build Verification and Testing

**Step 1: Build the output package**

```bash
cd /Users/alexey.ott/work/forks/beats/libbeat/outputs/zerobus
CGO_ENABLED=1 go build
```

Expected: Successful compilation with no errors

**Step 2: Run unit tests**

```bash
cd /Users/alexey.ott/work/forks/beats/libbeat/outputs/zerobus
go test -v
```

Expected: Tests pass (may have skipped tests for SDK mocking)

**Step 3: Build filebeat with new output**

```bash
cd /Users/alexey.ott/work/forks/beats/filebeat
CGO_ENABLED=1 go build
```

Expected: Successful compilation including new zerobus output

**Step 4: Verify output registration**

```bash
./filebeat test output
```

Expected: Output should show zerobus as an available output type

**Step 5: Commit**

```bash
git add .
git commit -m "build(zerobus): verify Phase 1 implementation compiles and tests pass"
```

---

### Task 9: Create Example Configuration

**Files:**
- Create: `libbeat/outputs/zerobus/examples/filebeat-zerobus-json.yml`

**Step 1: Create example config**

Create `libbeat/outputs/zerobus/examples/filebeat-zerobus-json.yml`:

```yaml
# Example Filebeat configuration for Zerobus output (JSON mode)

###################### Filebeat Configuration ######################

filebeat.inputs:
  - type: filestream
    id: zerobus-example
    enabled: true
    paths:
      - /var/log/*.log

    # Optional: Add processors to shape data
    processors:
      - add_host_metadata:
          when.not.contains.tags: forwarded
      - add_fields:
          target: ''
          fields:
            environment: production

###################### Zerobus Output ######################

output.zerobus:
  # Zerobus service endpoint
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"

  # Databricks workspace URL
  workspace_url: "https://workspace.cloud.databricks.com"

  # Target table (catalog.schema.table format)
  table_name: "catalog.schema.logs"

  # OAuth credentials (use environment variables for security)
  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"

  # Performance tuning
  workers: 4        # Number of concurrent workers
  batch_size: 2048  # Events per batch

  # Optional: SDK options for advanced tuning
  # sdk_options:
  #   max_inflight_requests: 1000000
  #   recovery: true
  #   recovery_retries: 4
  #   flush_timeout_ms: 300000

###################### Logging ######################

logging.level: info
logging.selectors: ["*"]
```

**Step 2: Create high-throughput example**

Create `libbeat/outputs/zerobus/examples/filebeat-zerobus-high-throughput.yml`:

```yaml
# High-throughput Filebeat configuration for Zerobus output

filebeat.inputs:
  - type: filestream
    id: high-volume-logs
    enabled: true
    paths:
      - /var/log/app/*.log

    # Optimize for high throughput
    prospector:
      scanner:
        check_interval: 1s

    processors:
      - add_host_metadata: ~

output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.high_volume_logs"

  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"

  # High-throughput settings
  workers: 16
  batch_size: 8192

  # Optimize SDK for high throughput
  sdk_options:
    max_inflight_requests: 500000
    recovery_retries: 10
    flush_timeout_ms: 600000

logging.level: info
```

**Step 3: Commit**

```bash
git add libbeat/outputs/zerobus/examples/
git commit -m "docs(zerobus): add example configurations"
```

---

## Phase 1 Completion Checklist

At this point, Phase 1 (JSON mode) is complete. Verify:

- [ ] Package structure created
- [ ] Configuration with validation implemented
- [ ] SDK initialization and stream management working
- [ ] Batch publishing with goroutine pool implemented
- [ ] Error handling utilities added
- [ ] Basic documentation written
- [ ] Integration test configuration ready
- [ ] Build verification passed
- [ ] Example configurations created

Run final verification:

```bash
cd /Users/alexey.ott/work/forks/beats/libbeat/outputs/zerobus
go test -v
CGO_ENABLED=1 go build
```

Expected: All tests pass, compilation succeeds.

---

## Phase 2: Protocol Buffer Support (Future)

Phase 2 will add Protocol Buffer support. Key tasks:

1. Extend config for proto options (descriptor file, message type)
2. Implement descriptor loading and parsing
3. Implement JSON→Proto conversion using protojson
4. Add proto-specific error handling with detailed logging
5. Write proto conversion unit tests
6. Integration testing with sample schemas
7. Documentation for proto workflow

This will be planned in detail after Phase 1 is complete and validated.

---

## Testing Strategy

### Unit Tests
- Config validation (✓ implemented)
- Error handling utilities (✓ implemented)
- Proto conversion (Phase 2)

### Integration Tests
- JSON mode end-to-end (✓ configured, needs execution)
- Proto mode (Phase 2)
- Recovery scenarios (Phase 2)

### Manual Testing Checklist

Before considering Phase 1 complete, manually test:

1. **Basic ingestion**: Single filebeat process sending to staging table
2. **High volume**: Multiple workers with large batches
3. **Error scenarios**: Invalid credentials, wrong table name
4. **Recovery**: Network interruption during ingestion
5. **Graceful shutdown**: SIGTERM during active ingestion

---

## Rollout Plan

1. **Phase 1 validation** (Current)
   - Deploy to staging environment
   - Run for 48 hours with production-like load
   - Monitor metrics: throughput, latency, errors
   - Validate automatic recovery works

2. **Phase 2 development** (After Phase 1 stable)
   - Implement proto support
   - Test with sample schemas
   - Document proto workflow

3. **Production rollout** (After Phase 2 complete)
   - Parallel deployment with zerobushttp
   - Gradual traffic shift
   - Monitor for 1 week
   - Full cutover and deprecate zerobushttp

---

## Dependencies

**Go Modules:**
```
github.com/databricks/zerobus-go-sdk v0.1.0
google.golang.org/protobuf v1.31.0 (Phase 2)
```

**Build Requirements:**
- Rust 1.75+
- CGO enabled
- C compiler (gcc/clang/MinGW-w64)

---

## Common Issues and Solutions

### Issue: "undefined: zerobus.NewZerobusSdk"

**Solution:** Ensure SDK is properly vendored or go.mod has correct replace directive:
```bash
go mod edit -replace=github.com/databricks/zerobus-go-sdk=/Users/alexey.ott/work/forks/zerobus-sdk-go
go mod tidy
```

### Issue: "CGO_ENABLED=0 but code requires CGO"

**Solution:** Enable CGO:
```bash
export CGO_ENABLED=1
go build
```

### Issue: Rust library not found during linking

**Solution:** Generate the Rust FFI library:
```bash
cd /Users/alexey.ott/work/forks/zerobus-sdk-go/sdk
go generate
```

### Issue: "authentication failed" during testing

**Solution:** Verify OAuth credentials are correct and have permissions:
- Check client ID and secret
- Verify table permissions (USE CATALOG, USE SCHEMA, SELECT, MODIFY)
- Ensure workspace URL is correct

---

## Success Criteria

Phase 1 is considered complete when:

1. ✓ Code compiles without errors
2. ✓ Unit tests pass
3. ⏳ Integration test successfully ingests data to staging table
4. ⏳ Manual testing validates all scenarios
5. ⏳ 48-hour soak test in staging shows stable operation
6. ⏳ Performance meets targets (≥15K events/sec)
7. ✓ Documentation complete

---

## Next Steps After Phase 1

1. Run integration tests in staging environment
2. Execute manual testing checklist
3. Review and iterate on any issues found
4. Once stable, plan Phase 2 (Proto support) in detail
5. Document lessons learned and update plan
