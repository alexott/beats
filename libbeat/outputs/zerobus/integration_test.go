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
		BatchSize:  1, // API limitation: must be 1 for JSON mode
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
		Beat:    "filebeat",
		Version: "8.0.0",
		Logger:  logp.NewLogger("zerobus-test"),
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
