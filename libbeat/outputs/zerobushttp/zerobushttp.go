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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
)

// zerobushttpOutput implements the outputs.Client interface for ZeroBus HTTP output
type zerobushttpOutput struct {
	log        *logp.Logger
	client     *http.Client
	config     *Config
	codec      codec.Codec
	observer   outputs.Observer
	index      string
	baseURL    string
	usingOAuth bool // true if OAuth is configured, false if using PAT
}

func init() {
	outputs.RegisterType("zerobushttp", makeZeroBusHttp)
}

// makeZeroBusHttp creates a new ZeroBus HTTP output
func makeZeroBusHttp(
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
		return outputs.Fail(fmt.Errorf("zerobushttp output configuration validation failed: %w", err))
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

	// Create HTTP client
	client, usingOAuth, err := createHTTPClient(config, beat.Logger)
	if err != nil {
		return outputs.Fail(fmt.Errorf("failed to create HTTP client: %w", err))
	}

	// Create output instance
	output, err := newZeroBusHttp(beat, observer, config, enc, client, usingOAuth)
	if err != nil {
		return outputs.Fail(fmt.Errorf("zerobushttp output initialization failed: %w", err))
	}

	return outputs.Success(config.Queue, config.BatchSize, config.Retry.Max, nil, beat.Logger, output)
}

// newZeroBusHttp creates a new zerobushttpOutput instance
func newZeroBusHttp(
	beat beat.Info,
	observer outputs.Observer,
	config *Config,
	codec codec.Codec,
	client *http.Client,
	usingOAuth bool,
) (*zerobushttpOutput, error) {
	output := &zerobushttpOutput{
		log:        beat.Logger.Named("zerobushttp"),
		client:     client,
		config:     config,
		codec:      codec,
		observer:   observer,
		index:      beat.Beat,
		baseURL:    config.buildURL(),
		usingOAuth: usingOAuth,
	}

	if usingOAuth {
		output.log.Infof("Initialized ZeroBus HTTP output for table: %s (using OAuth M2M authentication)", config.TableName)
	} else {
		output.log.Warnf("Initialized ZeroBus HTTP output for table: %s (using PAT token - deprecated, please migrate to OAuth)", config.TableName)
	}
	return output, nil
}

// Close implements the outputs.Client interface
func (o *zerobushttpOutput) Close() error {
	o.log.Info("Closing ZeroBus HTTP output")
	return nil
}

// Publish implements the outputs.Client interface
func (o *zerobushttpOutput) Publish(ctx context.Context, batch publisher.Batch) error {
	st := o.observer
	events := batch.Events()
	st.NewBatch(len(events))

	acked := 0
	failed := 0

	for i := range events {
		success := o.publishEvent(ctx, &events[i])
		if success {
			acked++
		} else {
			failed++
		}
	}

	// ACK the batch
	batch.ACK()

	st.AckedEvents(acked)
	if failed > 0 {
		st.PermanentErrors(failed)
	}

	return nil
}

// publishEvent publishes a single event to ZeroBus
func (o *zerobushttpOutput) publishEvent(ctx context.Context, event *publisher.Event) bool {
	// Encode event to JSON
	serializedEvent, err := o.codec.Encode(o.index, &event.Content)
	if err != nil {
		o.log.Errorf("Failed to encode event: %v", err)
		o.observer.WriteError(err)
		return false
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", o.baseURL, strings.NewReader(string(serializedEvent)))
	if err != nil {
		o.log.Errorf("Failed to create HTTP request: %v", err)
		o.observer.WriteError(err)
		return false
	}

	// Set required headers
	o.setRequiredHeaders(req)

	// Set optional headers
	o.setOptionalHeaders(req)

	// Send request
	resp, err := o.client.Do(req)
	if err != nil {
		o.log.Errorf("Failed to send HTTP request: %v", err)
		o.observer.WriteError(err)
		return false
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		o.log.Errorf("HTTP request failed with status %d", resp.StatusCode)
		o.observer.WriteError(fmt.Errorf("HTTP request failed with status %d", resp.StatusCode))
		return false
	}

	o.observer.WriteBytes(len(serializedEvent))
	return true
}

// setRequiredHeaders sets the required ZeroBus headers
func (o *zerobushttpOutput) setRequiredHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	// Only set Authorization header manually when using PAT token
	// When using OAuth, the oauth2 client sets it automatically
	if !o.usingOAuth {
		req.Header.Set("Authorization", "Bearer "+o.config.PATToken)
	}
	req.Header.Set("unity-catalog-endpoint", o.config.WorkspaceURL)
	req.Header.Set("x-databricks-zerobus-table-name", o.config.TableName)
}

// setOptionalHeaders sets any optional custom headers
func (o *zerobushttpOutput) setOptionalHeaders(req *http.Request) {
	for key, value := range o.config.Headers {
		req.Header.Set(key, value)
	}
}

// String implements the outputs.Client interface
func (o *zerobushttpOutput) String() string {
	return fmt.Sprintf("zerobushttp(%s)", o.config.TableName)
}

// createHTTPClient creates an HTTP client with the given configuration
// Returns the HTTP client, a boolean indicating if OAuth is being used, and an error
func createHTTPClient(config *Config, logger *logp.Logger) (*http.Client, bool, error) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	// Configure TLS if specified
	if config.TLS != nil {
		tlsConfig, err := tlscommon.LoadTLSConfig(config.TLS, logger)
		if err != nil {
			return nil, false, fmt.Errorf("failed to load TLS config: %w", err)
		}
		transport.TLSClientConfig = tlsConfig.BuildModuleClientConfig("")
	}

	baseClient := &http.Client{
		Transport: transport,
		Timeout:   config.Timeout,
	}

	// If OAuth is configured, wrap the client with OAuth2 client credentials
	if config.IsOAuthEnabled() {
		logger.Info("Configuring OAuth M2M authentication for ZeroBus")

		// Create OAuth2 client credentials config
		oauthConfig := clientcredentials.Config{
			ClientID:     config.OAuth.ClientID,
			ClientSecret: config.OAuth.ClientSecret,
			TokenURL:     config.GetOAuthTokenURL(),
			Scopes:       []string{"all-apis"},
			AuthStyle:    oauth2.AuthStyleInHeader,
			EndpointParams: url.Values{
				"resource":              []string{config.GetOAuthResource()},
				"authorization_details": []string{config.GetAuthorizationDetails()},
			},
		}

		// Create context with the base HTTP client for token requests
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, baseClient)

		// Get OAuth-wrapped client that handles token management automatically
		oauthClient := oauthConfig.Client(ctx)

		// Preserve timeout from base client
		oauthClient.Timeout = config.Timeout

		return oauthClient, true, nil
	}

	// Using PAT token
	return baseClient, false, nil
}
