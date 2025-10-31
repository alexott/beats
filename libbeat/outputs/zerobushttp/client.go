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
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/elastic-agent-libs/logp"
)

// HTTPClient wraps the standard http.Client with ZeroBus-specific functionality
type HTTPClient struct {
	client     *http.Client
	log        *logp.Logger
	config     *Config
	usingOAuth bool // true if OAuth is configured, false if using PAT
}

// NewHTTPClient creates a new HTTPClient instance
func NewHTTPClient(config *Config, logger *logp.Logger) (*HTTPClient, error) {
	client, usingOAuth, err := createHTTPClient(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	if usingOAuth {
		logger.Info("HTTPClient initialized with OAuth M2M authentication")
	} else {
		logger.Warn("HTTPClient initialized with PAT token (deprecated, please migrate to OAuth)")
	}

	return &HTTPClient{
		client:     client,
		log:        logger,
		config:     config,
		usingOAuth: usingOAuth,
	}, nil
}

// SendEvent sends a single event to the ZeroBus endpoint
func (c *HTTPClient) SendEvent(ctx context.Context, eventData []byte) error {
	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", c.config.buildURL(), strings.NewReader(string(eventData)))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set required headers
	c.setRequiredHeaders(req)

	// Set optional headers
	c.setOptionalHeaders(req)

	// Send request with retry logic
	return c.sendWithRetry(ctx, req)
}

// sendWithRetry sends the request with retry logic
func (c *HTTPClient) sendWithRetry(ctx context.Context, req *http.Request) error {
	var lastErr error

	for attempt := 0; attempt <= c.config.Retry.Max; attempt++ {
		if attempt > 0 {
			// Wait before retry
			backoff := time.Duration(attempt) * c.config.Retry.Backoff
			c.log.Debugf("Retrying request in %v (attempt %d/%d)", backoff, attempt, c.config.Retry.Max)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		// Send request
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			c.log.Debugf("Request failed (attempt %d/%d): %v", attempt+1, c.config.Retry.Max+1, err)

			// Check if error is retryable
			if !isRetryableError(err) {
				return err
			}
			continue
		}

		// Check response status
		if err := c.handleResponse(resp); err != nil {
			lastErr = err
			c.log.Debugf("Response error (attempt %d/%d): %v", attempt+1, c.config.Retry.Max+1, err)

			// Check if error is retryable
			if !isRetryableHTTPError(resp.StatusCode) {
				resp.Body.Close()
				return err
			}
			resp.Body.Close()
			continue
		}

		resp.Body.Close()
		return nil
	}

	return fmt.Errorf("request failed after %d attempts: %w", c.config.Retry.Max+1, lastErr)
}

// handleResponse handles the HTTP response
func (c *HTTPClient) handleResponse(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		// Read response body to verify success message
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}

		// Check for expected success message
		if string(body) != "Successfully ingested record." {
			c.log.Warnf("Unexpected response body: %s", string(body))
		}

		return nil
	}

	// Read error response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("HTTP request failed with status %d (failed to read error body: %w)", resp.StatusCode, err)
	}

	return fmt.Errorf("HTTP request failed with status %d: %s", resp.StatusCode, string(body))
}

// setRequiredHeaders sets the required ZeroBus headers
func (c *HTTPClient) setRequiredHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	// Only set Authorization header manually when using PAT token
	// When using OAuth, the oauth2 client sets it automatically
	if !c.usingOAuth {
		req.Header.Set("Authorization", "Bearer "+c.config.PATToken)
	}
	req.Header.Set("unity-catalog-endpoint", c.config.WorkspaceURL)
	req.Header.Set("x-databricks-zerobus-table-name", c.config.TableName)
}

// setOptionalHeaders sets any optional custom headers
func (c *HTTPClient) setOptionalHeaders(req *http.Request) {
	for key, value := range c.config.Headers {
		req.Header.Set(key, value)
	}
}

// Close closes the HTTP client
func (c *HTTPClient) Close() error {
	// Close idle connections
	c.client.CloseIdleConnections()
	return nil
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	// Network errors, timeouts, and connection issues are retryable
	return true // Most network errors are retryable
}

// isRetryableHTTPError checks if an HTTP status code is retryable
func isRetryableHTTPError(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests: // 429
		return true
	case http.StatusInternalServerError: // 500
		return true
	case http.StatusBadGateway: // 502
		return true
	case http.StatusServiceUnavailable: // 503
		return true
	case http.StatusGatewayTimeout: // 504
		return true
	default:
		return false
	}
}
