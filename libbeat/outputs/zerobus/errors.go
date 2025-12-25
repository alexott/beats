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
