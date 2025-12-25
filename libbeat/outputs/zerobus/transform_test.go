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
	"encoding/json"
	"testing"
)

// TestTransformJSONFieldNames tests the @ → _ and message → msg transformations
func TestTransformJSONFieldNames(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple @timestamp",
			input:    `{"@timestamp":"2024-01-01T00:00:00Z","message":"test"}`,
			expected: `{"_timestamp":"2024-01-01T00:00:00Z","msg":"test"}`,
		},
		{
			name:     "multiple @ fields",
			input:    `{"@timestamp":"2024-01-01T00:00:00Z","@metadata":{"beat":"filebeat"},"message":"test"}`,
			expected: `{"_timestamp":"2024-01-01T00:00:00Z","_metadata":{"beat":"filebeat"},"msg":"test"}`,
		},
		{
			name:     "nested @ fields",
			input:    `{"@timestamp":"2024-01-01T00:00:00Z","nested":{"@special":"value","normal":"data"}}`,
			expected: `{"_timestamp":"2024-01-01T00:00:00Z","nested":{"_special":"value","normal":"data"}}`,
		},
		{
			name:     "@ in array of objects",
			input:    `{"items":[{"@id":"1","name":"test"},{"@id":"2","name":"test2"}]}`,
			expected: `{"items":[{"_id":"1","name":"test"},{"_id":"2","name":"test2"}]}`,
		},
		{
			name:     "message field transformation",
			input:    `{"timestamp":"2024-01-01T00:00:00Z","message":"hello world"}`,
			expected: `{"timestamp":"2024-01-01T00:00:00Z","msg":"hello world"}`,
		},
		{
			name:     "nested message field",
			input:    `{"level":"info","nested":{"message":"nested message","status":"ok"}}`,
			expected: `{"level":"info","nested":{"msg":"nested message","status":"ok"}}`,
		},
		{
			name:     "message in array",
			input:    `{"logs":[{"message":"log1","severity":"info"},{"message":"log2","severity":"error"}]}`,
			expected: `{"logs":[{"msg":"log1","severity":"info"},{"msg":"log2","severity":"error"}]}`,
		},
		{
			name:     "combined @ and message transformations",
			input:    `{"@timestamp":"2024-01-01T00:00:00Z","@metadata":{"beat":"filebeat"},"message":"test log"}`,
			expected: `{"_timestamp":"2024-01-01T00:00:00Z","_metadata":{"beat":"filebeat"},"msg":"test log"}`,
		},
		{
			name:     "no transformations needed",
			input:    `{"timestamp":"2024-01-01T00:00:00Z","msg":"already correct"}`,
			expected: `{"timestamp":"2024-01-01T00:00:00Z","msg":"already correct"}`,
		},
		{
			name:     "empty object",
			input:    `{}`,
			expected: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := transformJSONFieldNames([]byte(tt.input))
			if err != nil {
				t.Fatalf("transformJSONFieldNames() error = %v", err)
			}

			// Compare as JSON objects (order may vary)
			var resultObj, expectedObj map[string]interface{}
			if err := json.Unmarshal(result, &resultObj); err != nil {
				t.Fatalf("Failed to unmarshal result: %v", err)
			}
			if err := json.Unmarshal([]byte(tt.expected), &expectedObj); err != nil {
				t.Fatalf("Failed to unmarshal expected: %v", err)
			}

			// Deep compare
			if !deepEqual(resultObj, expectedObj) {
				t.Errorf("transformJSONFieldNames() = %s, want %s", string(result), tt.expected)
			}
		})
	}
}

// TestTransformMap tests the map transformation function
func TestTransformMap(t *testing.T) {
	input := map[string]interface{}{
		"@timestamp": "2024-01-01T00:00:00Z",
		"@metadata": map[string]interface{}{
			"beat": "filebeat",
		},
		"message": "test message",
		"nested": map[string]interface{}{
			"@special": "value",
			"message":  "nested message",
			"normal":   "data",
		},
	}

	result := transformMap(input)

	// Check @ transformations
	if _, exists := result["@timestamp"]; exists {
		t.Error("@timestamp should be transformed")
	}
	if _, exists := result["_timestamp"]; !exists {
		t.Error("_timestamp should exist")
	}
	if _, exists := result["@metadata"]; exists {
		t.Error("@metadata should be transformed")
	}
	if _, exists := result["_metadata"]; !exists {
		t.Error("_metadata should exist")
	}

	// Check message transformation
	if _, exists := result["message"]; exists {
		t.Error("message should be transformed to msg")
	}
	if _, exists := result["msg"]; !exists {
		t.Error("msg should exist")
	}
	if result["msg"] != "test message" {
		t.Error("msg value should be preserved")
	}

	// Check nested transformations
	nested, ok := result["nested"].(map[string]interface{})
	if !ok {
		t.Fatal("nested should be a map")
	}
	if _, exists := nested["@special"]; exists {
		t.Error("nested @special should be transformed")
	}
	if _, exists := nested["_special"]; !exists {
		t.Error("nested _special should exist")
	}
	if _, exists := nested["message"]; exists {
		t.Error("nested message should be transformed to msg")
	}
	if _, exists := nested["msg"]; !exists {
		t.Error("nested msg should exist")
	}
	if nested["normal"] != "data" {
		t.Error("normal field should remain unchanged")
	}
}

// TestTransformSlice tests the slice transformation function
func TestTransformSlice(t *testing.T) {
	input := []interface{}{
		map[string]interface{}{
			"@id":   "1",
			"name":  "test1",
		},
		map[string]interface{}{
			"@id":   "2",
			"name":  "test2",
		},
		"string value",
		123,
	}

	result := transformSlice(input)

	if len(result) != len(input) {
		t.Fatalf("Expected length %d, got %d", len(input), len(result))
	}

	// Check first object
	obj1, ok := result[0].(map[string]interface{})
	if !ok {
		t.Fatal("First item should be a map")
	}
	if _, exists := obj1["@id"]; exists {
		t.Error("@id should be transformed")
	}
	if _, exists := obj1["_id"]; !exists {
		t.Error("_id should exist")
	}

	// Check second object
	obj2, ok := result[1].(map[string]interface{})
	if !ok {
		t.Fatal("Second item should be a map")
	}
	if _, exists := obj2["@id"]; exists {
		t.Error("@id should be transformed")
	}
	if _, exists := obj2["_id"]; !exists {
		t.Error("_id should exist")
	}

	// Check non-map values remain unchanged
	if result[2] != "string value" {
		t.Error("String value should remain unchanged")
	}
	if result[3] != 123 {
		t.Error("Integer value should remain unchanged")
	}
}

// deepEqual recursively compares two interface{} values
func deepEqual(a, b interface{}) bool {
	switch aVal := a.(type) {
	case map[string]interface{}:
		bMap, ok := b.(map[string]interface{})
		if !ok || len(aVal) != len(bMap) {
			return false
		}
		for k, v := range aVal {
			if !deepEqual(v, bMap[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bSlice, ok := b.([]interface{})
		if !ok || len(aVal) != len(bSlice) {
			return false
		}
		for i, v := range aVal {
			if !deepEqual(v, bSlice[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
