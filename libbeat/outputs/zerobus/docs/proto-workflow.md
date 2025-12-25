# Protocol Buffer Workflow Guide for Zerobus Output

This guide explains how to use Protocol Buffer (protobuf) format with the Zerobus output in Filebeat.

## Overview

The Zerobus output supports two ingestion modes:
- **JSON mode** (default): Events are sent as JSON
- **Proto mode**: Events are converted from JSON to Protocol Buffer format before sending

Proto mode provides:
- **Schema enforcement**: Strict validation against proto schema
- **Type safety**: Ensures data types match schema definitions
- **Smaller payload size**: Binary format is more compact than JSON
- **Better performance**: Faster serialization/deserialization

## Prerequisites

- Protocol Buffer compiler (`protoc`) version 3.x or later
- Basic understanding of Protocol Buffer syntax
- Filebeat with Zerobus output support

## Step 1: Create Your Proto Schema

Create a `.proto` file defining your message structure:

```proto
// log_event.proto
syntax = "proto3";

package logs;

message LogEvent {
  string timestamp = 1;
  string level = 2;
  string message = 3;
  string host = 4;
  map<string, string> labels = 5;
}
```

**Best Practices:**
- Use `proto3` syntax (required by Zerobus)
- Field names should match your JSON event structure
- **Important**: Use `_` prefix instead of `@` for Beats built-in fields (see Field Name Transformation below)
- Use appropriate proto types (string, int64, double, bool, etc.)
- Consider using `map<>` for dynamic key-value pairs
- Add comments describing each field's purpose

**Field Name Transformation:**

Beats uses field names starting with `@` (like `@timestamp` and `@metadata`), but protobuf field names cannot start with special characters. The Zerobus output **automatically transforms** these fields during proto conversion:

- `@timestamp` → `_timestamp`
- `@metadata` → `_metadata`
- Any field starting with `@` → starts with `_`

This transformation is recursive and applies to all nested objects and arrays. In your proto schema, use the underscore prefix:

```proto
message LogEvent {
  string _timestamp = 1;    // Matches @timestamp from Beats
  string _metadata = 2;     // Matches @metadata from Beats
  string message = 3;       // Regular fields remain unchanged
}
```

## Step 2: Generate Proto Descriptor

Generate a descriptor file containing the compiled proto schema:

```bash
protoc --descriptor_set_out=log_event.descriptor \
       --include_imports \
       log_event.proto
```

**Important flags:**
- `--descriptor_set_out`: Output file for the descriptor
- `--include_imports`: Include all dependencies (required)

**Verify the descriptor:**
```bash
# Check the descriptor was created
ls -lh log_event.descriptor

# Optional: Inspect the descriptor (if you have protoc with text format)
protoc --decode=google.protobuf.FileDescriptorSet \
       google/protobuf/descriptor.proto \
       < log_event.descriptor
```

## Step 3: Configure Filebeat

### Basic Proto Configuration

```yaml
filebeat.inputs:
  - type: filestream
    paths:
      - /var/log/*.log

output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.logs"

  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"

  # Proto mode configuration
  record_type: "proto"
  proto_descriptor_file: "/etc/filebeat/log_event.descriptor"
  proto_message_type: "logs.LogEvent"

  workers: 4
  batch_size: 1  # API limitation for JSON mode
```

**Configuration notes:**
- `record_type`: Must be set to `"proto"`
- `proto_descriptor_file`: Absolute path to `.descriptor` file
- `proto_message_type`: Fully qualified message name (package.MessageName) or just MessageName

## Step 4: Shape Data with Processors

Use Filebeat processors to transform events to match your proto schema:

```yaml
filebeat.inputs:
  - type: filestream
    paths:
      - /var/log/app/*.log

    processors:
      # Parse JSON logs
      - decode_json_fields:
          fields: ["message"]
          target: ""
          overwrite_keys: true

      # Add timestamp field
      - timestamp:
          field: "@timestamp"
          target_field: "timestamp"
          layouts:
            - '2006-01-02T15:04:05.000Z'

      # Rename fields to match proto schema
      - rename:
          fields:
            - from: "log.level"
              to: "level"
            - from: "agent.hostname"
              to: "host"

      # Add labels map
      - add_fields:
          target: "labels"
          fields:
            environment: "production"
            service: "my-app"

      # Drop unnecessary fields
      - drop_fields:
          fields: ["agent", "ecs", "input"]

output.zerobus:
  # ... proto configuration ...
```

**Processor tips:**
- Process data **before** proto conversion
- Ensure all required proto fields are present
- Remove fields not in your proto schema (use `drop_fields`)
- Convert types as needed (timestamps, numbers, etc.)

## Step 5: Deploy and Test

### Deploy the Descriptor File

```bash
# Copy descriptor to filebeat config directory
sudo cp log_event.descriptor /etc/filebeat/

# Set appropriate permissions
sudo chown root:root /etc/filebeat/log_event.descriptor
sudo chmod 644 /etc/filebeat/log_event.descriptor
```

### Test Configuration

```bash
# Test filebeat configuration
filebeat test config -c filebeat.yml

# Test output connection
filebeat test output -c filebeat.yml
```

### Run Filebeat

```bash
# Run in foreground for testing
filebeat -e -c filebeat.yml

# Run as service
sudo systemctl start filebeat
sudo systemctl status filebeat
```

## Troubleshooting

### Common Errors and Solutions

#### Error: "proto_descriptor_file is required when record_type is 'proto'"

**Cause:** Missing descriptor file configuration

**Solution:**
```yaml
record_type: "proto"
proto_descriptor_file: "/path/to/descriptor.descriptor"  # Add this
proto_message_type: "YourMessageType"                    # Add this
```

#### Error: "message type X not found in descriptor"

**Cause:** Message type name doesn't match proto definition

**Solutions:**
1. Use fully qualified name: `"package.MessageName"`
2. Check proto file for correct package and message names
3. Verify descriptor includes all imports

```bash
# Check your proto file
grep -E "^package|^message" log_event.proto
```

#### Error: "JSON→Proto conversion failed: unknown field"

**Cause:** JSON contains fields not in proto schema

**Solutions:**
1. Remove extra fields with processors:
```yaml
processors:
  - drop_fields:
      fields: ["extra_field1", "extra_field2"]
```

2. Add fields to your proto schema and regenerate descriptor

3. Use `map<string, string>` in proto for dynamic fields

#### Error: "unknown field @timestamp" or "unknown field @metadata"

**Cause:** Proto schema uses `@` prefix instead of `_` prefix

**Solution:**
The Zerobus output automatically transforms `@` to `_` in field names. Ensure your proto schema uses the underscore prefix:

```proto
message LogEvent {
  string _timestamp = 1;  // NOT @timestamp
  string _metadata = 2;   // NOT @metadata
}
```

**Note:** This transformation is automatic - you don't need processors. The output handles it during JSON→Proto conversion.

#### Error: "JSON→Proto conversion failed: missing required field"

**Cause:** Required proto fields missing from JSON

**Solutions:**
1. Add missing fields with processors:
```yaml
processors:
  - add_fields:
      target: ""
      fields:
        required_field: "default_value"
```

2. Make field optional in proto (proto3 fields are optional by default)

#### Error: "JSON→Proto conversion failed: type mismatch"

**Cause:** JSON value type doesn't match proto field type

**Example:** String value for int64 field

**Solutions:**
1. Convert types with processors:
```yaml
processors:
  - convert:
      fields:
        - {from: "count", type: "integer"}
        - {from: "price", type: "float"}
```

2. Update proto schema to match actual data types

### Debugging Tips

#### Enable Debug Logging

```yaml
logging.level: debug
logging.selectors: ["*"]
```

#### Check Event Structure

Add console output to see processed events:

```yaml
output.console:
  pretty: true

# Comment out zerobus output temporarily
# output.zerobus:
#   ...
```

#### Validate Proto Schema

Test JSON→Proto conversion outside Filebeat:

```bash
# Create test JSON
echo '{"timestamp":"2024-01-01T00:00:00Z","level":"info","message":"test","host":"localhost"}' > test.json

# Use protoc to validate (if you have the compiled proto)
protoc --encode=logs.LogEvent log_event.proto < test.json > test.bin
protoc --decode=logs.LogEvent log_event.proto < test.bin
```

## Advanced Topics

### Nested Messages

```proto
message LogEvent {
  string message = 1;
  UserInfo user = 2;  // Nested message
}

message UserInfo {
  string id = 1;
  string name = 2;
}
```

Filebeat configuration:
```yaml
processors:
  - add_fields:
      target: "user"
      fields:
        id: "${USER_ID}"
        name: "${USER_NAME}"
```

### Repeated Fields (Arrays)

```proto
message LogEvent {
  string message = 1;
  repeated string tags = 2;  // Array of strings
}
```

Filebeat configuration:
```yaml
processors:
  - add_tags:
      tags: ["production", "app-logs"]
      target: "tags"
```

### Timestamps

Proto timestamp format:
```proto
import "google/protobuf/timestamp.proto";

message LogEvent {
  google.protobuf.Timestamp event_time = 1;
}
```

**Note:** Currently, timestamps should be formatted as RFC3339 strings. Native protobuf timestamp support may be added in future versions.

### Maps for Dynamic Data

```proto
message LogEvent {
  string message = 1;
  map<string, string> metadata = 2;  // Dynamic key-value pairs
}
```

This allows any key-value pairs without schema changes.

## Performance Considerations

### Proto vs JSON

| Aspect | JSON | Proto |
|--------|------|-------|
| Payload size | Larger (~1.5-2x) | Smaller |
| CPU usage | Lower | Higher (conversion overhead) |
| Schema validation | None | Strict |
| Network bandwidth | Higher | Lower |
| Debugging | Easy (human-readable) | Harder (binary format) |

### When to Use Proto Mode

**Use Proto when:**
- Schema validation is critical
- Network bandwidth is limited
- Data quality issues need to be caught early
- Smaller payload size is beneficial

**Use JSON when:**
- Schema is frequently changing
- Debugging/troubleshooting is important
- Lower CPU overhead is preferred
- Simpler configuration is desired

## Example Schemas

### Application Logs

```proto
syntax = "proto3";
package app;

message AppLog {
  string timestamp = 1;
  string level = 2;        // DEBUG, INFO, WARN, ERROR
  string message = 3;
  string service_name = 4;
  string trace_id = 5;
  string span_id = 6;
  map<string, string> labels = 7;
  int64 duration_ms = 8;
}
```

### Metrics/Events

```proto
syntax = "proto3";
package metrics;

message MetricEvent {
  string timestamp = 1;
  string metric_name = 2;
  double value = 3;
  string unit = 4;
  map<string, string> dimensions = 5;
}
```

### Structured Logs

```proto
syntax = "proto3";
package logs;

message StructuredLog {
  string timestamp = 1;
  string severity = 2;
  string source = 3;
  string message = 4;
  map<string, string> fields = 5;
  repeated string tags = 6;
}
```

## References

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Filebeat Processors](https://www.elastic.co/guide/en/beats/filebeat/current/filtering-and-enhancing-data.html)
- [Zerobus Output Configuration](zerobus.asciidoc)

## Getting Help

If you encounter issues:
1. Check Filebeat logs for detailed error messages
2. Verify proto descriptor is accessible and valid
3. Test JSON structure matches proto schema
4. Use debug logging to inspect event transformations
