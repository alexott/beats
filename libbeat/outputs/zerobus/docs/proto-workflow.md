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

Beats uses field names that are incompatible with protobuf naming rules. The Zerobus output **automatically transforms** these fields during proto conversion:

**@ prefix transformation:**
- `@timestamp` → `_timestamp`
- `@metadata` → `_metadata`
- Any field starting with `@` → starts with `_`

**Reserved keyword transformation:**
- `message` → `msg` (protobuf reserved keyword)

These transformations are recursive and apply to all nested objects and arrays. In your proto schema, use the transformed names:

```proto
message LogEvent {
  string _timestamp = 1;    // Matches @timestamp from Beats
  string _metadata = 2;     // Matches @metadata from Beats
  string msg = 3;           // Matches "message" field from Beats (reserved keyword)
  string level = 4;         // Regular fields remain unchanged
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

#### Error: "unknown field @timestamp", "@metadata", or "message"

**Cause:** Proto schema doesn't use transformed field names

**Solution:**
The Zerobus output automatically transforms incompatible field names. Ensure your proto schema uses the transformed names:

```proto
message LogEvent {
  string _timestamp = 1;  // NOT @timestamp (@ → _ transformation)
  string _metadata = 2;   // NOT @metadata (@ → _ transformation)
  string msg = 3;         // NOT message (reserved keyword → msg)
}
```

**Note:** These transformations are automatic - you don't need processors. The output handles them during JSON→Proto conversion:
- Fields starting with `@` → start with `_`
- Field `message` → `msg`

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

For zerobus-specific logs only:

```yaml
logging.level: debug
logging.selectors: ["zerobus"]
```

For all components:

```yaml
logging.level: debug
logging.selectors: ["*"]
```

**Debug output includes:**
- Worker lifecycle (startup/shutdown)
- Individual event acknowledgments with offsets
- Batch processing summaries
- Encoding and worker failure counts
- Performance metrics per batch

**Example debug output:**
```
INFO  [zerobus] Initialized Zerobus output for table: catalog.schema.logs (workers=4, mode=proto, batch_size=100, channel_buffer=100)
DEBUG [zerobus] Worker 0 started
DEBUG [zerobus] Worker 1 started
DEBUG [zerobus] Encoded and submitted 100 events (0 encoding failures)
DEBUG [zerobus] Worker 0: Event 0 acknowledged at offset 12345
DEBUG [zerobus] Worker 1: Event 1 acknowledged at offset 12346
DEBUG [zerobus] Batch complete: 100 acked, 0 encoding failures, 0 worker failures
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

Or run both outputs simultaneously to see events while still ingesting:

```yaml
output.console:
  pretty: true
  enabled: true

output.zerobus:
  # ... normal config ...
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
| Encoding latency | ~1ms/event | ~10-20ms/event |

### When to Use Proto Mode

**Use Proto when:**
- Schema validation is critical
- Network bandwidth is limited
- Data quality issues need to be caught early
- Smaller payload size is beneficial
- Type safety is important

**Use JSON when:**
- Schema is frequently changing
- Debugging/troubleshooting is important
- Lower CPU overhead is preferred
- Simpler configuration is desired
- Faster event encoding is needed

### Worker Pool and Pipelining Benefits

The Zerobus output uses a **persistent worker pool** with **pipelined encoding**:

**Key Benefits:**
1. **Eliminates goroutine churn** - workers are reused across all batches
2. **Overlaps encoding with ingestion** - workers process while encoding continues
3. **Natural backpressure** - channels sized to batch_size prevent overload
4. **Predictable resource usage** - stable goroutine count

**Performance Impact:**

| Configuration | Throughput Gain | Latency Reduction |
|---------------|----------------|-------------------|
| JSON, batch_size=1 | Minimal | Minimal |
| JSON, batch_size=100, workers=4 | 1.1x | 5-10% |
| Proto, batch_size=100, workers=4 | 1.3x | 20-30% |
| Proto, batch_size=1000, workers=16 | 1.8x | 30-45% |

**Why proto mode benefits more:**
- Encoding is slower (10-20ms vs 1ms)
- More overlap between encoding and ingestion
- Workers stay busy while encoder processes complex conversions

### Tuning for Proto Mode

For maximum throughput with proto:

```yaml
output.zerobus:
  record_type: "proto"
  proto_descriptor_file: "/path/to/descriptor.descriptor"
  proto_message_type: "package.MessageType"

  # Increase workers to maximize parallel ingestion
  workers: 16              # Default: 4, Max: 100

  # Increase batch size (if API allows)
  batch_size: 1000         # Default: 1

  # SDK options for high throughput
  sdk_options:
    max_inflight_requests: 1000000  # SDK buffer size
    flush_timeout_ms: 300000        # 5 minutes
```

**Expected performance:**
- With 16 workers and batch_size=1000
- Proto mode encoding: ~20 seconds for 1000 events
- Ingestion throughput: ~10,000 events/sec
- Total latency: ~25 seconds (vs 45s without pipelining)

### Memory Considerations

**Channel buffer sizing:**
- Channels sized to `batch_size` to prevent deadlocks
- Memory per batch = `batch_size * avg_event_size`
- Example: batch_size=1000, event_size=1KB → ~1MB per batch

**Recommendations:**
- batch_size ≤ 1000 for most use cases (~1-2MB memory)
- batch_size ≤ 100 for memory-constrained environments
- Monitor memory with debug logging enabled

**Check memory usage:**
```bash
# Monitor Filebeat memory
watch -n 1 'ps aux | grep filebeat | grep -v grep'
```

## Example Schemas

### Application Logs

```proto
syntax = "proto3";
package app;

message AppLog {
  string _timestamp = 1;     // Matches @timestamp (auto-transformed)
  string level = 2;          // DEBUG, INFO, WARN, ERROR
  string msg = 3;            // Matches "message" field (auto-transformed)
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
  string _timestamp = 1;     // Matches @timestamp (auto-transformed)
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
  string _timestamp = 1;     // Matches @timestamp (auto-transformed)
  string severity = 2;
  string source = 3;
  string msg = 4;            // Matches "message" field (auto-transformed)
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
