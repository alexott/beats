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

The output creates a long-lived gRPC stream to the Zerobus service and uses a **persistent worker pool** with **pipelined encoding** for optimal throughput and low latency.

### Key Components

1. **Persistent Worker Pool**
   - Workers are created once during initialization and reused for all batches
   - Eliminates goroutine creation/destruction overhead
   - Configurable worker count (`workers` setting)
   - Default: 4 workers (can be increased to 100)

2. **Pipelined Encoding and Submission**
   - Events are encoded serially (codec thread-safety requirement)
   - Each encoded event is immediately submitted to the worker pool
   - Workers process events while encoding continues
   - Reduces end-to-end latency by 5-45% depending on encoding complexity

3. **Channel-Based Communication**
   - Work items sent via buffered channel (sized to `batch_size`)
   - Results collected via separate buffered channel
   - Prevents deadlocks and provides natural backpressure
   - Zero-copy data passing between encoder and workers

4. **SDK Integration**
   The SDK handles:
   - Authentication token management
   - Automatic retry of transient failures
   - Stream recovery on disconnection
   - Backpressure control (up to 1M in-flight requests)
   - Acknowledgment tracking

### Performance Characteristics

| Batch Size | Workers | Estimated Throughput | Latency Improvement |
|------------|---------|---------------------|---------------------|
| 1 (default) | 4 | ~100 events/sec | Minimal |
| 100 | 4 | ~1,000 events/sec | 10-20% |
| 100 | 16 | ~4,000 events/sec | 10-20% |
| 1000 | 16 | ~10,000 events/sec | 30-45% (proto mode) |

**Note:** Actual throughput depends on event size, network latency, and server capacity.

## Build Requirements

- Go 1.21+
- CGO enabled (`CGO_ENABLED=1`)
- Rust toolchain 1.75+
- C compiler (gcc/clang/MinGW-w64)

## Documentation

- **[README.md](README.md)** - This file, overview and architecture
- **[docs/proto-workflow.md](docs/proto-workflow.md)** - Complete Protocol Buffer workflow guide
- **[docs/debugging-quick-reference.md](docs/debugging-quick-reference.md)** - Quick debugging reference
- **[docs/zerobus.asciidoc](docs/zerobus.asciidoc)** - Complete configuration reference

## Configuration

See **[docs/zerobus.asciidoc](docs/zerobus.asciidoc)** for complete configuration reference.

### Quick Start

**Minimal JSON configuration:**
```yaml
output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.table"
  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"
  workers: 4       # Default: 4, increase for higher throughput
  batch_size: 1    # API limitation for JSON mode
```

**Proto mode configuration:**
```yaml
output.zerobus:
  # ... same as above ...
  record_type: "proto"
  proto_descriptor_file: "/etc/filebeat/descriptor.descriptor"
  proto_message_type: "package.MessageType"
  workers: 16      # Higher workers for proto mode
  batch_size: 100  # Can be higher in proto mode
```

**Important:** The Zerobus API currently requires `batch_size: 1` for JSON mode. This is an API limitation. To achieve high throughput, increase the `workers` setting (e.g., `workers: 16-50`).

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

## Integration Testing

Integration tests require a Databricks workspace with Zerobus enabled.

1. Copy `.env.example` to `.env` and fill in your credentials
2. Source the environment variables: `source .env`
3. Run integration tests:

```bash
go test -v -tags=integration
```

## Debugging and Monitoring

### Enable Debug Logging

To troubleshoot issues or monitor worker behavior, enable debug logging:

```yaml
# filebeat.yml
logging.level: debug
logging.selectors: ["zerobus"]  # Only zerobus output logs
```

Or for all components:

```yaml
logging.level: debug
logging.selectors: ["*"]
```

### Debug Log Messages

With debug logging enabled, you'll see:

**Worker Lifecycle:**
```
Worker 0 started
Worker 1 started
Worker 2 started
Worker 3 started
```

**Batch Processing:**
```
Encoded and submitted 100 events (0 encoding failures)
Worker 0: Event 0 acknowledged at offset 12345
Worker 1: Event 1 acknowledged at offset 12346
...
Batch complete: 100 acked, 0 encoding failures, 0 worker failures
```

**Graceful Shutdown:**
```
Closing Zerobus output
Worker 0: Channel closed, exiting
Worker 1: Channel closed, exiting
All workers stopped
```

**Error Scenarios:**
```
Worker 2: Failed to ingest event 42: connection refused
Worker 3: Failed to await ack for event 43: timeout
Batch complete: 98 acked, 0 encoding failures, 2 worker failures
```

### Performance Monitoring

#### Key Metrics to Monitor

1. **Throughput:**
   - Events/second processed
   - Monitor `acked` count in batch completion logs
   - Compare against expected ingestion rate

2. **Latency:**
   - Time between batch start and completion
   - Track using debug logs or external monitoring

3. **Error Rates:**
   - `encoding failures`: JSON/Proto conversion errors
   - `worker failures`: Network or SDK errors
   - Permanent errors trigger Beats retry mechanism

4. **Resource Usage:**
   - Goroutine count (should be stable at `workers` count)
   - Memory usage (increases with `batch_size`)
   - CPU usage (higher in proto mode due to conversion)

#### Monitor with Beats Monitoring

Enable Beats internal monitoring:

```yaml
monitoring.enabled: true
monitoring.elasticsearch:
  hosts: ["localhost:9200"]
```

Key metrics:
- `libbeat.output.events.acked`: Successfully delivered events
- `libbeat.output.events.failed`: Failed events
- `libbeat.output.write.bytes`: Network bytes written

### Troubleshooting Guide

#### High Latency

**Symptoms:** Batches taking too long to process

**Diagnosis:**
```yaml
logging.level: debug
logging.selectors: ["zerobus"]
```

Check logs for:
- Long gaps between "Encoded and submitted" and "Batch complete"
- Individual event ACK delays

**Solutions:**
1. Increase workers:
   ```yaml
   output.zerobus:
     workers: 16  # Increase from default 4
   ```

2. Increase batch size (if API allows):
   ```yaml
   output.zerobus:
     batch_size: 100  # Increase from default 1
   ```

3. Check network latency to Zerobus endpoint

#### Memory Issues

**Symptoms:** High memory usage, OOM kills

**Diagnosis:**
```bash
# Monitor memory during operation
watch -n 1 'ps aux | grep filebeat'
```

**Solutions:**
1. Reduce batch size:
   ```yaml
   output.zerobus:
     batch_size: 100  # Reduce from 1000
   ```
   Note: Channels are sized to `batch_size` (each event ~500-2000 bytes)

2. Reduce workers:
   ```yaml
   output.zerobus:
     workers: 4  # Reduce from 16
   ```

3. Check for memory leaks in SDK (upgrade SDK if needed)

#### Worker Stalls

**Symptoms:** Workers stop processing, batches hang

**Diagnosis:**
```yaml
logging.level: debug
logging.selectors: ["zerobus"]
```

Look for:
- Missing "Event X acknowledged" messages
- No "Batch complete" messages
- Workers stuck on specific events

**Solutions:**
1. Check network connectivity to Zerobus endpoint
2. Verify OAuth credentials are valid
3. Increase SDK timeout settings:
   ```yaml
   output.zerobus:
     sdk_options:
       server_lack_of_ack_timeout_ms: 120000  # Increase from 60s
   ```

#### Encoding Failures

**Symptoms:** Many `encoding failures` in logs

**Diagnosis:**
Look for specific error messages:
```
Failed to encode event 42: JSON→Proto conversion failed: unknown field "extra"
```

**Solutions:**

For **JSON mode:**
- Check codec configuration
- Verify event structure is valid JSON

For **Proto mode:**
- Ensure all JSON fields exist in proto schema
- Use processors to remove extra fields:
  ```yaml
  processors:
    - drop_fields:
        fields: ["extra_field"]
  ```
- Check field name transformations (@ → _, message → msg)

#### Connection Errors

**Symptoms:** "Failed to ingest event" or "connection refused"

**Diagnosis:**
```
Worker 2: Failed to ingest event 5: connection refused
```

**Solutions:**
1. Verify Zerobus URI is correct:
   ```yaml
   output.zerobus:
     zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
   ```

2. Check OAuth credentials:
   ```bash
   # Test authentication
   filebeat test output -c filebeat.yml
   ```

3. Verify network connectivity:
   ```bash
   # Test DNS resolution
   nslookup 12345.zerobus.region.cloud.databricks.com

   # Test connectivity
   telnet 12345.zerobus.region.cloud.databricks.com 443
   ```

4. Check firewall rules (allow outbound HTTPS/gRPC)

### Performance Tuning

#### For Maximum Throughput

```yaml
output.zerobus:
  workers: 50              # Increase workers (max 100)
  batch_size: 1000         # Increase batch size (if API allows)

  sdk_options:
    max_inflight_requests: 1000000  # SDK can buffer 1M requests
    flush_timeout_ms: 300000         # 5 minute flush timeout
```

#### For Low Latency

```yaml
output.zerobus:
  workers: 8               # Moderate worker count
  batch_size: 10           # Smaller batches for faster feedback

  sdk_options:
    flush_timeout_ms: 10000  # 10 second flush timeout
```

#### For Resource-Constrained Environments

```yaml
output.zerobus:
  workers: 2               # Minimal workers
  batch_size: 10           # Small batches

  sdk_options:
    max_inflight_requests: 1000  # Limit SDK buffer
```

### Advanced Debugging

#### Capture Event Structure

Temporarily enable console output to see event structure:

```yaml
# Add alongside zerobus output
output.console:
  pretty: true
  enabled: true

# Zerobus output continues working
output.zerobus:
  # ... config ...
```

#### Monitor Goroutines

Check goroutine count remains stable:

```bash
# Filebeat with HTTP endpoint enabled
curl -s http://localhost:5066/debug/vars | jq '.cmdline, .goroutines'
```

Expected: Goroutine count = workers + overhead (~10-20 base goroutines)

#### Profile Memory/CPU

Enable profiling:

```yaml
http.enabled: true
http.host: localhost
http.port: 5066
```

Capture profiles:
```bash
# CPU profile (30 seconds)
go tool pprof http://localhost:5066/debug/pprof/profile?seconds=30

# Memory profile
go tool pprof http://localhost:5066/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:5066/debug/pprof/goroutine
```
