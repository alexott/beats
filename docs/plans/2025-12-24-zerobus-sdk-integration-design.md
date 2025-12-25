# Zerobus Go SDK Integration Design

**Date:** 2025-12-24
**Status:** Approved
**Author:** Design Session with User

## Overview

This document describes the design for integrating the [Zerobus Go SDK](https://github.com/databricks/zerobus-sdk-go) into Filebeat as a new output type. The integration will replace the current manual HTTP implementation with the SDK's higher-level abstractions, providing better reliability, performance, and Protocol Buffer support.

## Background

### Current Implementation (zerobushttp)

The existing `libbeat/outputs/zerobushttp` package:
- Makes direct HTTP POST requests to Zerobus REST API
- Manually handles OAuth token management
- JSON-only ingestion via Beats codec system
- Worker pool for parallel HTTP requests
- No automatic retry/recovery (relies on Beats retry mechanism)

### Zerobus Go SDK

The [Zerobus Go SDK](https://github.com/databricks/zerobus-sdk-go) provides:
- High-level streaming API built on Rust implementation (via CGO)
- Automatic OAuth 2.0 authentication with Unity Catalog
- Built-in retry and stream recovery
- Backpressure management via `MaxInflightRequests`
- Async acknowledgments for ingested records
- Support for both JSON and Protocol Buffer ingestion
- Static linking (no runtime dependencies)

## Goals

1. **Phase 1**: Replace HTTP-based implementation with SDK for JSON ingestion
2. **Phase 2**: Add Protocol Buffer support using SDK capabilities
3. Maintain backward-compatible configuration (easy migration)
4. Leverage SDK's reliability features (recovery, retries)
5. Integrate with Beats patterns (DLQ, metrics, codec system)

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│ Filebeat Input Pipeline                                     │
│ (file readers, processors, codec)                          │
└───────────────────────────┬─────────────────────────────────┘
                            │ Batch of Events (JSON maps)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ zerobus Output (New)                                        │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Output Client (implements outputs.Client)            │  │
│  │  - Long-lived stream (created at initialization)     │  │
│  │  - Goroutine pool for parallel ingestion            │  │
│  │  - Acknowledgment tracking                           │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                 │
│  Phase 1: JSON Mode       │      Phase 2: Proto Mode       │
│  ┌────────────────────┐   │   ┌─────────────────────────┐  │
│  │ Codec → JSON bytes │───┼──▶│ JSON → protojson        │  │
│  └────────────────────┘   │   │    → Proto bytes        │  │
│                            │   └─────────────────────────┘  │
│                            ▼                                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Zerobus Go SDK                                       │  │
│  │  stream.IngestRecord(jsonBytes | protoBytes)        │  │
│  │  ack.Await() → offset                               │  │
│  └──────────────────────────────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────┘
                            │ gRPC (HTTP/2 bidirectional)
                            ▼
                ┌───────────────────────┐
                │ Databricks Zerobus    │
                │ Service               │
                └───────────────────────┘
```

### Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Stream lifecycle** | Long-lived (single stream per output) | Leverages SDK's recovery features, reduces connection overhead |
| **Concurrency model** | Goroutine pool calling `IngestRecord()` | Balances parallelism with SDK's backpressure handling |
| **Error handling** | Dead letter queue pattern | Integrates with Beats' existing retry/DLQ infrastructure |
| **Stream failure** | No automatic recreation | Simpler semantics, clearer failure signals to operators |
| **Proto encoding** | JSON→Proto via protojson | Leverages existing Beats processors for data shaping |
| **Proto validation** | Strict (fail on mismatch) | Ensures data quality, makes schema issues visible |

## Phase 1: JSON Ingestion with SDK

### Package Structure

```
libbeat/outputs/zerobus/
├── zerobus.go          # Main output implementation
├── config.go           # Configuration structures
├── errors.go           # Error handling utilities
├── zerobus_test.go     # Unit tests
├── docs/
│   └── zerobus.asciidoc # User documentation
└── vendor/
    └── zerobus-go-sdk/    # SDK dependency (or via go.mod)
```

### Output Implementation

#### Initialization

```go
type zerobusOutput struct {
    log      *logp.Logger
    config   *Config
    codec    codec.Codec
    observer outputs.Observer
    index    string

    // SDK components
    sdk      *zerobus.ZerobusSdk
    stream   *zerobus.ZerobusStream

    // Concurrency control
    workerSem chan struct{}  // Semaphore for worker pool
}

func newZerobusOutput(
    beat beat.Info,
    observer outputs.Observer,
    config *Config,
    codec codec.Codec,
) (*zerobusOutput, error) {
    // 1. Create SDK instance
    sdk, err := zerobus.NewZerobusSdk(
        config.ZeroBusURI,
        config.WorkspaceURL,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to create SDK: %w", err)
    }

    // 2. Configure stream options
    options := zerobus.DefaultStreamConfigurationOptions()
    options.RecordType = zerobus.RecordTypeJson
    options.MaxInflightRequests = config.SDKOptions.MaxInflightRequests
    options.Recovery = config.SDKOptions.Recovery
    options.RecoveryRetries = config.SDKOptions.RecoveryRetries
    options.RecoveryTimeoutMs = config.SDKOptions.RecoveryTimeoutMs
    options.RecoveryBackoffMs = config.SDKOptions.RecoveryBackoffMs
    options.FlushTimeoutMs = config.SDKOptions.FlushTimeoutMs
    options.ServerLackOfAckTimeoutMs = config.SDKOptions.ServerLackOfAckTimeoutMs

    // 3. Create long-lived stream
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

    // 4. Create worker semaphore
    workerSem := make(chan struct{}, config.Workers)

    output := &zerobusOutput{
        log:       beat.Logger.Named("zerobus"),
        config:    config,
        codec:     codec,
        observer:  observer,
        index:     beat.Beat,
        sdk:       sdk,
        stream:    stream,
        workerSem: workerSem,
    }

    output.log.Infof("Initialized Zerobus output for table: %s (%d workers)",
        config.TableName, config.Workers)

    return output, nil
}
```

#### Batch Publishing

```go
type encodedEvent struct {
    index int
    data  []byte
    err   error
}

type result struct {
    index   int
    success bool
    err     error
}

func (o *zerobusOutput) Publish(ctx context.Context, batch publisher.Batch) error {
    events := batch.Events()
    o.observer.NewBatch(len(events))

    // Phase 1: Pre-encode all events (codec is not thread-safe)
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
                <-o.workerSem  // Release worker
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

#### Graceful Shutdown

```go
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

func (o *zerobusOutput) String() string {
    return fmt.Sprintf("zerobus(%s)", o.config.TableName)
}
```

## Phase 2: Protocol Buffer Support

### Proto Descriptor Loading

```go
type Config struct {
    // ... existing fields ...
    RecordType          string `config:"record_type"`           // "json" or "proto"
    ProtoDescriptorFile string `config:"proto_descriptor_file"` // Path to .descriptor file
    ProtoMessageType    string `config:"proto_message_type"`    // Message type name
}

func newZerobusOutput(...) (*zerobusOutput, error) {
    var descriptorBytes []byte
    var messageDescriptor protoreflect.MessageDescriptor

    if config.RecordType == "proto" {
        // 1. Load descriptor file
        descriptorBytes, err = os.ReadFile(config.ProtoDescriptorFile)
        if err != nil {
            return nil, fmt.Errorf("failed to load proto descriptor: %w", err)
        }

        // 2. Parse descriptor
        fileDescSet := &descriptorpb.FileDescriptorSet{}
        if err := proto.Unmarshal(descriptorBytes, fileDescSet); err != nil {
            return nil, fmt.Errorf("failed to parse descriptor: %w", err)
        }

        // 3. Find message type
        files, err := protodesc.NewFiles(fileDescSet)
        if err != nil {
            return nil, fmt.Errorf("failed to create file registry: %w", err)
        }

        messageDescriptor, err = findMessageDescriptor(files, config.ProtoMessageType)
        if err != nil {
            return nil, fmt.Errorf("message type %s not found: %w",
                config.ProtoMessageType, err)
        }
    }

    // Create stream with descriptor (for proto mode)
    tableProps := zerobus.TableProperties{
        TableName: config.TableName,
    }
    if config.RecordType == "proto" {
        tableProps.DescriptorProto = descriptorBytes
    }

    options := zerobus.DefaultStreamConfigurationOptions()
    if config.RecordType == "proto" {
        options.RecordType = zerobus.RecordTypeProto
    } else {
        options.RecordType = zerobus.RecordTypeJson
    }

    stream, err := sdk.CreateStream(tableProps, clientID, clientSecret, options)

    // Store message descriptor for later use
    output.messageDescriptor = messageDescriptor

    return output, nil
}
```

### JSON → Proto Conversion

```go
func (o *zerobusOutput) encodeEvent(event *publisher.Event) ([]byte, error) {
    if o.config.RecordType == "json" {
        // Phase 1: Use codec for JSON
        return o.codec.Encode(o.index, &event.Content)
    }

    // Phase 2: JSON → Proto conversion

    // 1. Encode to JSON first
    jsonBytes, err := o.codec.Encode(o.index, &event.Content)
    if err != nil {
        return nil, fmt.Errorf("JSON encoding failed: %w", err)
    }

    // 2. Create dynamic proto message
    msg := dynamicpb.NewMessage(o.messageDescriptor)

    // 3. Convert JSON → Proto (strict validation)
    unmarshaler := protojson.UnmarshalOptions{
        AllowPartial:   false,  // Require all required fields
        DiscardUnknown: false,  // Fail on unknown fields
    }

    if err := unmarshaler.Unmarshal(jsonBytes, msg); err != nil {
        return nil, &ProtoConversionError{
            Message:      fmt.Sprintf("JSON→Proto conversion failed: %v", err),
            OriginalJSON: string(jsonBytes),
            MessageType:  o.config.ProtoMessageType,
            Err:          err,
        }
    }

    // 4. Marshal to proto wire format
    protoBytes, err := proto.Marshal(msg)
    if err != nil {
        return nil, fmt.Errorf("proto marshaling failed: %w", err)
    }

    return protoBytes, nil
}

type ProtoConversionError struct {
    Message      string
    OriginalJSON string
    MessageType  string
    Err          error
}

func (e *ProtoConversionError) Error() string {
    return e.Message
}
```

### User Workflow

**Step 1: Create Proto Schema**
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

**Step 2: Generate Descriptor**
```bash
protoc --descriptor_set_out=log_event.descriptor \
       --include_imports \
       log_event.proto
```

**Step 3: Configure Filebeat**
```yaml
filebeat.inputs:
  - type: filestream
    paths:
      - /var/log/*.log
    # Use processors to shape data to match proto schema
    processors:
      - decode_json_fields:
          fields: ["message"]
          target: ""
      - timestamp:
          field: timestamp
          layouts:
            - '2006-01-02T15:04:05.000Z'
      - rename:
          fields:
            - from: "log.level"
              to: "level"

output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.logs"

  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"

  # Proto configuration
  record_type: "proto"
  proto_descriptor_file: "/etc/filebeat/log_event.descriptor"
  proto_message_type: "logs.LogEvent"

  workers: 8
  batch_size: 4096
```

## Configuration

### Complete Schema

```go
type Config struct {
    // Connection settings
    ZeroBusURI   string `config:"zerobus_uri" validate:"required"`
    WorkspaceURL string `config:"workspace_url" validate:"required"`
    TableName    string `config:"table_name" validate:"required"`

    // Authentication (OAuth only)
    OAuth OAuthConfig `config:"oauth" validate:"required"`

    // Record format
    RecordType          string `config:"record_type"`           // "json" or "proto", default "json"
    ProtoDescriptorFile string `config:"proto_descriptor_file"` // Required if record_type="proto"
    ProtoMessageType    string `config:"proto_message_type"`    // Required if record_type="proto"

    // SDK Options (full exposure with defaults)
    SDKOptions SDKOptions `config:"sdk_options"`

    // Concurrency settings
    Workers int `config:"workers"` // Number of goroutines, default 4

    // Beats integration
    Timeout   time.Duration     `config:"timeout"`
    Retry     retryConfig       `config:"retry"`
    TLS       *tlscommon.Config `config:"ssl"`
    Codec     codec.Config      `config:"codec"`
    BatchSize int               `config:"batch_size"`
    Queue     config.Namespace  `config:"queue"`
}

type OAuthConfig struct {
    ClientID     string `config:"client_id" validate:"required"`
    ClientSecret string `config:"client_secret" validate:"required"`
}

type SDKOptions struct {
    MaxInflightRequests      uint64 `config:"max_inflight_requests"`       // Default: 1,000,000
    Recovery                 bool   `config:"recovery"`                     // Default: true
    RecoveryTimeoutMs        uint64 `config:"recovery_timeout_ms"`          // Default: 15,000
    RecoveryBackoffMs        uint64 `config:"recovery_backoff_ms"`          // Default: 2,000
    RecoveryRetries          uint32 `config:"recovery_retries"`             // Default: 4
    FlushTimeoutMs           uint64 `config:"flush_timeout_ms"`             // Default: 300,000
    ServerLackOfAckTimeoutMs uint64 `config:"server_lack_of_ack_timeout_ms"` // Default: 60,000
}
```

### Defaults

```go
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
```

### Example Configurations

**Minimal (JSON Mode):**
```yaml
output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.table"
  oauth:
    client_id: "your-client-id"
    client_secret: "your-client-secret"
```

**High Throughput (JSON Mode):**
```yaml
output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.table"
  oauth:
    client_id: "your-client-id"
    client_secret: "your-client-secret"

  workers: 16
  batch_size: 8192

  sdk_options:
    max_inflight_requests: 500000
    recovery_retries: 10
    flush_timeout_ms: 600000
```

**Proto Mode:**
```yaml
output.zerobus:
  zerobus_uri: "12345.zerobus.region.cloud.databricks.com"
  workspace_url: "https://workspace.cloud.databricks.com"
  table_name: "catalog.schema.structured_logs"

  oauth:
    client_id: "your-client-id"
    client_secret: "your-client-secret"

  record_type: "proto"
  proto_descriptor_file: "/etc/filebeat/schemas/log_event.descriptor"
  proto_message_type: "LogEvent"

  workers: 8
  batch_size: 4096
```

## Error Handling

### Error Classification Matrix

| Error Type | SDK Behavior | Output Behavior | User Impact |
|------------|-------------|-----------------|-------------|
| Network failure | Auto-retry with backoff | Log warning, SDK recovers | Transparent recovery |
| Connection timeout | Auto-retry with recovery | Log warning, SDK recovers | Transparent recovery |
| Temporary server error | Auto-retry | Log warning, SDK recovers | Transparent recovery |
| Auth failure (401/403) | No retry (non-retryable) | PermanentError → Beats retry → DLQ | Fix credentials |
| Invalid table name | No retry | PermanentError → Beats retry → DLQ | Fix config |
| Schema mismatch | No retry | PermanentError → Beats retry → DLQ | Fix schema |
| JSON encoding error | N/A | PermanentError → Beats retry → DLQ | Fix codec config |
| Proto conversion error | N/A | PermanentError → Beats retry → DLQ | Fix schema/data |
| Stream closed unexpectedly | Auto-recovery (if enabled) | Log error, SDK recovers | Transparent or retry |
| Acknowledgment timeout | Configurable | PermanentError → Beats retry → DLQ | Check timeouts |

### Logging Strategy

```go
// SDK auto-recoverable errors (INFO/WARN)
log.Warnf("Stream connection lost, SDK recovering (attempt %d/%d): %v",
    attempt, maxRetries, err)

// Non-retryable errors (ERROR with actionable guidance)
log.Errorf("Authentication failed - check OAuth credentials in config: %v", err)
log.Errorf("Table '%s' not found - verify table_name setting: %v", tableName, err)

// Per-event errors (ERROR with event context)
log.Errorf("Failed to encode event %d: %v\n  Event data: %s",
    eventID, err, truncateJSON(eventData, 500))

// Proto conversion errors (ERROR with schema details)
log.Errorf("Proto conversion failed for event %d: %v\n"+
    "  Expected schema: %s\n"+
    "  Received JSON: %s\n"+
    "  Hint: Check field names and types match proto definition",
    eventID, err, messageType, truncateJSON(jsonData, 500))
```

## Migration Strategy

### From zerobushttp to zerobus

**Option 1: Direct Cutover**
```yaml
# Change output type, keep same config structure
output.zerobus:  # Was: output.zerobushttp
  zerobus_uri: "..."
  workspace_url: "..."
  table_name: "..."
  oauth:
    client_id: "..."
    client_secret: "..."
  workers: 4
```

**Option 2: Parallel Deployment (Recommended for Validation)**
```yaml
# Run both outputs temporarily
output.zerobushttp:
  enabled: true
  table_name: "catalog.schema.table_old"
  # ... existing config ...

output.zerobus:
  enabled: true
  table_name: "catalog.schema.table_new"
  # ... new config ...
```

### Backward Compatibility

- Keep `zerobushttp` package for existing users
- Document `zerobus` as recommended for new deployments
- Deprecation timeline: TBD after SDK proves stable in production

## Build & Dependencies

### CGO Requirement

The Zerobus Go SDK uses CGO to wrap a Rust library. Building Filebeat with the SDK requires:

1. **Rust toolchain** (1.75+): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. **C compiler**: gcc (Linux), clang (macOS), MinGW-w64 (Windows)
3. **CGO enabled**: `CGO_ENABLED=1` (default for most platforms)

### Build Process

```bash
# One-time setup: Build Rust FFI library
cd libbeat/outputs/zerobus/vendor/zerobus-go-sdk/sdk
go generate  # Builds Rust library (~2-5 minutes first time)
cd ../../../../..

# Regular build (includes statically-linked Rust library)
cd filebeat
CGO_ENABLED=1 go build

# Result: Self-contained binary with no runtime dependencies
```

### Dependency Management

```go
// go.mod additions
require (
    github.com/databricks/zerobus-go-sdk v0.x.x
    google.golang.org/protobuf v1.31.0
)
```

## Testing Strategy

### Unit Tests

- Mock SDK interfaces (`ZerobusSdk`, `ZerobusStream`, `RecordAck`)
- Test error handling paths (retryable vs non-retryable)
- Test proto conversion with various schemas and error cases
- Test worker pool behavior under load
- Test graceful shutdown and resource cleanup

### Integration Tests

- Real Zerobus connection (staging environment)
- JSON mode end-to-end ingestion
- Proto mode with sample schemas
- Recovery scenarios (network interruption simulation)
- Performance benchmarks (throughput, latency, memory)
- Multi-worker concurrency testing

### Performance Benchmarks

| Metric | zerobushttp (baseline) | zerobus (target) |
|--------|------------------------|------------------|
| Throughput | ~10K events/sec | ~15-20K events/sec |
| Latency (p50) | ~50-100ms | ~30-70ms |
| Latency (p99) | ~200-500ms | ~100-300ms |
| Memory overhead | Low (stateless) | Moderate (configurable) |
| Connection overhead | High (per-request) | Low (persistent) |
| Recovery time | Manual restart | ~5-10s (automatic) |

## Documentation Deliverables

1. **User Guide**: `libbeat/outputs/zerobus/docs/zerobus.asciidoc`
   - Configuration reference with all options
   - JSON vs Proto mode comparison
   - Performance tuning guide
   - Troubleshooting common issues
   - Migration guide from zerobushttp

2. **Proto Workflow Guide**: Separate document for proto setup
   - Proto schema design best practices
   - Descriptor generation commands
   - Filebeat processor configuration for schema alignment
   - Troubleshooting conversion errors
   - Example schemas for common log formats

3. **Example Configurations**: In documentation and examples directory
   - Basic JSON ingestion
   - High-throughput tuning
   - Proto with complex schemas
   - Multi-table setup
   - Testing/staging vs production configs

## Implementation Plan

### Phase 1: JSON Mode (Priority 1)

1. Create `libbeat/outputs/zerobus` package structure
2. Implement config.go with validation
3. Implement zerobus.go (SDK initialization, stream management)
4. Implement batch publishing with goroutine pool
5. Add error handling and logging
6. Write unit tests with mocked SDK
7. Integration testing in staging environment
8. Documentation (basic usage, migration guide)

### Phase 2: Proto Support (Priority 2)

1. Extend config for proto options
2. Implement descriptor loading and parsing
3. Implement JSON→Proto conversion
4. Add proto-specific error handling
5. Write proto conversion unit tests
6. Integration testing with sample schemas
7. Documentation (proto workflow guide, examples)

### Phase 3: Production Readiness

1. Performance benchmarking and tuning
2. Load testing with realistic workloads
3. Recovery scenario testing
4. Complete documentation review
5. Migration testing from zerobushttp
6. Deprecation plan for zerobushttp (if applicable)

## Open Questions

1. **PAT token support**: Should we maintain backward compatibility with PAT tokens, or OAuth-only?
   - **Decision**: OAuth-only for new output (PAT deprecated in zerobushttp already)

2. **Stream pool vs single stream**: Revisit if single stream becomes bottleneck
   - **Decision**: Start with single stream, can add pool if needed

3. **Metrics/telemetry**: Should we expose SDK-level metrics (inflight count, retry attempts)?
   - **Decision**: Use Beats' existing observer metrics initially, extend if needed

4. **Vendoring**: Vendor SDK or use go.mod?
   - **Decision**: TBD based on Beats project conventions

## Success Criteria

1. **Functional**: All JSON ingestion scenarios working with SDK
2. **Performance**: ≥50% throughput improvement over zerobushttp
3. **Reliability**: Automatic recovery from transient failures
4. **Proto**: Working end-to-end proto ingestion with validation
5. **Migration**: Smooth migration path from zerobushttp
6. **Documentation**: Complete user guide and examples

## References

- [Zerobus Go SDK GitHub](https://github.com/databricks/zerobus-sdk-go)
- [Zerobus Go SDK README](https://github.com/databricks/zerobus-sdk-go/blob/main/README.md)
- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Beats Developer Guide](https://www.elastic.co/guide/en/beats/devguide/current/index.html)
