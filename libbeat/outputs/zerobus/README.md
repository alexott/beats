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

**Important:** The Zerobus API currently requires `batch_size: 1` for JSON mode. This is an API limitation. To achieve high throughput, increase the `workers` setting instead (e.g., `workers: 16`).

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
