# Zerobus Output - Quick Debugging Reference

Quick reference guide for debugging Zerobus output issues.

## Enable Debug Logging

```yaml
# filebeat.yml
logging.level: debug
logging.selectors: ["zerobus"]
```

## Common Debug Output

### Normal Operation

```
INFO  [zerobus] Initialized Zerobus output for table: catalog.schema.logs (workers=4, mode=json, batch_size=100, channel_buffer=100)
DEBUG [zerobus] Worker 0 started
DEBUG [zerobus] Worker 1 started
DEBUG [zerobus] Worker 2 started
DEBUG [zerobus] Worker 3 started
DEBUG [zerobus] Encoded and submitted 100 events (0 encoding failures)
DEBUG [zerobus] Worker 0: Event 0 acknowledged at offset 12345
DEBUG [zerobus] Batch complete: 100 acked, 0 encoding failures, 0 worker failures
```

### Graceful Shutdown

```
INFO  [zerobus] Closing Zerobus output
DEBUG [zerobus] Worker 0: Channel closed, exiting
DEBUG [zerobus] Worker 1: Channel closed, exiting
DEBUG [zerobus] Worker 2: Channel closed, exiting
DEBUG [zerobus] Worker 3: Channel closed, exiting
DEBUG [zerobus] All workers stopped
```

### With Errors

```
ERROR [zerobus] Failed to encode event 42: JSON→Proto conversion failed: unknown field "extra"
DEBUG [zerobus] Encoded and submitted 98 events (2 encoding failures)
ERROR [zerobus] Worker 2: Failed to ingest event 5: connection refused
ERROR [zerobus] Worker 3: Failed to await ack for event 10: timeout
DEBUG [zerobus] Batch complete: 96 acked, 2 encoding failures, 2 worker failures
```

## Quick Diagnostics

### Check Configuration

```bash
# Validate configuration syntax
filebeat test config -c filebeat.yml

# Test connection to Zerobus
filebeat test output -c filebeat.yml
```

### Monitor Running Instance

```bash
# Watch debug logs
tail -f /var/log/filebeat/filebeat.log | grep zerobus

# Check Filebeat process
ps aux | grep filebeat

# Monitor memory usage
watch -n 1 'ps aux | grep filebeat | grep -v grep'
```

### Check Goroutine Count

```bash
# Enable HTTP endpoint first
# filebeat.yml:
#   http.enabled: true
#   http.host: localhost
#   http.port: 5066

# Check goroutine count (should be ~workers + 10-20)
curl -s http://localhost:5066/debug/vars | jq '.goroutines'
```

## Common Issues - Quick Fixes

### Issue: High Latency

**Quick Check:**
```yaml
logging.level: debug
logging.selectors: ["zerobus"]
```

Look for: Long gaps between "Encoded and submitted" and "Batch complete"

**Quick Fix:**
```yaml
output.zerobus:
  workers: 16  # Increase from 4
```

---

### Issue: High Memory Usage

**Quick Check:**
```bash
ps aux | grep filebeat
```

**Quick Fix:**
```yaml
output.zerobus:
  batch_size: 100  # Reduce from 1000
  workers: 4       # Reduce from 16
```

---

### Issue: Encoding Failures

**Quick Check:**
```bash
grep "Failed to encode" /var/log/filebeat/filebeat.log
```

**Quick Fix (Proto mode):**
```yaml
processors:
  - drop_fields:
      fields: ["extra_field_causing_error"]
```

---

### Issue: Connection Errors

**Quick Check:**
```bash
# Test DNS
nslookup YOUR_ZEROBUS_URI

# Test connectivity
telnet YOUR_ZEROBUS_URI 443

# Test authentication
filebeat test output -c filebeat.yml
```

**Quick Fix:**
```yaml
output.zerobus:
  zerobus_uri: "correct-uri.zerobus.region.cloud.databricks.com"
  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"
```

---

### Issue: Workers Stalled

**Symptoms:** No log output, batches not completing

**Quick Check:**
```bash
# Check if process is alive
ps aux | grep filebeat

# Check if workers are blocked
curl -s http://localhost:5066/debug/pprof/goroutine?debug=1 | grep zerobus
```

**Quick Fix:**
```yaml
output.zerobus:
  sdk_options:
    server_lack_of_ack_timeout_ms: 120000  # Increase timeout
```

Restart Filebeat after config change.

---

## Performance Tuning Quick Reference

### Maximum Throughput

```yaml
output.zerobus:
  workers: 50
  batch_size: 1000
  sdk_options:
    max_inflight_requests: 1000000
```

### Low Latency

```yaml
output.zerobus:
  workers: 8
  batch_size: 10
  sdk_options:
    flush_timeout_ms: 10000
```

### Low Resource Usage

```yaml
output.zerobus:
  workers: 2
  batch_size: 10
```

## Debug Command Cheatsheet

```bash
# Test config
filebeat test config -c filebeat.yml

# Test output connection
filebeat test output -c filebeat.yml

# Run in foreground with debug
filebeat -e -c filebeat.yml -d "zerobus"

# Watch logs
tail -f /var/log/filebeat/filebeat.log | grep zerobus

# Check goroutines
curl -s http://localhost:5066/debug/vars | jq '.goroutines'

# Memory profile
curl -s http://localhost:5066/debug/pprof/heap > heap.prof
go tool pprof heap.prof

# CPU profile (30 seconds)
curl -s http://localhost:5066/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof cpu.prof

# Check process memory
ps aux | grep filebeat

# Monitor continuously
watch -n 1 'ps aux | grep filebeat | grep -v grep'
```

## Critical Metrics to Watch

| Metric | Normal Range | Action If Outside Range |
|--------|-------------|------------------------|
| Encoding failures | 0-1% of events | Check event structure, proto schema |
| Worker failures | 0-5% of events | Check network, credentials |
| Goroutines | workers + 10-20 | Check for leaks, restart if growing |
| Memory | < 500MB | Reduce batch_size or workers |
| Batch latency | < 10s (batch_size=100) | Increase workers |

## When to Restart Filebeat

Restart if you see:
- ❌ Goroutine count continuously growing
- ❌ Memory usage continuously growing
- ❌ No log output for > 5 minutes
- ❌ Workers stalled (no "Event X acknowledged" messages)
- ❌ All worker failures (100% failure rate)

## Getting More Help

1. **Enable debug logging** and collect logs for 5-10 minutes
2. **Check metrics**: goroutines, memory, batch completion times
3. **Capture profiles** if memory/CPU issues suspected
4. **Document configuration** used when issue occurred
5. Open issue with collected information

## Quick Config Templates

### Development (Verbose Logging)

```yaml
logging.level: debug
logging.selectors: ["zerobus"]

output.zerobus:
  zerobus_uri: "${ZEROBUS_URI}"
  workspace_url: "${WORKSPACE_URL}"
  table_name: "${TABLE_NAME}"
  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"
  workers: 4
  batch_size: 1

output.console:  # Also log to console
  pretty: true
  enabled: true
```

### Production (Optimized)

```yaml
logging.level: info
logging.selectors: ["*"]

output.zerobus:
  zerobus_uri: "${ZEROBUS_URI}"
  workspace_url: "${WORKSPACE_URL}"
  table_name: "${TABLE_NAME}"
  oauth:
    client_id: "${OAUTH_CLIENT_ID}"
    client_secret: "${OAUTH_CLIENT_SECRET}"
  workers: 16
  batch_size: 100
  sdk_options:
    max_inflight_requests: 1000000
    flush_timeout_ms: 300000

monitoring.enabled: true
monitoring.elasticsearch:
  hosts: ["${ES_HOST}"]

http.enabled: true
http.host: localhost
http.port: 5066
```

### Proto Mode

```yaml
output.zerobus:
  record_type: "proto"
  proto_descriptor_file: "/etc/filebeat/descriptor.descriptor"
  proto_message_type: "package.MessageType"
  workers: 16
  batch_size: 100
  # ... rest of config ...
```
