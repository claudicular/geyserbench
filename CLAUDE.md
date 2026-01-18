# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GeyserBench is a Rust CLI benchmarking tool for Solana gRPC-compatible data feeds. It benchmarks multiple providers simultaneously (Yellowstone, aRPC, Thor, Shredstream, Shreder, Jetstream) and tracks metrics like first-detection share, latency percentiles (P50/P95/P99), valid transaction counts, and backfill events.

## Build Commands

```bash
# Development build
cargo build

# Release build
cargo build --release
# Output: target/release/geyserbench

# Run the binary
./target/release/geyserbench                     # Uses config.toml, streams to backend
./target/release/geyserbench --config path.toml  # Custom config path
./target/release/geyserbench --private           # Disable backend streaming
```

## Architecture

### Module Structure

- **main.rs** - CLI argument parsing, tokio runtime setup, orchestrates providers and backend streaming
- **config.rs** - TOML config parsing (`Config`, `Endpoint`, `EndpointKind`)
- **analysis.rs** - Results aggregation and CLI table rendering
- **backend.rs** - WebSocket streaming to SolStack backend
- **utils.rs** - `Comparator` (thread-safe results aggregation via DashMap), `ProgressTracker`
- **proto.rs** - Protobuf module exports (generated at build time)
- **providers/** - Provider implementations

### Provider System

The `GeyserProvider` trait (in `providers/mod.rs`) defines the interface for all data providers:

```rust
pub trait GeyserProvider: Send + Sync {
    fn process(&self, endpoint: Endpoint, config: Config, context: ProviderContext)
        -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
}
```

Factory function `create_provider()` instantiates providers by `EndpointKind`. Each provider:
- Runs concurrently via `tokio::task::spawn`
- Receives a `ProviderContext` with shared state (comparator, counters, shutdown channel)
- Streams transaction observations to `TransactionAccumulator`

Supported providers: `yellowstone`, `arpc`, `thor`, `shredstream`, `shreder`, `jetstream`, `influxdb`

### Concurrency Model

- All providers run concurrently, sharing state via `Arc<Comparator>` (backed by DashMap)
- `broadcast::channel` coordinates graceful shutdown across all tasks
- `AtomicBool`/`AtomicUsize` for lock-free shared counters
- Signature forwarding to backend uses a dedicated thread with `ArrayQueue`

### Protocol Buffers

The `build.rs` script compiles 8 proto files at build time using `tonic-prost-build`:
- arpc.proto, geyser.proto, jetstream.proto, shredstream.proto, shreder.proto, events.proto, publisher.proto, solana-storage.proto

Proto files are in the `proto/` directory. Rebuild triggers automatically when proto files change.

## Configuration

Config is TOML-based (`config.toml`). Auto-generated on first run:

```toml
[config]
transactions = 1000                              # Number of signatures to evaluate
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"                         # processed | confirmed | finalized

[[endpoint]]
name = "Provider Name"
url = "https://endpoint.url:port"
kind = "yellowstone"                             # yellowstone | arpc | thor | shredstream | shreder | jetstream
x_token = "optional-auth-token"
```

## InfluxDB Provider for Latency Analysis

The InfluxDB provider enables benchmarking geyser streams against specific instrumentation points in the Agave RPC pipeline. Unlike other providers that use current time when data is received, the InfluxDB provider uses the **logged timestamp** from InfluxDB as the observation time.

### Configuration

```toml
[[endpoint]]
name = "Agave Execution"
url = "http://localhost:8086"          # InfluxDB URL
kind = "influxdb"
x_token = "your-influxdb-token"        # InfluxDB API token
influx_org = "my-org"                  # InfluxDB organization
influx_bucket = "solana-metrics"       # InfluxDB bucket
influx_stage = "execution_complete"    # Stage to query
```

### Supported Stages

The `fast_geyser_latency` measurement tracks transaction signatures at various Agave pipeline stages:
- `entry_available` - Entry is available for processing
- `replay_entries_received` - Replay entries received
- `verification_complete` - Transaction verification complete
- `accounts_locked` - Accounts locked for execution
- `execution_complete` - Transaction execution complete
- `geyser_notify` - Geyser notification sent

### Timestamp Semantics

| Provider Type | `wallclock_secs` | `elapsed_since_start` |
|--------------|------------------|----------------------|
| Geyser/gRPC | Current time when message received | `Instant::now() - start_instant` |
| InfluxDB | InfluxDB logged timestamp | `influx_timestamp - start_wallclock_secs` |

When comparing InfluxDB vs Geyser providers:
- A negative delta means the pipeline stage occurred before Geyser received the notification (expected)
- The magnitude shows how much latency exists between the pipeline stage and Geyser notification

## Key Dependencies

- **tokio** - Async runtime
- **tonic/prost** - gRPC client and Protocol Buffers
- **dashmap** - Concurrent hashmap for results aggregation
- **crossbeam-queue** - Lock-free queue for signature forwarding
- **comfy-table** - CLI table rendering
- **tracing** - Structured logging (configure via `RUST_LOG` env var)
