# AGENTS.md

This file provides guidance to coding agents working in this repository.

## Fork Purpose

This repository is a modified fork of `geyserbench` used to benchmark:

- Modified Yellowstone Geyser implementations
- Modified Agave RPC / pipeline instrumentation

Primary goal: find and validate optimizations and new features in those forks with repeatable, side-by-side latency and detection metrics.

## Project Overview

GeyserBench is a Rust CLI benchmarking tool for Solana gRPC-compatible data feeds. It benchmarks multiple providers concurrently (`yellowstone`, `arpc`, `thor`, `shredstream`, `shreder`, `jetstream`, `influxdb`) and tracks:

- first-detection share
- latency percentiles (P50/P95/P99)
- valid transaction counts
- backfill events

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

- `main.rs` - CLI parsing, tokio runtime setup, provider orchestration, backend streaming
- `config.rs` - TOML config parsing (`Config`, `Endpoint`, `EndpointKind`)
- `analysis.rs` - result aggregation and CLI table rendering
- `backend.rs` - WebSocket streaming to backend
- `utils.rs` - `Comparator` (DashMap-backed aggregation), `ProgressTracker`, helpers
- `proto.rs` - protobuf module exports (generated at build time)
- `providers/` - provider implementations

### Provider System

The `GeyserProvider` trait (in `src/providers/mod.rs`) defines the interface for all data providers:

```rust
pub trait GeyserProvider: Send + Sync {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
}
```

`create_provider()` instantiates providers by `EndpointKind`. Each provider:

- runs concurrently via `tokio::task::spawn`
- receives shared state in `ProviderContext` (comparator, counters, shutdown channel, optional signature queue)
- streams observations through `TransactionAccumulator` before global comparison

### Concurrency Model

- All providers run concurrently and share comparison state via `Arc<Comparator>`
- `broadcast::channel` coordinates graceful shutdown
- `AtomicBool`/`AtomicUsize` are used for lock-free shared counters and target-based stop
- Signature forwarding to backend uses a dedicated thread with `ArrayQueue`

### Protocol Buffers

`build.rs` compiles 8 proto files at build time using `tonic-prost-build`:

- `arpc.proto`
- `events.proto`
- `publisher.proto`
- `shredstream.proto`
- `shreder.proto`
- `jetstream.proto`
- `geyser.proto`
- `solana-storage.proto`

Proto files live in `proto/`, and rebuilds trigger automatically when they change.

## Configuration

Config is TOML-based (`config.toml`) and is auto-generated on first run if missing.

```toml
[config]
transactions = 1000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"  # processed | confirmed | finalized

[[endpoint]]
name = "Provider Name"
url = "https://endpoint.url:port"
kind = "yellowstone"      # yellowstone | arpc | thor | shredstream | shreder | jetstream | influxdb
x_token = "optional-auth-token"
```

InfluxDB endpoints additionally require `influx_org`, `influx_bucket`, and `influx_stage`.

## InfluxDB Provider Notes (Agave Pipeline Stages)

The InfluxDB provider is used to compare stream arrival against instrumented Agave pipeline stages (`fast_geyser_latency` measurement), e.g.:

- `entry_available`
- `replay_entries_received`
- `verification_complete`
- `accounts_locked`
- `execution_complete`
- `geyser_notify`

Timestamp semantics:

- Geyser/gRPC providers use current receive time.
- InfluxDB uses logged `timestamp_us` from Influx.
- Influx elapsed time is computed as `max(influx_timestamp - start_wallclock_secs, 0)`.

Important metric behavior:

- Comparison output stores non-negative delays relative to the earliest observation for each signature.
- Current summary math does not emit negative latency deltas.

## Fork-Specific Guidance

- Keep benchmarking behavior deterministic and comparable across modified and baseline endpoints.
- Prefer changes that make optimization validation easier: clear metric definitions, explicit stage naming, and consistent timestamp handling.
- Treat backfill separately from live-path latency when evaluating modified Agave/Yellowstone behavior.
- When changing provider logic, preserve shared comparator semantics unless the benchmark definition is intentionally changing.

## Key Dependencies

- `tokio` - async runtime
- `tonic`/`prost` - gRPC client and protobuf support
- `dashmap` - concurrent hashmap for aggregation
- `crossbeam-queue` - lock-free queue for signature forwarding
- `comfy-table` - CLI table rendering
- `tracing` - structured logging (`RUST_LOG`)
