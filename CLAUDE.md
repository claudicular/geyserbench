# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GeyserBench is a Rust CLI benchmarking tool for Solana gRPC-compatible data feeds. It benchmarks multiple providers simultaneously and tracks first-detection share, latency percentiles (P50/P95/P99), valid transaction counts, and backfill events.

Supported endpoint kinds:

- `yellowstone` (transaction notify mode)
- `yellowstone_tx_accounts` (transaction-accounts notify mode)
- `arpc`
- `thor`
- `shredstream`
- `shreder`
- `jetstream`
- `influxdb`

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
- **analysis.rs** - results aggregation and CLI table rendering (includes per-endpoint mode column)
- **backend.rs** - WebSocket streaming to SolStack backend
- **utils.rs** - `Comparator` (thread-safe results aggregation via DashMap), `ProgressTracker`
- **proto.rs** - protobuf module exports (generated at build time)
- **providers/** - provider implementations

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
- Streams transaction observations through a `TransactionAccumulator`

### Concurrency Model

- All providers run concurrently, sharing state via `Arc<Comparator>` (DashMap-backed)
- `broadcast::channel` coordinates graceful shutdown across tasks
- `AtomicBool`/`AtomicUsize` provide lock-free shared counters
- Signature forwarding to backend uses a dedicated thread with `ArrayQueue`

### Protocol Buffers

The `build.rs` script compiles 8 proto files at build time using `tonic-prost-build`:

- `arpc.proto`
- `events.proto`
- `publisher.proto`
- `shredstream.proto`
- `shreder.proto`
- `jetstream.proto`
- `geyser.proto`
- `solana-storage.proto`

`geyser.proto` in this repo includes `transaction_accounts` request/update types used by `yellowstone_tx_accounts` mode.

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
kind = "yellowstone"                             # yellowstone | yellowstone_tx_accounts | arpc | thor | shredstream | shreder | jetstream | influxdb
x_token = "optional-auth-token"
```

## Yellowstone Transaction Accounts Mode

`kind = "yellowstone_tx_accounts"` subscribes to Yellowstone `transaction_accounts` updates instead of regular transaction updates.

Provider behavior:

- Uses `SubscribeRequestFilterTransactionAccounts`
- Filters by `config.account` as the `owner` filter (fork-specific convention)
- Uses `include_all_accounts = true` and `readonly_mints_only = false`
- Extracts signature from `SubscribeUpdateTransactionAccounts.signature`
- Records first-seen timing and latency metrics using the same comparator pipeline as other providers

This keeps existing `kind = "yellowstone"` transaction-notify benchmarking unchanged.

Fork note: for `kind = "yellowstone_tx_accounts"`, `config.account` should be a program owner pubkey (not a concrete account pubkey).

## Metrics and Reporting

- Summary output includes per-endpoint **Mode** (`EndpointKind::as_str()`)
- P50/P95/P99, first-share, first detections, and backfill counts are reported per endpoint/mode row
- Fastest endpoint selection still uses latency ordering on the per-endpoint summaries

## InfluxDB Provider for Latency Analysis

The InfluxDB provider benchmarks geyser streams against specific instrumentation points in the Agave RPC pipeline. Unlike gRPC providers that use receive-time timestamps, InfluxDB uses the logged `timestamp_us` value.

### Configuration

```toml
[[endpoint]]
name = "Agave Execution"
url = "http://localhost:8086"
kind = "influxdb"
x_token = "your-influxdb-token"
influx_org = "my-org"
influx_bucket = "solana-metrics"
influx_stage = "execution_complete"
```

### Timestamp Semantics

| Provider Type | `wallclock_secs` | `elapsed_since_start` |
|--------------|------------------|----------------------|
| Geyser/gRPC | Current time when message received | `Instant::now() - start_instant` |
| InfluxDB | InfluxDB logged timestamp | `max(influx_timestamp - start_wallclock_secs, 0)` |

Current comparison summary math emits non-negative delays relative to the first observed endpoint per signature.

## Validator + Plugin Compatibility Notes

For transaction-accounts benchmarking against modified Agave/Yellowstone forks:

- `--enable-transaction-accounts-notify` must be enabled on validator (default is OFF)
- Callback source path is `runtime/src/bank.rs` (`notify_transaction_accounts_to_plugins`)
- Plugin callback shape is `ReplicaTransactionAccountsInfoVersions::V0_0_1`
- Rebuild plugin `.so` from the exact same Agave commit/toolchain as validator to avoid ABI mismatch and potential segfaults

## Key Dependencies

- **tokio** - Async runtime
- **tonic/prost** - gRPC client and Protocol Buffers
- **dashmap** - Concurrent hashmap for results aggregation
- **crossbeam-queue** - Lock-free queue for signature forwarding
- **comfy-table** - CLI table rendering
- **tracing** - Structured logging (configure via `RUST_LOG`)
