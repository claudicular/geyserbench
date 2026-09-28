# AGENTS.md

This file provides guidance to coding agents working in this repository.

## Fork Purpose

This repository is a modified fork of `geyserbench` used to benchmark:

- Modified Yellowstone Geyser implementations
- Modified Agave RPC / pipeline instrumentation

Primary goal: find and validate optimizations and new features in those forks with repeatable, side-by-side latency and detection metrics.

## Project Overview

GeyserBench is a Rust CLI benchmarking tool for Solana gRPC-compatible data feeds. It benchmarks multiple providers concurrently (`yellowstone`, `yellowstone_tx_accounts`, `yellowstone_deshred`, `arpc`, `thor`, `shredstream`, `shredstream_shmem`, `shreder`, `raiden_pulse`, `jetstream`, `influxdb`) and tracks:

- first-detection share
- latency percentiles (P50/P95/P99)
- peer coverage across the union of live signatures
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
- `entry_decode.rs` - v1-aware shredstream micro-batch decoder, ported from arb_bot's `integrations/shredstream/entry_decode.rs`; keep the two in sync
- `shmem_ring.rs` - read-only consumer of the shredstream-proxy shared-memory ring
- `providers/` - provider implementations (`shredstream.rs` holds `EntryObserver`, shared by the gRPC and shmem shred providers)

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

`build.rs` compiles 7 proto files at build time using `tonic-prost-build`:

- `arpc.proto`
- `shredstream.proto`
- `shreder.proto`
- `raiden_binary.proto` (Raiden Pulse V2)
- `jetstream.proto`
- `geyser.proto`
- `solana-storage.proto`

Proto files live in `proto/`, and rebuilds trigger automatically when they change.

`geyser.proto` carries the fork-only `transaction_accounts` request/update on protobuf field **100** (not 12, which upstream uses for `block_footer`), matching the fork plugin branch `add-transaction-accounts-sub-v13` (agave 4.3.0 fork). `kind = "yellowstone_tx_accounts"` therefore requires that plugin; against an older field-12 fork build or an upstream plugin it connects but never receives an update.

`geyser.proto` also carries the `SubscribeDeshred` RPC and its messages, with names and field numbers identical to the fork's proto (`add-transaction-accounts-sub-v13`). `kind = "yellowstone_deshred"` uses it: pre-execution transactions from agave's blockstore insert (`CompletedDataSetsService`, before replay), filtered server-side with `account_include = [config.account]`, `vote = false`, and matched against static plus ALT-resolved accounts (ALTs resolve on the rooted bank). It needs a plugin that serves `SubscribeDeshred`. Wire tests live in `src/providers/yellowstone_deshred.rs`.

`GEYSERBENCH_SIG_CSV=<path>` writes one `endpoint,signature,slot,elapsed_ns,wallclock_secs,wallclock_unix_ns,server_created_unix_ns` row per (endpoint, signature) observation after all providers finish (`utils::write_signature_csv`). It never writes during the run.

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
kind = "yellowstone"      # yellowstone | yellowstone_tx_accounts | yellowstone_deshred | arpc | thor | shredstream | shredstream_shmem | shreder | raiden_pulse | jetstream | influxdb
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
