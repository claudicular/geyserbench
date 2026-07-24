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
- **config.rs** - TOML config parsing (`Config`, `Endpoint`, `EndpointKind`, `ValidatorMapSettings`, `InfluxSinkSettings`)
- **analysis.rs** - results aggregation and CLI table rendering (per-endpoint mode column, leader/region breakdown)
- **backend.rs** - WebSocket streaming to SolStack backend
- **leader.rs** - `LeaderResolver` (slot -> leader identity via RPC leader schedule) and `ValidatorMap` (RTT-map-based in-region classification)
- **sink.rs** - InfluxDB v2 line-protocol writer for long-running per-(leader, endpoint) window counters
- **utils.rs** - `Comparator` (thread-safe results aggregation via DashMap; emits `CompleteObservation`s to the sink), `ProgressTracker`
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
rpc_url = "https://api.mainnet-beta.solana.com"  # Optional: enables leader-aware reporting
duration_secs = 3600                             # Optional: duration mode (see below)

[[endpoint]]
name = "Provider Name"
url = "https://endpoint.url:port"
kind = "yellowstone"                             # yellowstone | yellowstone_tx_accounts | arpc | thor | shredstream | shreder | jetstream | influxdb
x_token = "optional-auth-token"

# Optional: validator location input (see docs/validator-map.md)
[validator_map]
source = "/path/to/validator_rtt_map.json"       # path or http(s) URL, PersistedRttMap v1

# Optional: long-running metrics export to InfluxDB v2
[influx_sink]
url = "http://localhost:8086"
org = "my-org"
bucket = "geyserbench"
token = "influx-token"
measurement = "geyserbench_vs_winner"            # optional, default shown
flush_interval_secs = 15                         # optional, default shown
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

## Validator-Aware Benchmarking

Latency between feeds depends heavily on which leader validator produced the
slot (and therefore where in the world the transaction originated). Three
layered, individually optional features make the bench leader/region aware:

### Slot capture + leader resolution (`config.rpc_url`)

- `TransactionData` carries `slot: Option<u64>`. Providers that expose slot on
  the wire populate it: `yellowstone`, `yellowstone_tx_accounts`,
  `shredstream`, `shreder`, `jetstream`, `arpc`. `thor` and `influxdb` record
  `None`.
- With `config.rpc_url` set, `LeaderResolver` (leader.rs) fetches
  `getEpochInfo` + `getLeaderSchedule` once per epoch (cached, arithmetic
  epoch location, 30s retry backoff on RPC failure) and maps each complete
  signature's slot to its leader identity pubkey. A signature's slot is the
  winning endpoint's slot, falling back to any endpoint that reported one.
- The final report gains a **Leader-aware results** section: per-leader rows
  (top 20 by signatures, minimum 5 signatures, gated leader count printed) with
  per-endpoint First% / P50 delta vs the winner.

### Region classification (`[validator_map]`)

- Consumes an externally produced RTT map (PersistedRttMap v1 — schema and
  in-region predicate documented in `docs/validator-map.md`). Leaders classify
  as `in` / `out` / `unknown`; the report adds a per-region summary table and a
  Region column on leader rows.
- Location is an *input contract*, deliberately not derived internally: RTT
  maps are produced by live probing because validator identities move hosts
  while keeping pubkeys, so static geo mappings go stale.

### Duration mode + InfluxDB sink (`config.duration_secs`, `[influx_sink]`)

Short runs cannot cover the validator set (leader rotation is stake-weighted;
per-validator confidence needs hours). For long runs:

- `duration_secs` runs for a fixed wall-clock duration instead of a signature
  target. It disables backend streaming and progress tracking, and Ctrl+C
  finalizes the report early instead of aborting.
- `[influx_sink]` streams per-(leader, endpoint) window counters to InfluxDB v2
  whenever a signature has been observed by **all** endpoints (matching the CLI
  report's contested semantics, "all pairs vs winner"). Backfilled signatures
  are skipped.

Sink schema (`geyserbench_vs_winner` by default):

- tags: `leader` (identity pubkey, plus an `ALL` rollup row), `endpoint`,
  `region` (`in`/`out`/`unknown`, `all` on rollup rows)
- fields (i64): `contested`, `firsts`, `delta_sum_us`, `delta_p50_us`,
  `delta_p90_us`, `delta_p99_us`

Aggregation rule: `contested`/`firsts`/`delta_sum_us` sum exactly across flush
windows — compute `first_share = sum(firsts)/sum(contested)` and
`mean_delta_us = sum(delta_sum_us)/sum(contested)` at query time. The
`delta_pXX_us` percentiles are per-window only; do not sum them. Stake weights
should be fetched fresh from RPC (`getVoteAccounts`) at analysis time and
joined on the `leader` tag, not stored in InfluxDB.

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
