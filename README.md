# GeyserBench

GeyserBench benchmarks the speed and reliability of Solana gRPC-compatible data feeds so you can compare providers with consistent metrics.

## Highlights

- Benchmark multiple feeds at once (Yellowstone, aRPC, Thor, Shredstream, Raiden Pulse, Jetstream, and custom gRPC endpoints)
- Track first-detection share, latency percentiles (P50/P95/P99), peer coverage, valid transaction counts, and backfill events
- Stream results to the SolStack backend for shareable reports, or keep runs local with a single flag
- Generate a ready-to-edit TOML config on first launch; supply auth tokens and endpoints without code changes

## Installation

### Prebuilt binaries
- Download the latest release from the [GitHub releases page](https://github.com/solstackapp/geyserbench/releases) and place the binary on your `PATH`.

### Build from source
```bash
cargo build --release
```
The compiled binary is written to `target/release/geyserbench`.

## Quick Start

1. Run the binary once to scaffold `config.toml` in the current directory:
   ```bash
   ./target/release/geyserbench
   ```
2. Edit `config.toml` with the accounts, endpoints, and tokens you want to test.
3. Run the benchmark. Use `--config <PATH>` to point at another file or `--private` to disable backend streaming:
   ```bash
   ./target/release/geyserbench --private
   ```

During a run, GeyserBench prints progress updates followed by a side-by-side comparison table. When streaming is enabled the tool also returns a shareable link once the backend finalizes the report.

## Example Output

![CLI output showing endpoint win rates and latency percentiles](./assets/cli_screenshot.png)

## Configuration Reference

`geyserbench` reads a single TOML file that defines the run parameters and endpoints:

```toml
[config]
transactions = 1000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"  # processed | confirmed | finalized

[[endpoint]]
name = "Jito Shredstream"
url = "http://localhost:10000"
kind = "shredstream"

[[endpoint]]
name = "Corvus aRPC"
url = "https://fra.corvus-labs.io:20202"
kind = "arpc"

[[endpoint]]
name = "Corvus gRPC"
url = "https://fra.corvus-labs.io:10101"
x_token = "optional-auth-token"
kind = "yellowstone"

[[endpoint]]
name = "Raiden Pulse FRA"
url = "http://fra.pulse.raiden.wtf:16000"
kind = "raiden_pulse"

[[endpoint]]
name = "Local shreds (shmem)"
url = "/dev/shm/shredstream.ring"   # the proxy's SHMEM_RING_PATH
kind = "shredstream_shmem"
shmem_core = 12                     # optional, Linux only
```

- `config.transactions` sets how many signatures to evaluate (backend streaming automatically disables itself for extremely large runs).
- `config.account` is the pubkey monitored for transactions during the benchmark.
- `config.commitment` accepts `processed`, `confirmed`, or `finalized`.
- Repeat `[[endpoint]]` blocks for each feed. Supported `kind` values: `yellowstone`, `yellowstone_tx_accounts`, `arpc`, `thor`, `shredstream`, `shredstream_shmem`, `shreder`, `raiden_pulse`, `jetstream`, and `influxdb`. `x_token` is optional.
- `shredstream` (proxy gRPC `SubscribeEntries`) and `shredstream_shmem` (the proxy's shared-memory ring, the same feed the arb bot reads via `SHREDSTREAM_SHMEM_PATH`) share one decoder ported from the arb bot: legacy, v0, and SIMD-0385 v1 transactions. A micro-batch's transactions are timestamped once, right after the batch decodes; `config.account` must be a static account key. Decode failures are logged and counted (`decode_errors`) instead of silently dropping batches.
- For `shredstream_shmem`, `url` is the ring's file path. A dedicated thread busy-polls the ring for the whole run, so it keeps one CPU fully busy; set `shmem_core` to pin it away from validator and bot cores. The reader maps the ring read-only and starts at the current write position, so it can run next to the production bot.
- For `raiden_pulse`, use the exact URL and port issued by the Raiden dashboard (e.g. `http://fra.pulse.raiden.wtf:16000`). The client speaks the Pulse V2 `raiden_binary` proto; the retired `shreder_binary` service now returns UNIMPLEMENTED. Access is by source-IP whitelist (a non-whitelisted host gets PERMISSION_DENIED right after connecting), so `x_token` is unused. Pulse applies `config.account` as an `account_required` server-side filter over static and lookup-table-resolved accounts, and reports pre-execution transaction detection, so `config.commitment` does not apply. The shred providers match static keys only, so Pulse can see signatures they don't. The end-of-run log counts v1 transactions Pulse delivered (`v1_transactions`).
- Peer coverage uses the union of live signatures observed by any configured endpoint. `Seen` and `Coverage %` show how much of that union each endpoint observed, `Unique` counts signatures seen only by that endpoint, and `Missed` counts signatures seen by at least one peer but not that endpoint. Backfill observations are excluded.
- Prefer `config.duration_secs` runs for representative coverage comparisons. Transaction-target runs stop after the configured number of complete matches and therefore favor signatures shared by every endpoint.
- When `config.rpc_url` and `[validator_map]` are configured, the final report repeats peer coverage for `in`, `out`, and `unknown` leader regions. See [the validator-map input contract](./docs/validator-map.md).

## CLI Options

- `--config <PATH>` &mdash; load configuration from a different TOML file (defaults to `config.toml`).
- `--private` &mdash; keep results local by skipping the streaming backend, even when the run qualifies for sharing.
- `-h`, `--help` &mdash; show usage information.

Streaming is enabled by default for standard-sized runs and publishes to `https://runs.solstack.app`. You can always opt out with `--private` or by configuring the backend section to point at your own infrastructure.
