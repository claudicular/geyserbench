# GeyserBench

GeyserBench benchmarks the speed and reliability of Solana gRPC-compatible data feeds so you can compare providers with consistent metrics.

## Highlights

- Benchmark multiple feeds at once (Yellowstone, Yellowstone deshred, aRPC, Thor, Shredstream, Raiden Pulse, Jetstream, and custom gRPC endpoints)
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
name = "Local deshred"
url = "http://127.0.0.1:10000"
kind = "yellowstone_deshred"

[[endpoint]]
name = "Raiden Pulse FRA"
url = "http://fra.pulse.raiden.wtf:16000"
kind = "raiden_pulse"

[[endpoint]]
name = "Local shreds (shmem)"
url = "/dev/shm/shredstream.ring"   # the proxy's SHMEM_RING_PATH
kind = "shredstream_shmem"
shmem_core = 12                     # optional, Linux only

[[endpoint]]
name = "Fast lane (shmem)"
url = "/dev/shm/fastlane.out.ring"  # agave fast lane out_ring_path
kind = "fastlane_ring"
shmem_core = 35                     # optional, Linux only
```

- `config.transactions` sets how many signatures to evaluate (backend streaming automatically disables itself for extremely large runs).
- `config.account` is the pubkey monitored for transactions during the benchmark.
- `config.commitment` accepts `processed`, `confirmed`, or `finalized`.
- Repeat `[[endpoint]]` blocks for each feed. Supported `kind` values: `yellowstone`, `yellowstone_tx_accounts`, `yellowstone_deshred`, `arpc`, `thor`, `shredstream`, `shredstream_shmem`, `fastlane_ring`, `shreder`, `raiden_pulse`, `jetstream`, and `influxdb`. `x_token` is optional.
- `shredstream` (proxy gRPC `SubscribeEntries`) and `shredstream_shmem` (the proxy's shared-memory ring, the same feed the arb bot reads via `SHREDSTREAM_SHMEM_PATH`) share one decoder ported from the arb bot: legacy, v0, and SIMD-0385 v1 transactions. A micro-batch's transactions are timestamped once, right after the batch decodes; `config.account` must be a static account key. Decode failures are logged and counted (`decode_errors`) instead of silently dropping batches.
- `yellowstone_tx_accounts` subscribes to the fork-only grouped `transaction_accounts` stream and uses `config.account` as an owner (program) filter. It requires the Yellowstone fork plugin that serves that stream on protobuf field 100 (`add-transaction-accounts-sub-v13`, agave 4.3.0 fork); upstream plugins and older field-12 fork builds never deliver updates. See [ACCOUNTS.md](./ACCOUNTS.md).
- `yellowstone_deshred` subscribes to Yellowstone's `SubscribeDeshred` RPC: pre-execution transactions that agave emits from `CompletedDataSetsService` right after shreds are inserted into the blockstore, before replay. It needs a validator and Yellowstone plugin that serve `SubscribeDeshred` with deshred notifications on, such as the fork branch `add-transaction-accounts-sub-v13`. A plugin without the RPC returns UNIMPLEMENTED. The provider sends one filter, `account_include = [config.account]` with `vote = false`, and requests no update-parent or slot messages. The plugin matches that filter against static keys plus lookup-table addresses resolved on the rooted bank, so it can see signatures that the static-key shred providers miss. A table created or extended after the current root does not resolve, and those transactions match on static keys only. Deshred is pre-execution, so `config.commitment` does not apply and transactions that later fail are included. The server pings every 10s and the provider answers each ping. The server drops a client that falls behind, and the run then ends with a stream error.
- For `shredstream_shmem`, `url` is the ring's file path. A dedicated thread busy-polls the ring for the whole run, so it keeps one CPU fully busy; set `shmem_core` to pin it away from validator and bot cores. The reader maps the ring read-only and starts at the current write position, so it can run next to the production bot.
- `fastlane_ring` reads the agave fast lane's output ring (agave fork branch `fast-lane`, fast-lane config `out_ring = true`; `url` is `out_ring_path`, default `/dev/shm/fastlane.out.ring`). The fast lane publishes one record per transaction when it has finished executing it, with the transaction's grouped-notification accounts whose owner is in the fast lane's owner filter (SPL Token and Token-2022 by default, plus `out_owners`). A signature is observed when its record has an account owned by `config.account`, the same rule as `yellowstone_tx_accounts` (`transaction_accounts{owner: [account]}`, matching accounts only), so the two providers see the same transactions provided `config.account` is in the fast lane's owner filter; otherwise this provider sees nothing. Like `shredstream_shmem`, a dedicated thread busy-polls the ring for the whole run (pin it with `shmem_core`), reads from the current write position, and stamps each observation after the record passed the seqlock check. A lapped or torn read re-syncs to the write position and is logged; a replaced ring file is reopened. `server_created_unix_ns` is the fast lane's publish time (validator host `CLOCK_REALTIME`).
- For `raiden_pulse`, use the exact URL and port issued by the Raiden dashboard (e.g. `http://fra.pulse.raiden.wtf:16000`). The client speaks the Pulse V2 `raiden_binary` proto; the retired `shreder_binary` service now returns UNIMPLEMENTED. Access is by source-IP whitelist (a non-whitelisted host gets PERMISSION_DENIED right after connecting), so `x_token` is unused. Pulse applies `config.account` as an `account_required` server-side filter over static and lookup-table-resolved accounts, and reports pre-execution transaction detection, so `config.commitment` does not apply. The shred providers match static keys only, so Pulse can see signatures they don't. The end-of-run log counts v1 transactions Pulse delivered (`v1_transactions`).
- Peer coverage uses the union of live signatures observed by any configured endpoint. `Seen` and `Coverage %` show how much of that union each endpoint observed, `Unique` counts signatures seen only by that endpoint, and `Missed` counts signatures seen by at least one peer but not that endpoint. Backfill observations are excluded.
- Prefer `config.duration_secs` runs for representative coverage comparisons. Transaction-target runs stop after the configured number of complete matches and therefore favor signatures shared by every endpoint.
- When `config.rpc_url` and `[validator_map]` are configured, the final report repeats peer coverage for `in`, `out`, and `unknown` leader regions. See [the validator-map input contract](./docs/validator-map.md).

## Per-signature CSV

Set `GEYSERBENCH_SIG_CSV=/path/to/sigs.csv` to write every (endpoint, signature) observation after the run ends, one row each:

```
endpoint,signature,slot,elapsed_ns,wallclock_secs,wallclock_unix_ns,server_created_unix_ns
```

- `elapsed_ns` is the monotonic time since the run started, the value the comparator uses to rank endpoints. It is comparable across endpoints within one run.
- `wallclock_unix_ns` is the receive time in nanoseconds since the Unix epoch. It is read from `SystemTime` immediately before the monotonic `elapsed_ns` stamp. For `influxdb` it is the logged stage timestamp (microsecond resolution).
- `wallclock_secs` is the same value in Unix seconds, printed exactly with 9 decimals.
- `server_created_unix_ns` is the Yellowstone plugin's `created_at` in nanoseconds since the Unix epoch, for `yellowstone`, `yellowstone_tx_accounts` and `yellowstone_deshred` rows, and the fast lane's record publish time for `fastlane_ring` rows. It is empty for other providers. The plugin stamps it with the validator host's wallclock when it builds the message inside the geyser callback (`SubscribeUpdate.created_at`, field 11; `SubscribeUpdateDeshred.created_at`, field 5). When the bench runs on the validator host, `wallclock_unix_ns - server_created_unix_ns` is the plugin's delivery time: filtering, queueing, encoding and gRPC transport. The time before the callback, inside agave, is not included. On a different host the difference also includes clock offset.
- `slot` is empty for providers that do not report one (`thor`, `influxdb`).
- The file holds each endpoint's earliest observation of every signature it saw, including signatures that other endpoints missed. Rows are sorted by signature, then endpoint.
- The file is written once at the end of the run, including after a Ctrl+C. Nothing is written while providers are receiving.

## CLI Options

- `--config <PATH>` &mdash; load configuration from a different TOML file (defaults to `config.toml`).
- `--private` &mdash; keep results local by skipping the streaming backend, even when the run qualifies for sharing.
- `-h`, `--help` &mdash; show usage information.

Streaming is enabled by default for standard-sized runs and publishes to `https://runs.solstack.app`. You can always opt out with `--private` or by configuring the backend section to point at your own infrastructure.
