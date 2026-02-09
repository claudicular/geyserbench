# ACCOUNTS.md

This document covers transaction-accounts geyser benchmarking setup for this fork of `geyserbench`.

## Goal

Benchmark Yellowstone `transaction_accounts` stream latency and first-seen behavior against other endpoints, while keeping standard transaction-notify benchmarking available.

## Endpoint Kind

Use `kind = "yellowstone_tx_accounts"` for transaction-accounts mode.

```toml
[config]
transactions = 1000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"

[[endpoint]]
name = "Yellowstone tx-accounts"
url = "http://127.0.0.1:10000"
kind = "yellowstone_tx_accounts"
x_token = "optional-auth-token"
```

The provider subscribes with a `transaction_accounts` filter using `config.account` as the **owner** filter.
In this fork, set `config.account` to a program owner pubkey (not a concrete account pubkey).

## Runtime Requirements (Agave)

Transaction-accounts callbacks are emitted from `runtime/src/bank.rs` via `notify_transaction_accounts_to_plugins`.

Enable this gate on validator:

```bash
--enable-transaction-accounts-notify
```

Default is OFF.

## Interface Shape

Callback interface source:

- `geyser-plugin-interface/src/geyser_plugin_interface.rs`
- `ReplicaTransactionAccountsInfoVersions::V0_0_1`

Relevant fields:

- transaction: `signature`, `slot`, `index`, `accounts[]`
- account entries (`ReplicaAccountInfoV3`): `pubkey`, `lamports`, `owner`, `executable`, `rent_epoch`, `data`, `write_version`, `txn`

## ABI Safety

Plugin `.so` must be rebuilt from the same Agave commit and toolchain as the running validator.

Mismatched builds can cause callback ABI mismatch and segfaults.

## Metrics

`geyserbench` reports:

- first-seen/first-share
- p50/p95/p99 latency
- valid transaction count
- backfill count

Output is mode-aware and includes endpoint mode (`yellowstone` vs `yellowstone_tx_accounts`).
