# Validator map input contract (PersistedRttMap v1)

GeyserBench's region classification consumes an externally produced validator
RTT map. The map is a **pluggable input contract**, not a dependency on any
specific tool: anything that writes this JSON shape can drive the `[validator_map]`
feature. In practice it is produced by an RTT prober that re-measures each
upcoming leader's current gossip IP every ~60 seconds — location is decided
purely by live RTT because validator identities move hosts while keeping their
pubkey, so any static geo/IP mapping goes stale within weeks.

## Configuration

```toml
[validator_map]
source = "/path/to/validator_rtt_map.json"   # or an http(s) URL
rtt_icmp_threshold_us = 5000                  # optional, default 5000
rtt_quic_threshold_us = 6000                  # optional, default icmp + 1000
refresh_secs = 300                            # optional, reload cadence for long runs
```

`source` may be a filesystem path or an http(s) URL. In duration mode the map
is re-fetched when older than `refresh_secs` (checked on each sink flush); load
failures keep the previous contents.

## JSON shape

```json
{
  "version": 1,
  "written_at_unix": 1753228800,
  "bot_name": "fra",
  "entries": {
    "<validator-identity-pubkey>": {
      "gossip_ip": "1.2.3.4",
      "tpu_quic": "1.2.3.4:8002",
      "measurement": { "Icmp": { "rtt_us": 420 } },
      "measured_at_unix": 1753228800,
      "prev_gossip_ip": null
    }
  }
}
```

`measurement` is one of:

- `{"Icmp": {"rtt_us": <u64>}}`
- `{"QuicTpu": {"rtt_us": <u64>, "shared_relay": <bool>, "offbox": <bool>}}`
- `"Unreachable"`

Unknown fields are ignored, so richer producer formats parse as-is.

## In-region predicate

A leader is classified `in` when:

- ICMP RTT is under `rtt_icmp_threshold_us` (default 5 ms), **or**
- QUIC-TPU RTT is under `rtt_quic_threshold_us` (default ICMP threshold + 1 ms)
  **and** `shared_relay` and `offbox` are both false.

`Unreachable` (or a missing measurement) classifies as `out`. Leaders absent
from the map entirely classify as `unknown`.

An entry may also carry an optional `"in_region": <bool>` field. When present
it **overrides** the local predicate, letting the producer keep the
classification policy in one place.

## Where it shows up

- CLI report: the per-region peer-coverage and latency tables, plus the
  `Region` column of the per-leader table (`in` / `out` / `unknown`).
- InfluxDB sink: the `region` tag on every emitted row (`all` on the `ALL`
  rollup rows).
