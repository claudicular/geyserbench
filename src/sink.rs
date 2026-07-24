//! Long-running metrics export: aggregates complete signatures into
//! per-(leader, endpoint) window counters and writes them to InfluxDB v2 as
//! line protocol. Counters are summable across flush windows (compute
//! `first_share = sum(firsts)/sum(contested)` and
//! `mean_delta_us = sum(delta_sum_us)/sum(contested)` at query time);
//! the per-window percentile fields are not summable.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};
use tracing::{debug, info, trace, warn};

use crate::{
    config::InfluxSinkSettings,
    leader::{LeaderResolver, Region, ValidatorMap},
    utils::CompleteObservation,
};

const ALL_LEADERS: &str = "ALL";
const UNKNOWN_LEADER: &str = "UNKNOWN";
const MAX_PENDING_LINES: usize = 10_000;

#[derive(Default)]
struct WindowStats {
    contested: u64,
    firsts: u64,
    delta_sum_us: i64,
    deltas_us: Vec<i64>,
}

struct SinkState {
    settings: InfluxSinkSettings,
    client: Client,
    resolver: Option<Arc<LeaderResolver>>,
    validator_map: Option<Arc<ValidatorMap>>,
    window: HashMap<(Arc<str>, String), WindowStats>,
    pending_lines: Vec<String>,
}

pub fn spawn_influx_sink(
    settings: InfluxSinkSettings,
    resolver: Option<Arc<LeaderResolver>>,
    validator_map: Option<Arc<ValidatorMap>>,
    mut rx: UnboundedReceiver<CompleteObservation>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let flush_secs = settings.flush_interval_secs.max(1);
        let mut state = SinkState {
            settings,
            client: Client::new(),
            resolver,
            validator_map,
            window: HashMap::new(),
            pending_lines: Vec::new(),
        };
        let mut ticker = tokio::time::interval(Duration::from_secs(flush_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            url = %state.settings.url,
            bucket = %state.settings.bucket,
            measurement = %state.settings.measurement,
            flush_interval_secs = flush_secs,
            "InfluxDB sink started"
        );

        loop {
            tokio::select! {
                observation = rx.recv() => {
                    match observation {
                        Some(observation) => state.ingest(observation).await,
                        None => {
                            state.flush().await;
                            info!("InfluxDB sink stopped");
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    state.flush().await;
                    if let Some(map) = state.validator_map.as_ref() {
                        map.maybe_refresh().await;
                    }
                }
            }
        }
    })
}

impl SinkState {
    async fn ingest(&mut self, observation: CompleteObservation) {
        let observations = &observation.observations;
        // Mirror the analysis rules: skip backfilled signatures.
        if observations
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs)
        {
            return;
        }
        let Some((winner_endpoint, winner_tx)) = observations
            .iter()
            .min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            return;
        };
        let winner_endpoint = winner_endpoint.clone();
        let winner_elapsed = winner_tx.elapsed_since_start;

        let slot = winner_tx
            .slot
            .or_else(|| observations.values().find_map(|tx| tx.slot));
        let leader: Arc<str> = match (slot, self.resolver.as_ref()) {
            (Some(slot), Some(resolver)) => resolver
                .resolve(slot)
                .await
                .unwrap_or_else(|| Arc::from(UNKNOWN_LEADER)),
            _ => Arc::from(UNKNOWN_LEADER),
        };

        trace!(
            signature = %observation.signature,
            slot = ?slot,
            leader = %leader,
            winner = %winner_endpoint,
            "Sink ingesting complete signature"
        );

        for (endpoint, tx) in observations {
            let delta_us = tx
                .elapsed_since_start
                .saturating_sub(winner_elapsed)
                .as_micros() as i64;
            let is_first = *endpoint == winner_endpoint;
            for key_leader in [leader.clone(), Arc::from(ALL_LEADERS)] {
                let stats = self
                    .window
                    .entry((key_leader, endpoint.clone()))
                    .or_default();
                stats.contested += 1;
                if is_first {
                    stats.firsts += 1;
                }
                stats.delta_sum_us += delta_us;
                stats.deltas_us.push(delta_us);
            }
        }
    }

    async fn flush(&mut self) {
        if self.window.is_empty() && self.pending_lines.is_empty() {
            return;
        }

        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for ((leader, endpoint), mut stats) in self.window.drain() {
            let region = if leader.as_ref() == ALL_LEADERS {
                "all"
            } else if leader.as_ref() == UNKNOWN_LEADER {
                Region::Unknown.as_str()
            } else {
                self.validator_map
                    .as_ref()
                    .map(|map| map.classify(&leader))
                    .unwrap_or(Region::Unknown)
                    .as_str()
            };
            stats.deltas_us.sort_unstable();
            let p50 = percentile_i64(&stats.deltas_us, 0.5);
            let p90 = percentile_i64(&stats.deltas_us, 0.9);
            let p99 = percentile_i64(&stats.deltas_us, 0.99);
            self.pending_lines.push(format!(
                "{measurement},leader={leader},endpoint={endpoint},region={region} \
contested={contested}i,firsts={firsts}i,delta_sum_us={delta_sum}i,\
delta_p50_us={p50}i,delta_p90_us={p90}i,delta_p99_us={p99}i {timestamp_ns}",
                measurement = escape_measurement(&self.settings.measurement),
                leader = escape_tag_value(&leader),
                endpoint = escape_tag_value(endpoint.as_str()),
                region = region,
                contested = stats.contested,
                firsts = stats.firsts,
                delta_sum = stats.delta_sum_us,
            ));
        }

        if self.pending_lines.len() > MAX_PENDING_LINES {
            let dropped = self.pending_lines.len() - MAX_PENDING_LINES;
            self.pending_lines.drain(..dropped);
            warn!(
                dropped,
                "InfluxDB sink backlog exceeded cap; dropped oldest lines"
            );
        }

        let body = self.pending_lines.join("\n");
        let url = format!("{}/api/v2/write", self.settings.url.trim_end_matches('/'));
        let result = self
            .client
            .post(&url)
            .query(&[
                ("org", self.settings.org.as_str()),
                ("bucket", self.settings.bucket.as_str()),
                ("precision", "ns"),
            ])
            .header("Authorization", format!("Token {}", self.settings.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                debug!(lines = self.pending_lines.len(), "InfluxDB sink flushed");
                self.pending_lines.clear();
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                warn!(%status, body = %body, "InfluxDB sink write failed; will retry next flush");
            }
            Err(err) => {
                warn!(error = %err, "InfluxDB sink write failed; will retry next flush");
            }
        }
    }
}

fn percentile_i64(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn escape_tag_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

fn escape_measurement(value: &str) -> String {
    value.replace(',', "\\,").replace(' ', "\\ ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_values_are_escaped() {
        assert_eq!(escape_tag_value("Provider A,eu=1"), "Provider\\ A\\,eu\\=1");
        assert_eq!(escape_tag_value("plain"), "plain");
    }

    #[test]
    fn percentiles_from_sorted_deltas() {
        assert_eq!(percentile_i64(&[], 0.5), 0);
        assert_eq!(percentile_i64(&[7], 0.99), 7);
        let sorted: Vec<i64> = (0..=100).collect();
        assert_eq!(percentile_i64(&sorted, 0.5), 50);
        assert_eq!(percentile_i64(&sorted, 0.9), 90);
    }
}
