use crate::leader::{Region, ValidatorMap};
use crate::utils::{Comparator, TransactionData, percentile};
use comfy_table::{ContentArrangement, Table};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::sync::Arc;

#[cfg(target_os = "windows")]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::ASCII_FULL
}

#[cfg(not(target_os = "windows"))]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::UTF8_FULL
}
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct EndpointDescriptor {
    pub name: String,
    pub mode: String,
}

#[derive(Default)]
pub struct EndpointStats {
    pub total_observations: usize,
    pub first_detections: usize,
    pub delays_ms: Vec<f64>,
    pub backfill_transactions: usize,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointSummary {
    pub name: String,
    pub mode: String,
    pub first_share: f64,
    pub p50_delay_ms: Option<f64>,
    pub p95_delay_ms: Option<f64>,
    pub p99_delay_ms: Option<f64>,
    pub valid_transactions: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
}

pub fn compute_run_summary(
    comparator: &Comparator,
    endpoints: &[EndpointDescriptor],
) -> RunSummary {
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();
    let endpoint_modes: HashMap<String, String> = endpoints
        .iter()
        .map(|endpoint| (endpoint.name.clone(), endpoint.mode.clone()))
        .collect();
    let expected_producers = endpoints.len();
    let mut total_signatures = 0usize;
    let mut backfill_signatures = 0usize;

    for endpoint in endpoints {
        endpoint_stats.insert(endpoint.name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if expected_producers > 0 && sig_data.len() != expected_producers {
            // Skip partial observations to mirror backend results
            continue;
        }

        let is_historical = sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs);

        if is_historical {
            backfill_signatures += 1;
            for endpoint in sig_data.keys() {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.backfill_transactions += 1;
                }
            }
            continue;
        }

        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };

        total_signatures += 1;
        let first_endpoint_name = first_endpoint.clone();

        for (endpoint, tx) in sig_data.iter() {
            if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                stats.total_observations += 1;
                if endpoint == &first_endpoint_name {
                    stats.first_detections += 1;
                    stats.delays_ms.push(0.0);
                } else {
                    let delay_ms = diff_ms(tx, first_tx).max(0.0);
                    stats.delays_ms.push(delay_ms);
                }
            }
        }
    }

    let endpoints: Vec<EndpointSummary> = endpoint_stats
        .into_iter()
        .map(|(endpoint, stats)| {
            let mode = endpoint_modes
                .get(&endpoint)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            build_summary(endpoint, mode, stats, total_signatures)
        })
        .collect();

    let has_data = total_signatures > 0;

    let fastest_endpoint = endpoints
        .iter()
        .filter(|summary| summary.valid_transactions > 0)
        .min_by(|a, b| compare_latency(a, b))
        .map(|summary| summary.name.clone());

    RunSummary {
        endpoints,
        fastest_endpoint,
        has_data,
        total_signatures,
        backfill_signatures,
    }
}

pub fn display_run_summary(summary: &RunSummary) {
    println!("\nFinished test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
    } else {
        let fastest_name_ref = summary.fastest_endpoint.as_deref();
        let mut summary_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
        summary_rows.sort_by(|a, b| compare_latency(a, b));

        for summary in summary_rows {
            if summary.valid_transactions == 0 {
                println!("{}: Not enough data", summary.name);
                continue;
            }

            let raw_win_rate = format_percent(summary.first_share);
            let win_rate = if raw_win_rate == "—" {
                raw_win_rate
            } else {
                format!("{}%", raw_win_rate)
            };
            let is_fastest = fastest_name_ref == Some(summary.name.as_str());

            if is_fastest {
                println!(
                    "{} [{}]: Win rate {}, p50 0.00ms (fastest)",
                    summary.name, summary.mode, win_rate,
                );
            } else {
                let p50_delay = summary
                    .p50_delay_ms
                    .map(|v| format!("{:.2}ms", v))
                    .unwrap_or_else(|| "—".to_string());
                println!(
                    "{} [{}]: Win rate {}, p50 {}",
                    summary.name, summary.mode, win_rate, p50_delay
                );
            }
        }
    }

    println!("\nDetailed test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
        return;
    }

    let mut table_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
    table_rows.sort_by(|a, b| compare_latency(a, b));

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "Endpoint", "Mode", "First %", "P50 ms", "P95 ms", "P99 ms", "Valid Tx", "Firsts",
        "Backfill",
    ]);

    for summary in table_rows {
        table.add_row(vec![
            summary.name.clone(),
            summary.mode.clone(),
            format_percent(summary.first_share),
            format_latency_value(summary.p50_delay_ms),
            format_latency_value(summary.p95_delay_ms),
            format_latency_value(summary.p99_delay_ms),
            summary.valid_transactions.to_string(),
            summary.first_detections.to_string(),
            summary.backfill_transactions.to_string(),
        ]);
    }

    println!("{}", table);
}

pub fn build_metrics_report(summary: &RunSummary) -> Value {
    let mut per_endpoint = Map::new();
    for endpoint in &summary.endpoints {
        let payload = json!({
            "mode": endpoint.mode,
            "first_detection_rate": endpoint.first_share,
            "p50_latency_ms": endpoint.p50_delay_ms,
            "p95_latency_ms": endpoint.p95_delay_ms,
            "p99_latency_ms": endpoint.p99_delay_ms,
            "observations": endpoint.valid_transactions,
            "first_detections": endpoint.first_detections,
            "backfill_transactions": endpoint.backfill_transactions,
        });
        per_endpoint.insert(endpoint.name.clone(), payload);
    }

    json!({
        "total_signatures": summary.total_signatures,
        "backfill_signatures": summary.backfill_signatures,
        "per_endpoint": per_endpoint
    })
}

const MAX_LEADER_ROWS: usize = 20;
const MIN_LEADER_SIGS: usize = 5;

#[derive(Debug, Default, Clone)]
pub struct GroupEndpointStats {
    pub firsts: usize,
    pub delays_ms: Vec<f64>,
}

impl GroupEndpointStats {
    fn first_share(&self, sigs: usize) -> f64 {
        if sigs == 0 {
            return f64::NAN;
        }
        self.firsts as f64 / sigs as f64
    }

    fn p50(&self) -> Option<f64> {
        if self.delays_ms.is_empty() {
            return None;
        }
        let mut sorted = self.delays_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        Some(percentile(&sorted, 0.5))
    }

    fn percentiles(&self) -> Option<(f64, f64, f64)> {
        if self.delays_ms.is_empty() {
            return None;
        }
        let mut sorted = self.delays_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        Some((
            percentile(&sorted, 0.5),
            percentile(&sorted, 0.95),
            percentile(&sorted, 0.99),
        ))
    }
}

#[derive(Debug, Default)]
struct GroupAgg {
    sigs: usize,
    per_endpoint: HashMap<String, GroupEndpointStats>,
}

#[derive(Debug)]
pub struct LeaderRow {
    pub leader: String,
    pub region: Region,
    pub sigs: usize,
    pub per_endpoint: HashMap<String, GroupEndpointStats>,
}

#[derive(Debug)]
pub struct RegionRow {
    pub region: Region,
    pub sigs: usize,
    pub per_endpoint: HashMap<String, GroupEndpointStats>,
}

#[derive(Debug)]
pub struct LeaderBreakdown {
    pub endpoint_names: Vec<String>,
    pub leaders: Vec<LeaderRow>,
    pub regions: Vec<RegionRow>,
    pub total_sigs: usize,
    pub resolved_sigs: usize,
    pub gated_leaders: usize,
    pub has_region_data: bool,
}

/// Slot attributed to a complete signature: the winning endpoint's slot when
/// available, otherwise any endpoint's.
fn signature_slot(sig_data: &HashMap<String, TransactionData>) -> Option<u64> {
    let winner = sig_data.values().min_by_key(|tx| tx.elapsed_since_start)?;
    winner
        .slot
        .or_else(|| sig_data.values().find_map(|tx| tx.slot))
}

/// Distinct slots across complete, non-backfill signatures — the set the
/// leader resolver needs to cover for the final report.
pub fn collect_signature_slots(comparator: &Comparator, expected_producers: usize) -> Vec<u64> {
    let mut slots = Vec::new();
    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if expected_producers > 0 && sig_data.len() != expected_producers {
            continue;
        }
        if sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs)
        {
            continue;
        }
        if let Some(slot) = signature_slot(sig_data) {
            slots.push(slot);
        }
    }
    slots.sort_unstable();
    slots.dedup();
    slots
}

pub fn compute_leader_breakdown(
    comparator: &Comparator,
    endpoints: &[EndpointDescriptor],
    leaders_by_slot: &HashMap<u64, Arc<str>>,
    validator_map: Option<&ValidatorMap>,
) -> LeaderBreakdown {
    let expected_producers = endpoints.len();
    let endpoint_names: Vec<String> = endpoints.iter().map(|e| e.name.clone()).collect();

    let mut per_leader: HashMap<Arc<str>, GroupAgg> = HashMap::new();
    let mut per_region: HashMap<Region, GroupAgg> = HashMap::new();
    let mut total_sigs = 0usize;
    let mut resolved_sigs = 0usize;

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if expected_producers > 0 && sig_data.len() != expected_producers {
            continue;
        }
        if sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs)
        {
            continue;
        }

        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };

        total_sigs += 1;

        let leader = signature_slot(sig_data)
            .and_then(|slot| leaders_by_slot.get(&slot))
            .cloned();
        let Some(leader) = leader else {
            continue;
        };
        resolved_sigs += 1;

        let region = validator_map
            .map(|map| map.classify(&leader))
            .unwrap_or(Region::Unknown);

        let first_endpoint_name = first_endpoint.clone();
        let leader_agg = per_leader.entry(leader).or_default();
        leader_agg.sigs += 1;
        let region_agg = per_region.entry(region).or_default();
        region_agg.sigs += 1;

        for (endpoint, tx) in sig_data.iter() {
            let delay_ms = if endpoint == &first_endpoint_name {
                0.0
            } else {
                diff_ms(tx, first_tx).max(0.0)
            };
            for agg in [&mut *leader_agg, &mut *region_agg] {
                let stats = agg.per_endpoint.entry(endpoint.clone()).or_default();
                if endpoint == &first_endpoint_name {
                    stats.firsts += 1;
                }
                stats.delays_ms.push(delay_ms);
            }
        }
    }

    let mut leaders: Vec<LeaderRow> = per_leader
        .into_iter()
        .map(|(leader, agg)| {
            let region = validator_map
                .map(|map| map.classify(&leader))
                .unwrap_or(Region::Unknown);
            LeaderRow {
                leader: leader.to_string(),
                region,
                sigs: agg.sigs,
                per_endpoint: agg.per_endpoint,
            }
        })
        .collect();
    leaders.sort_by(|a, b| b.sigs.cmp(&a.sigs).then_with(|| a.leader.cmp(&b.leader)));

    let gated_leaders = leaders
        .iter()
        .filter(|row| row.sigs < MIN_LEADER_SIGS)
        .count();
    leaders.retain(|row| row.sigs >= MIN_LEADER_SIGS);
    leaders.truncate(MAX_LEADER_ROWS);

    let has_region_data = validator_map.is_some_and(|map| map.is_loaded());
    let mut regions: Vec<RegionRow> = per_region
        .into_iter()
        .map(|(region, agg)| RegionRow {
            region,
            sigs: agg.sigs,
            per_endpoint: agg.per_endpoint,
        })
        .collect();
    regions.sort_by_key(|row| row.region);

    LeaderBreakdown {
        endpoint_names,
        leaders,
        regions,
        total_sigs,
        resolved_sigs,
        gated_leaders,
        has_region_data,
    }
}

pub fn display_leader_breakdown(breakdown: &LeaderBreakdown) {
    println!("\nLeader-aware results");
    println!("--------------------------------------------");

    if breakdown.total_sigs == 0 {
        println!("Not enough data");
        return;
    }

    println!(
        "Resolved leader for {}/{} signatures",
        breakdown.resolved_sigs, breakdown.total_sigs
    );

    if breakdown.resolved_sigs == 0 {
        println!("No leader-attributed signatures; check rpc_url and provider slot support");
        return;
    }

    if breakdown.has_region_data && !breakdown.regions.is_empty() {
        let mut table = Table::new();
        table.load_preset(table_preset());
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            "Region", "Endpoint", "Sigs", "First %", "P50 ms", "P95 ms", "P99 ms",
        ]);
        for row in &breakdown.regions {
            for endpoint in &breakdown.endpoint_names {
                let Some(stats) = row.per_endpoint.get(endpoint) else {
                    continue;
                };
                let (p50, p95, p99) = match stats.percentiles() {
                    Some(values) => values,
                    None => continue,
                };
                table.add_row(vec![
                    row.region.as_str().to_string(),
                    endpoint.clone(),
                    row.sigs.to_string(),
                    format_percent(stats.first_share(row.sigs)),
                    format!("{:.2}", p50),
                    format!("{:.2}", p95),
                    format!("{:.2}", p99),
                ]);
            }
        }
        println!("{}", table);
    }

    if breakdown.leaders.is_empty() {
        println!(
            "No leaders with at least {} signatures ({} below threshold)",
            MIN_LEADER_SIGS, breakdown.gated_leaders
        );
        return;
    }

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    let mut header = vec![
        "Leader".to_string(),
        "Region".to_string(),
        "Sigs".to_string(),
    ];
    for endpoint in &breakdown.endpoint_names {
        header.push(format!("{} First% / P50ms", endpoint));
    }
    table.set_header(header);

    for row in &breakdown.leaders {
        let mut cells = vec![
            row.leader.clone(),
            row.region.as_str().to_string(),
            row.sigs.to_string(),
        ];
        for endpoint in &breakdown.endpoint_names {
            let cell = match row.per_endpoint.get(endpoint) {
                Some(stats) => {
                    let p50 = stats
                        .p50()
                        .map(|v| format!("{:.2}", v))
                        .unwrap_or_else(|| "—".to_string());
                    format!("{} / {}", format_percent(stats.first_share(row.sigs)), p50)
                }
                None => "—".to_string(),
            };
            cells.push(cell);
        }
        table.add_row(cells);
    }
    println!("{}", table);

    if breakdown.gated_leaders > 0 {
        println!(
            "({} leaders with fewer than {} signatures not shown)",
            breakdown.gated_leaders, MIN_LEADER_SIGS
        );
    }
}

fn diff_ms(tx: &TransactionData, first_tx: &TransactionData) -> f64 {
    let delta: Duration = tx
        .elapsed_since_start
        .saturating_sub(first_tx.elapsed_since_start);
    delta.as_secs_f64() * 1_000.0
}

fn build_summary(
    endpoint: String,
    mode: String,
    stats: EndpointStats,
    total_signatures: usize,
) -> EndpointSummary {
    let mut summary = EndpointSummary {
        name: endpoint,
        mode,
        valid_transactions: stats.total_observations,
        first_detections: stats.first_detections,
        backfill_transactions: stats.backfill_transactions,
        ..Default::default()
    };

    if total_signatures > 0 {
        summary.first_share = stats.first_detections as f64 / total_signatures as f64;
    }

    if !stats.delays_ms.is_empty() {
        let mut sorted = stats.delays_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        summary.p50_delay_ms = Some(percentile(&sorted, 0.5));
        summary.p95_delay_ms = Some(percentile(&sorted, 0.95));
        summary.p99_delay_ms = Some(percentile(&sorted, 0.99));
    }

    summary
}

fn format_latency_value(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "—".to_string())
}

fn compare_latency(lhs: &EndpointSummary, rhs: &EndpointSummary) -> Ordering {
    match (lhs.p50_delay_ms, rhs.p50_delay_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{:.2}", value * 100.0)
    } else {
        "—".to_string()
    }
}
