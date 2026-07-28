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
    pub peer_observations: usize,
    pub peer_unique_detections: usize,
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
    pub peer_observations: usize,
    pub peer_coverage: f64,
    pub peer_unique_detections: usize,
    pub peer_missed_signatures: usize,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
    pub peer_union_signatures: usize,
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
    let mut peer_union_signatures = 0usize;

    for endpoint in endpoints {
        endpoint_stats.insert(endpoint.name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        let is_historical = sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs);
        let observed_endpoints: Vec<&str> = endpoints
            .iter()
            .filter(|endpoint| sig_data.contains_key(&endpoint.name))
            .map(|endpoint| endpoint.name.as_str())
            .collect();

        if !is_historical && !observed_endpoints.is_empty() {
            peer_union_signatures += 1;
            for endpoint in &observed_endpoints {
                if let Some(stats) = endpoint_stats.get_mut(*endpoint) {
                    stats.peer_observations += 1;
                }
            }
            if observed_endpoints.len() == 1
                && let Some(stats) = endpoint_stats.get_mut(observed_endpoints[0])
            {
                stats.peer_unique_detections += 1;
            }
        }

        if expected_producers > 0 && observed_endpoints.len() != expected_producers {
            // Latency comparisons require every endpoint, but peer coverage
            // intentionally retains these partial observations.
            continue;
        }

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
            build_summary(
                endpoint,
                mode,
                stats,
                total_signatures,
                peer_union_signatures,
            )
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
        peer_union_signatures,
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

    if summary.has_data {
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
    } else {
        println!("Not enough complete matches for latency comparison");
    }

    display_peer_coverage(summary);
}

fn display_peer_coverage(summary: &RunSummary) {
    println!("\nPeer coverage");
    println!("--------------------------------------------");

    if summary.endpoints.len() < 2 {
        println!("Peer coverage requires at least two endpoints");
        return;
    }
    if summary.peer_union_signatures == 0 {
        println!("Not enough data");
        return;
    }

    println!(
        "Union: {} live signatures; shared by every endpoint: {}",
        summary.peer_union_signatures, summary.total_signatures
    );

    let mut rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
    rows.sort_by(|a, b| {
        b.peer_coverage
            .partial_cmp(&a.peer_coverage)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "Endpoint",
        "Mode",
        "Seen",
        "Coverage %",
        "Unique",
        "Missed",
    ]);
    for endpoint in rows {
        table.add_row(vec![
            endpoint.name.clone(),
            endpoint.mode.clone(),
            endpoint.peer_observations.to_string(),
            format_percent(endpoint.peer_coverage),
            endpoint.peer_unique_detections.to_string(),
            endpoint.peer_missed_signatures.to_string(),
        ]);
    }
    println!("{}", table);
    println!(
        "Unique = seen only by that endpoint; Missed = seen by at least one peer; backfill is excluded"
    );
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
            "peer_observations": endpoint.peer_observations,
            "peer_coverage_rate": endpoint.peer_coverage,
            "peer_unique_detections": endpoint.peer_unique_detections,
            "peer_missed_signatures": endpoint.peer_missed_signatures,
        });
        per_endpoint.insert(endpoint.name.clone(), payload);
    }

    json!({
        "total_signatures": summary.total_signatures,
        "backfill_signatures": summary.backfill_signatures,
        "peer_union_signatures": summary.peer_union_signatures,
        "peer_shared_signatures": summary.total_signatures,
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

#[derive(Debug, Default, Clone)]
pub struct PeerCoverageStats {
    pub observations: usize,
    pub unique_detections: usize,
}

impl PeerCoverageStats {
    fn coverage(&self, union_signatures: usize) -> f64 {
        if union_signatures == 0 {
            return 0.0;
        }
        self.observations as f64 / union_signatures as f64
    }

    fn missed(&self, union_signatures: usize) -> usize {
        union_signatures.saturating_sub(self.observations)
    }
}

#[derive(Debug, Default)]
struct CoverageAgg {
    union_signatures: usize,
    shared_signatures: usize,
    per_endpoint: HashMap<String, PeerCoverageStats>,
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
pub struct CoverageRegionRow {
    pub region: Region,
    pub union_signatures: usize,
    pub shared_signatures: usize,
    pub per_endpoint: HashMap<String, PeerCoverageStats>,
}

#[derive(Debug)]
pub struct LeaderBreakdown {
    pub endpoint_names: Vec<String>,
    pub leaders: Vec<LeaderRow>,
    pub regions: Vec<RegionRow>,
    pub total_sigs: usize,
    pub resolved_sigs: usize,
    pub coverage_regions: Vec<CoverageRegionRow>,
    pub coverage_total_sigs: usize,
    pub coverage_resolved_sigs: usize,
    pub gated_leaders: usize,
    pub has_region_data: bool,
}

/// Slot attributed to a signature: the earliest endpoint's slot when
/// available, otherwise any endpoint's.
fn signature_slot(sig_data: &HashMap<String, TransactionData>) -> Option<u64> {
    let winner = sig_data.values().min_by_key(|tx| tx.elapsed_since_start)?;
    winner
        .slot
        .or_else(|| sig_data.values().find_map(|tx| tx.slot))
}

/// Distinct slots across all live signatures observed by at least one endpoint
/// — the set the leader resolver needs for latency and peer-coverage reports.
pub fn collect_signature_slots(comparator: &Comparator) -> Vec<u64> {
    let mut slots = Vec::new();
    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
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
    let mut coverage_per_region: HashMap<Region, CoverageAgg> = HashMap::new();
    let mut total_sigs = 0usize;
    let mut resolved_sigs = 0usize;
    let mut coverage_total_sigs = 0usize;
    let mut coverage_resolved_sigs = 0usize;

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs)
        {
            continue;
        }

        let observed_endpoints: Vec<&str> = endpoint_names
            .iter()
            .filter(|endpoint| sig_data.contains_key(*endpoint))
            .map(String::as_str)
            .collect();
        if observed_endpoints.is_empty() {
            continue;
        }

        let is_complete = expected_producers > 0 && observed_endpoints.len() == expected_producers;
        coverage_total_sigs += 1;
        if is_complete {
            total_sigs += 1;
        }

        let leader = signature_slot(sig_data)
            .and_then(|slot| leaders_by_slot.get(&slot))
            .cloned();
        let Some(leader) = leader else {
            continue;
        };
        coverage_resolved_sigs += 1;

        let region = validator_map
            .map(|map| map.classify(&leader))
            .unwrap_or(Region::Unknown);

        let coverage_agg = coverage_per_region.entry(region).or_default();
        coverage_agg.union_signatures += 1;
        if is_complete {
            coverage_agg.shared_signatures += 1;
        }
        for endpoint in &observed_endpoints {
            coverage_agg
                .per_endpoint
                .entry((*endpoint).to_string())
                .or_default()
                .observations += 1;
        }
        if observed_endpoints.len() == 1 {
            coverage_agg
                .per_endpoint
                .entry(observed_endpoints[0].to_string())
                .or_default()
                .unique_detections += 1;
        }

        if !is_complete {
            continue;
        }
        resolved_sigs += 1;

        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };
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

    let mut coverage_regions: Vec<CoverageRegionRow> = coverage_per_region
        .into_iter()
        .map(|(region, agg)| CoverageRegionRow {
            region,
            union_signatures: agg.union_signatures,
            shared_signatures: agg.shared_signatures,
            per_endpoint: agg.per_endpoint,
        })
        .collect();
    coverage_regions.sort_by_key(|row| row.region);

    LeaderBreakdown {
        endpoint_names,
        leaders,
        regions,
        total_sigs,
        resolved_sigs,
        coverage_regions,
        coverage_total_sigs,
        coverage_resolved_sigs,
        gated_leaders,
        has_region_data,
    }
}

pub fn display_leader_breakdown(breakdown: &LeaderBreakdown) {
    println!("\nLeader-aware results");
    println!("--------------------------------------------");

    if breakdown.coverage_total_sigs == 0 {
        println!("Not enough data");
        return;
    }

    println!(
        "Resolved leader for {}/{} peer-union signatures",
        breakdown.coverage_resolved_sigs, breakdown.coverage_total_sigs
    );

    if breakdown.coverage_resolved_sigs == 0 {
        println!("No leader-attributed signatures; check rpc_url and provider slot support");
        return;
    }

    if breakdown.has_region_data && !breakdown.coverage_regions.is_empty() {
        println!("\nPeer coverage by leader region");
        let mut table = Table::new();
        table.load_preset(table_preset());
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            "Region",
            "Endpoint",
            "Union",
            "Shared",
            "Seen",
            "Coverage %",
            "Unique",
            "Missed",
        ]);
        for row in &breakdown.coverage_regions {
            for endpoint in &breakdown.endpoint_names {
                let stats = row.per_endpoint.get(endpoint).cloned().unwrap_or_default();
                table.add_row(vec![
                    row.region.as_str().to_string(),
                    endpoint.clone(),
                    row.union_signatures.to_string(),
                    row.shared_signatures.to_string(),
                    stats.observations.to_string(),
                    format_percent(stats.coverage(row.union_signatures)),
                    stats.unique_detections.to_string(),
                    stats.missed(row.union_signatures).to_string(),
                ]);
            }
        }
        println!("{}", table);
    }

    if breakdown.total_sigs == 0 {
        println!("No complete matches for leader-aware latency comparison");
        return;
    }

    println!(
        "\nResolved leader for {}/{} complete signatures",
        breakdown.resolved_sigs, breakdown.total_sigs
    );

    if breakdown.resolved_sigs == 0 {
        println!("No leader-attributed complete signatures for latency comparison");
        return;
    }

    if breakdown.has_region_data && !breakdown.regions.is_empty() {
        println!("\nLatency by leader region");
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
    peer_union_signatures: usize,
) -> EndpointSummary {
    let peer_coverage = if peer_union_signatures == 0 {
        0.0
    } else {
        stats.peer_observations as f64 / peer_union_signatures as f64
    };
    let mut summary = EndpointSummary {
        name: endpoint,
        mode,
        valid_transactions: stats.total_observations,
        first_detections: stats.first_detections,
        backfill_transactions: stats.backfill_transactions,
        peer_observations: stats.peer_observations,
        peer_coverage,
        peer_unique_detections: stats.peer_unique_detections,
        peer_missed_signatures: peer_union_signatures.saturating_sub(stats.peer_observations),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ValidatorMapSettings;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn endpoints() -> Vec<EndpointDescriptor> {
        vec![
            EndpointDescriptor {
                name: "Pulse".to_string(),
                mode: "raiden_pulse".to_string(),
            },
            EndpointDescriptor {
                name: "Shredstream".to_string(),
                mode: "shredstream".to_string(),
            },
        ]
    }

    fn transaction(elapsed_ms: u64, slot: u64, historical: bool) -> TransactionData {
        TransactionData {
            wallclock_secs: if historical { 99.0 } else { 101.0 },
            elapsed_since_start: Duration::from_millis(elapsed_ms),
            start_wallclock_secs: 100.0,
            slot: Some(slot),
        }
    }

    fn coverage_fixture() -> Comparator {
        let comparator = Comparator::new();
        comparator.add_batch(
            "Pulse",
            HashMap::from([
                ("shared".to_string(), transaction(5, 1, false)),
                ("pulse-in".to_string(), transaction(10, 2, false)),
                ("pulse-out".to_string(), transaction(20, 3, false)),
                ("backfill".to_string(), transaction(30, 99, true)),
            ]),
        );
        comparator.add_batch(
            "Shredstream",
            HashMap::from([
                ("shared".to_string(), transaction(6, 1, false)),
                ("shred-out".to_string(), transaction(15, 4, false)),
            ]),
        );
        comparator
    }

    #[test]
    fn peer_coverage_uses_live_union_and_keeps_partial_observations() {
        let summary = compute_run_summary(&coverage_fixture(), &endpoints());

        assert_eq!(summary.peer_union_signatures, 4);
        assert_eq!(summary.total_signatures, 1);

        let pulse = summary
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "Pulse")
            .unwrap();
        assert_eq!(pulse.peer_observations, 3);
        assert_eq!(pulse.peer_unique_detections, 2);
        assert_eq!(pulse.peer_missed_signatures, 1);
        assert!((pulse.peer_coverage - 0.75).abs() < f64::EPSILON);

        let shredstream = summary
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "Shredstream")
            .unwrap();
        assert_eq!(shredstream.peer_observations, 2);
        assert_eq!(shredstream.peer_unique_detections, 1);
        assert_eq!(shredstream.peer_missed_signatures, 2);
        assert!((shredstream.peer_coverage - 0.5).abs() < f64::EPSILON);

        let metrics = build_metrics_report(&summary);
        assert_eq!(metrics["peer_union_signatures"], 4);
        assert_eq!(metrics["peer_shared_signatures"], 1);
        assert_eq!(metrics["per_endpoint"]["Pulse"]["peer_coverage_rate"], 0.75);
    }

    #[test]
    fn slot_collection_includes_partial_live_signatures() {
        assert_eq!(
            collect_signature_slots(&coverage_fixture()),
            vec![1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn peer_coverage_is_split_by_leader_region() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let map_path = std::env::temp_dir().join(format!(
            "geyserbench-validator-map-{}-{suffix}.json",
            std::process::id()
        ));
        fs::write(
            &map_path,
            r#"{
                "version": 1,
                "entries": {
                    "leader-in": {"in_region": true},
                    "leader-out": {"in_region": false}
                }
            }"#,
        )
        .unwrap();
        let validator_map = ValidatorMap::new(ValidatorMapSettings {
            source: map_path.to_string_lossy().into_owned(),
            rtt_icmp_threshold_us: 5_000,
            rtt_quic_threshold_us: None,
            refresh_secs: 300,
        });
        assert!(validator_map.load().await);

        let leaders_by_slot = HashMap::from([
            (1, Arc::<str>::from("leader-in")),
            (2, Arc::<str>::from("leader-in")),
            (3, Arc::<str>::from("leader-out")),
            (4, Arc::<str>::from("leader-out")),
        ]);
        let breakdown = compute_leader_breakdown(
            &coverage_fixture(),
            &endpoints(),
            &leaders_by_slot,
            Some(&validator_map),
        );
        let _ = fs::remove_file(map_path);

        assert_eq!(breakdown.coverage_total_sigs, 4);
        assert_eq!(breakdown.coverage_resolved_sigs, 4);

        let in_region = breakdown
            .coverage_regions
            .iter()
            .find(|row| row.region == Region::In)
            .unwrap();
        assert_eq!(in_region.union_signatures, 2);
        assert_eq!(in_region.shared_signatures, 1);
        assert_eq!(in_region.per_endpoint["Pulse"].observations, 2);
        assert_eq!(in_region.per_endpoint["Shredstream"].observations, 1);

        let out_region = breakdown
            .coverage_regions
            .iter()
            .find(|row| row.region == Region::Out)
            .unwrap();
        assert_eq!(out_region.union_signatures, 2);
        assert_eq!(out_region.shared_signatures, 0);
        assert_eq!(out_region.per_endpoint["Pulse"].unique_detections, 1);
        assert_eq!(out_region.per_endpoint["Shredstream"].unique_detections, 1);
    }
}
