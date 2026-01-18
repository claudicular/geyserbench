use std::{error::Error, sync::atomic::Ordering, time::Duration};

use reqwest::Client;
use tokio::task;
use tracing::{debug, error, info, warn};

use crate::{
    config::{Config, Endpoint},
    utils::TransactionData,
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature,
    },
};

pub struct InfluxdbProvider;

impl GeyserProvider for InfluxdbProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_influxdb_endpoint(endpoint, config, context).await })
    }
}

#[derive(Debug, Default)]
struct InfluxRecord {
    time: String,
    tx_signature: String,
    timestamp_us: i64,
}

/// Parse InfluxDB CSV response into records
/// InfluxDB annotated CSV format has:
/// - Lines starting with # are annotations (metadata)
/// - Empty lines separate tables
/// - Header row with column names
/// - Data rows
fn parse_influx_csv(csv_text: &str) -> Vec<InfluxRecord> {
    let mut records = Vec::new();
    let mut headers: Vec<String> = Vec::new();
    let mut in_data_section = false;

    for line in csv_text.lines() {
        // Skip empty lines
        if line.is_empty() {
            in_data_section = false;
            headers.clear();
            continue;
        }

        // Skip annotation lines (start with #)
        if line.starts_with('#') {
            continue;
        }

        // Parse as CSV
        let fields: Vec<&str> = line.split(',').collect();

        if !in_data_section {
            // This is a header row
            headers = fields.iter().map(|s| s.to_string()).collect();
            in_data_section = true;
            continue;
        }

        // This is a data row - find our columns
        let time_idx = headers.iter().position(|h| h == "_time");
        let sig_idx = headers.iter().position(|h| h == "tx_signature");
        let ts_idx = headers.iter().position(|h| h == "timestamp_us");

        if let (Some(time_idx), Some(sig_idx), Some(ts_idx)) = (time_idx, sig_idx, ts_idx) {
            if fields.len() > time_idx && fields.len() > sig_idx && fields.len() > ts_idx {
                let timestamp_us = fields[ts_idx].parse::<i64>().unwrap_or(0);

                records.push(InfluxRecord {
                    time: fields[time_idx].to_string(),
                    tx_signature: fields[sig_idx].to_string(),
                    timestamp_us,
                });
            }
        }
    }

    records
}

/// Query InfluxDB using raw HTTP API
async fn query_influxdb(
    client: &Client,
    url: &str,
    org: &str,
    token: &str,
    query: &str,
) -> Result<Vec<InfluxRecord>, Box<dyn Error + Send + Sync>> {
    let query_url = format!("{}/api/v2/query?org={}", url.trim_end_matches('/'), org);

    let response = client
        .post(&query_url)
        .header("Authorization", format!("Token {}", token))
        .header("Content-Type", "application/vnd.flux")
        .header("Accept", "application/csv")
        .body(query.to_string())
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("InfluxDB query failed with status {}: {}", status, body).into());
    }

    let csv_text = response.text().await?;
    debug!(csv_length = csv_text.len(), "Received CSV response from InfluxDB");

    let records = parse_influx_csv(&csv_text);
    Ok(records)
}

/// Diagnostic function to test InfluxDB connectivity and query parsing
async fn run_influxdb_diagnostic(
    client: &Client,
    url: &str,
    org: &str,
    token: &str,
    bucket: &str,
    stage: &str,
    endpoint_name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(endpoint = %endpoint_name, "=== INFLUXDB DIAGNOSTIC START ===");

    let diagnostic_query = format!(
        r#"from(bucket: "{bucket}")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "fast_geyser_latency")
  |> filter(fn: (r) => r.stage == "{stage}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r.tx_signature and exists r.timestamp_us)
  |> keep(columns: ["_time", "tx_signature", "timestamp_us"])
  |> sort(columns: ["_time"])
  |> limit(n: 10)"#,
        bucket = bucket,
        stage = stage,
    );

    info!(endpoint = %endpoint_name, query = %diagnostic_query, "Diagnostic query (should return results with -1m range)");
    info!(endpoint = %endpoint_name, "Sending diagnostic query to InfluxDB...");

    match query_influxdb(client, url, org, token, &diagnostic_query).await {
        Ok(records) => {
            let count = records.len();
            info!(endpoint = %endpoint_name, record_count = count, "Diagnostic query returned");

            if count == 0 {
                warn!(endpoint = %endpoint_name, "Diagnostic returned 0 records - check if data exists for this stage");
            } else {
                info!(endpoint = %endpoint_name, "Diagnostic SUCCESS - InfluxDB queries working");
                for (i, record) in records.iter().enumerate().take(3) {
                    info!(
                        endpoint = %endpoint_name,
                        index = i,
                        time = %record.time,
                        tx_signature = %record.tx_signature,
                        timestamp_us = record.timestamp_us,
                        "Sample record"
                    );
                }
            }
        }
        Err(e) => {
            error!(endpoint = %endpoint_name, error = ?e, "Diagnostic query FAILED");
        }
    }

    info!(endpoint = %endpoint_name, "=== INFLUXDB DIAGNOSTIC END ===");
    Ok(())
}

async fn process_influxdb_endpoint(
    endpoint: Endpoint,
    _config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant: _,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;
    let endpoint_name = endpoint.name.clone();

    // Extract InfluxDB-specific configuration
    let org = endpoint
        .influx_org
        .as_ref()
        .ok_or("influx_org is required for InfluxDB endpoints")?;
    let bucket = endpoint
        .influx_bucket
        .as_ref()
        .ok_or("influx_bucket is required for InfluxDB endpoints")?;
    let stage = endpoint
        .influx_stage
        .as_ref()
        .ok_or("influx_stage is required for InfluxDB endpoints")?;
    let token = endpoint
        .x_token
        .as_ref()
        .ok_or("x_token is required for InfluxDB endpoints")?;

    info!(
        endpoint = %endpoint_name,
        url = %endpoint.url,
        org = %org,
        bucket = %bucket,
        stage = %stage,
        "Connecting to InfluxDB"
    );

    let client = Client::new();

    // Run diagnostic before starting the main loop
    if let Err(e) = run_influxdb_diagnostic(&client, &endpoint.url, org, token, bucket, stage, &endpoint_name).await {
        error!(endpoint = %endpoint_name, error = ?e, "Diagnostic failed");
    }

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    info!(endpoint = %endpoint_name, "Connected to InfluxDB, starting poll loop with -15s lookback window");

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Build Flux query with fixed lookback window
                // Using -15s to catch batched writes (InfluxDB producer may batch every ~10s)
                // Deduplication is handled by TransactionAccumulator
                let query_str = format!(
                    r#"from(bucket: "{bucket}")
  |> range(start: -15s)
  |> filter(fn: (r) => r._measurement == "fast_geyser_latency")
  |> filter(fn: (r) => r.stage == "{stage}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r.tx_signature and exists r.timestamp_us)
  |> keep(columns: ["_time", "tx_signature", "timestamp_us"])
  |> sort(columns: ["_time"])"#,
                    bucket = bucket,
                    stage = stage,
                );

                debug!(endpoint = %endpoint_name, query = %query_str, "Executing InfluxDB query");

                match query_influxdb(&client, &endpoint.url, org, token, &query_str).await {
                    Ok(records) => {
                        let record_count = records.len();
                        debug!(endpoint = %endpoint_name, record_count, "Query completed");
                        if record_count == 0 {
                            debug!(endpoint = %endpoint_name, "No records returned - check if data exists for stage and time range");
                        }

                        for record in records {
                            debug!(
                                endpoint = %endpoint_name,
                                time = %record.time,
                                signature = %record.tx_signature,
                                timestamp_us = record.timestamp_us,
                                "Processing InfluxDB record"
                            );

                            let signature = record.tx_signature;
                            if signature.is_empty() {
                                debug!(endpoint = %endpoint_name, "Skipping record with empty signature");
                                continue;
                            }

                            // Use the timestamp_us field from the record as the observation time
                            // This is required - we do not fall back to _time
                            let influx_timestamp_us = record.timestamp_us;
                            if influx_timestamp_us == 0 {
                                error!(
                                    endpoint = %endpoint_name,
                                    signature = %signature,
                                    "Record missing valid timestamp_us - this field is required for latency comparison"
                                );
                                continue;
                            }
                            let influx_timestamp_secs = influx_timestamp_us as f64 / 1_000_000.0;

                            // Calculate elapsed_since_start using InfluxDB timestamp
                            // This is the key difference from other providers:
                            // we use the logged timestamp, not the current time
                            let elapsed_secs = (influx_timestamp_secs - start_wallclock_secs).max(0.0);
                            let elapsed = Duration::from_secs_f64(elapsed_secs);

                            let tx_data = TransactionData {
                                wallclock_secs: influx_timestamp_secs,
                                elapsed_since_start: elapsed,
                                start_wallclock_secs,
                            };

                            let updated = accumulator.record(signature.clone(), tx_data.clone());

                            if updated {
                                if let Some(envelope) = build_signature_envelope(
                                    &comparator,
                                    &endpoint_name,
                                    &signature,
                                    tx_data,
                                    total_producers,
                                ) {
                                    if let Some(target) = target_transactions {
                                        let shared = shared_counter
                                            .fetch_add(1, Ordering::AcqRel)
                                            + 1;
                                        if let Some(tracker) = progress.as_ref() {
                                            tracker.record(shared);
                                        }
                                        if shared >= target
                                            && !shared_shutdown.swap(true, Ordering::AcqRel)
                                        {
                                            info!(
                                                endpoint = %endpoint_name,
                                                target,
                                                "Reached shared signature target; broadcasting shutdown"
                                            );
                                            let _ = shutdown_tx.send(());
                                        }
                                    }

                                    if let Some(sender) = signature_sender.as_ref() {
                                        enqueue_signature(sender, &endpoint_name, &signature, envelope);
                                    }
                                }
                            }

                            transaction_count += 1;
                        }
                    }
                    Err(e) => {
                        warn!(
                            endpoint = %endpoint_name,
                            error = ?e,
                            "Error querying InfluxDB, will retry"
                        );
                    }
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "InfluxDB provider closed after processing transactions"
    );
    Ok(())
}
