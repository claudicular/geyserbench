use std::{error::Error, sync::atomic::Ordering, time::Duration};

use influxdb2::Client;
use influxdb2::models::Query;
use influxdb2_structmap::FromMap;
use tokio::task;
use tracing::{debug, info, warn};

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

impl FromMap for InfluxRecord {
    fn from_genericmap(map: influxdb2_structmap::GenericMap) -> Self {
        let mut record = InfluxRecord::default();

        if let Some(influxdb2_structmap::value::Value::String(v)) = map.get("_time") {
            record.time = v.clone();
        }
        if let Some(influxdb2_structmap::value::Value::String(v)) = map.get("tx_signature") {
            record.tx_signature = v.clone();
        }
        if let Some(v) = map.get("timestamp_us") {
            record.timestamp_us = match v {
                influxdb2_structmap::value::Value::Long(n) => *n,
                influxdb2_structmap::value::Value::Double(n) => n.into_inner() as i64,
                influxdb2_structmap::value::Value::UnsignedLong(n) => *n as i64,
                _ => 0,
            };
        }

        record
    }
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

    let client = Client::new(&endpoint.url, org, token);

    // Track the last timestamp we've seen to avoid duplicates (as RFC3339 for Flux queries)
    let start_datetime = chrono::DateTime::from_timestamp(
        start_wallclock_secs as i64,
        ((start_wallclock_secs.fract()) * 1_000_000_000.0) as u32,
    )
    .unwrap_or_else(|| chrono::Utc::now().into());
    let mut last_query_time = start_datetime.to_rfc3339();
    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    info!(endpoint = %endpoint_name, start_time = %last_query_time, "Connected to InfluxDB, starting poll loop");

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Build Flux query to get new signatures since last query
                // We query for records where:
                // - measurement is "fast_geyser_latency"
                // - stage matches the configured stage
                // - _time is after our last query time
                // Note: stage is a field, not a tag, so we must filter after pivot
                // This is less efficient than tag-based filtering but works with current schema
                let query_str = format!(
                    r#"from(bucket: "{bucket}")
  |> range(start: {last_time})
  |> filter(fn: (r) => r._measurement == "fast_geyser_latency")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r.stage == "{stage}")
  |> filter(fn: (r) => exists r.tx_signature and exists r.timestamp_us)
  |> keep(columns: ["_time", "tx_signature", "timestamp_us"])
  |> sort(columns: ["_time"])"#,
                    bucket = bucket,
                    stage = stage,
                    last_time = last_query_time,
                );

                debug!(endpoint = %endpoint_name, query = %query_str, "Executing InfluxDB query");

                let query = Query::new(query_str);

                match client.query::<InfluxRecord>(Some(query)).await {
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
                            let influx_timestamp_us = record.timestamp_us;
                            let influx_timestamp_secs = influx_timestamp_us as f64 / 1_000_000.0;

                            // Update last query time to avoid re-processing
                            // Use the record's _time field directly (already RFC3339)
                            if !record.time.is_empty() && record.time > last_query_time {
                                last_query_time = record.time.clone();
                            }

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
