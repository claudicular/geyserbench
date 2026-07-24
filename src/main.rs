pub use {
    bs58,
    futures_util::stream::StreamExt,
    serde::{Deserialize, Serialize},
    std::{
        env,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
};

mod analysis;
mod config;
mod leader;
mod proto;
mod providers;
mod sink;
mod utils;

use anyhow::{Result, anyhow};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use utils::{Comparator, ProgressTracker, get_current_timestamp};

const DEFAULT_CONFIG_PATH: &str = "config.toml";

struct CliArgs {
    config_path: Option<String>,
}

impl CliArgs {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = CliArgs { config_path: None };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let value = args.next().unwrap_or_else(|| {
                        eprintln!("Missing value for --config");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.config_path = Some(value);
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("Unknown argument: {}", other);
                    print_usage();
                    std::process::exit(1);
                }
            }
        }

        parsed
    }
}

fn print_usage() {
    eprintln!("Usage: geyserbench [--config <PATH>]");
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|err| anyhow!(err))?;

    let cli = CliArgs::parse();
    let config_path = cli.config_path.as_deref().unwrap_or(DEFAULT_CONFIG_PATH);
    let config = config::ConfigToml::load_or_create(config_path)?;
    info!(config_path = config_path, "Loaded configuration");

    let duration_mode = config.config.duration_secs.is_some();
    if duration_mode {
        info!(
            duration_secs = config.config.duration_secs,
            "Duration mode: transaction target ignored"
        );
    }

    let leader_resolver = config
        .config
        .rpc_url
        .clone()
        .map(|url| Arc::new(leader::LeaderResolver::new(url)));
    if let Some(resolver) = leader_resolver.as_ref() {
        resolver.prefetch_current().await;
    }

    let validator_map = config
        .validator_map
        .clone()
        .map(|settings| Arc::new(leader::ValidatorMap::new(settings)));
    if let Some(map) = validator_map.as_ref() {
        map.load().await;
        if leader_resolver.is_none() {
            warn!(
                "validator_map configured without config.rpc_url; leaders cannot be resolved so region classification will be unused"
            );
        }
    }

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let start_time_local = get_current_timestamp();
    let comparator = Arc::new(Comparator::new());
    let start_instant = Instant::now();
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    let endpoint_descriptors: Vec<analysis::EndpointDescriptor> = config
        .endpoint
        .iter()
        .map(|endpoint| analysis::EndpointDescriptor {
            name: endpoint.name.clone(),
            mode: endpoint.kind.as_str().to_string(),
        })
        .collect();
    let global_target = if duration_mode {
        None
    } else if config.config.transactions > 0 {
        Some(config.config.transactions as usize)
    } else {
        None
    };
    let progress_tracker = global_target.map(|target| Arc::new(ProgressTracker::new(target)));

    let mut sink_handle = None;
    if let Some(sink_settings) = config.influx_sink.clone() {
        let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel();
        comparator.set_sink(sink_tx);
        sink_handle = Some(sink::spawn_influx_sink(
            sink_settings,
            leader_resolver.clone(),
            validator_map.clone(),
            sink_rx,
        ));
    }

    if let Some(duration_secs) = config.config.duration_secs {
        let shutdown_for_timer = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
            info!(
                duration_secs,
                "Benchmark duration elapsed; broadcasting shutdown"
            );
            let _ = shutdown_for_timer.send(());
        });
    }

    let total_producers = config.endpoint.len();
    for endpoint in config.endpoint.clone() {
        let provider = providers::create_provider(&endpoint.kind);
        let shared_config = config.config.clone();
        let context = providers::ProviderContext {
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx: shutdown_tx.subscribe(),
            start_wallclock_secs: start_time_local,
            start_instant,
            comparator: comparator.clone(),
            shared_counter: shared_counter.clone(),
            shared_shutdown: shared_shutdown.clone(),
            target_transactions: global_target,
            total_producers,
            progress: progress_tracker.clone(),
        };

        handles.push(provider.process(endpoint, shared_config, context));
    }

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let shared_shutdown = shared_shutdown.clone();
        let aborted = aborted.clone();
        async move {
            match ctrl_c().await {
                Ok(()) => {
                    if duration_mode {
                        // Long-running benches keep their results on interrupt:
                        // finalize early instead of aborting.
                        info!("Received Ctrl+C; finishing duration run early");
                    } else {
                        let already_aborting = aborted.swap(true, Ordering::AcqRel);
                        if already_aborting {
                            info!("Received additional Ctrl+C; shutdown already in progress");
                        } else {
                            info!("Received Ctrl+C; initiating shutdown");
                        }
                    }
                    shared_shutdown.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(());
                }
                Err(err) => error!(error = %err, "Failed to listen for Ctrl+C"),
            }
        }
    });

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => error!(error = ?e, "Provider task returned error"),
            Err(e) => error!(error = ?e, "Provider join error"),
        }
    }

    comparator.close_sink();
    if let Some(handle) = sink_handle
        && let Err(err) = handle.await
    {
        warn!(error = ?err, "InfluxDB sink task join error");
    }

    let run_aborted = aborted.load(Ordering::Acquire);

    if !run_aborted {
        let summary = analysis::compute_run_summary(comparator.as_ref(), &endpoint_descriptors);
        analysis::display_run_summary(&summary);
        let metrics_json = analysis::build_metrics_report(&summary);
        debug!(metrics = %metrics_json, "Computed run metrics");

        if let Some(resolver) = leader_resolver.as_ref() {
            let slots =
                analysis::collect_signature_slots(comparator.as_ref(), endpoint_descriptors.len());
            let leaders_by_slot = resolver.resolve_many(slots).await;
            let breakdown = analysis::compute_leader_breakdown(
                comparator.as_ref(),
                &endpoint_descriptors,
                &leaders_by_slot,
                validator_map.as_deref(),
            );
            analysis::display_leader_breakdown(&breakdown);
        }
    } else {
        info!("Benchmark aborted before completion; no results were generated");
    }

    Ok(())
}
