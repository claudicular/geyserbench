//! `fastlane_ring`: reads the agave fast lane's phase-3 output ring
//! (`/dev/shm/fastlane.out.ring`, agave fork branch `fast-lane`, config `out_ring = true`).
//!
//! The fast lane publishes one record per transaction when it becomes FINAL, carrying the
//! transaction's grouped-notification accounts whose owner is in the fast lane's owner
//! filter (SPL Token and Token-2022 by default, plus its `out_owners`). A signature is
//! observed when its record has an account **owned by `config.account`**: the same rule as
//! `yellowstone_tx_accounts` (`transaction_accounts{owner: [account]}`), so both providers
//! observe the same population as long as `config.account` is in the fast lane's owner
//! filter (otherwise this provider sees nothing).
//!
//! Like `shredstream_shmem`, a dedicated OS thread spin-polls the ring for the whole run
//! (pin it with `shmem_core`) and stamps each observation right after the record passed
//! the seqlock check. `server_created_unix_ns` is the fast lane's publish time.

use solana_pubkey::Pubkey;
use std::{
    error::Error,
    io::{self, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, oneshot},
    task,
};
use tracing::{Level, info, warn};

use crate::{
    config::{Config, Endpoint},
    fastlane_ring::{FastlaneRingConsumer, KIND_ROLLBACK, PollResult, ResetReason},
    utils::{
        Comparator, ProgressTracker, TransactionData, open_log_file, unix_ns_to_secs, unix_time_ns,
        write_log_entry,
    },
};

use super::{
    GeyserProvider, ProviderContext,
    common::{TransactionAccumulator, fatal_connection_error},
};

pub struct FastlaneRingProvider;

impl GeyserProvider for FastlaneRingProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_fastlane_endpoint(endpoint, config, context).await })
    }
}

struct Observer {
    endpoint_name: String,
    start_wallclock_secs: f64,
    start_instant: Instant,
    comparator: Arc<Comparator>,
    shared_counter: Arc<AtomicUsize>,
    shared_shutdown: Arc<AtomicBool>,
    target_transactions: Option<usize>,
    total_producers: usize,
    progress: Option<Arc<ProgressTracker>>,
    shutdown_tx: broadcast::Sender<()>,
    log_file: Option<Box<dyn Write + Send>>,
    accumulator: TransactionAccumulator,
    tx_records: u64,
    matched: u64,
    markers: u64,
    rollbacks: u64,
}

impl Observer {
    fn record(
        &mut self,
        signature: String,
        wallclock_unix_ns: u64,
        elapsed: Duration,
        slot: u64,
        publish_ns: u64,
    ) -> io::Result<()> {
        let wallclock = unix_ns_to_secs(wallclock_unix_ns);
        if let Some(file) = self.log_file.as_mut() {
            write_log_entry(file, wallclock, &self.endpoint_name, &signature)?;
        }
        self.matched += 1;
        let tx_data = TransactionData {
            wallclock_secs: wallclock,
            wallclock_unix_ns,
            elapsed_since_start: elapsed,
            start_wallclock_secs: self.start_wallclock_secs,
            slot: Some(slot),
            server_created_unix_ns: (publish_ns > 0).then_some(publish_ns),
        };
        let updated = self.accumulator.record(signature.clone(), tx_data.clone());
        if updated
            && self.comparator.record_observation(
                &self.endpoint_name,
                &signature,
                tx_data,
                self.total_producers,
            )
            && let Some(target) = self.target_transactions
        {
            let shared = self.shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
            if let Some(tracker) = self.progress.as_ref() {
                tracker.record(shared);
            }
            if shared >= target && !self.shared_shutdown.swap(true, Ordering::AcqRel) {
                info!(endpoint = %self.endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                let _ = self.shutdown_tx.send(());
            }
        }
        Ok(())
    }

    fn finish(self, resets: u64, reopens: u64) {
        let unique_signatures = self.accumulator.len();
        self.comparator
            .add_batch(&self.endpoint_name, self.accumulator.into_inner());
        info!(
            endpoint = %self.endpoint_name,
            tx_records = self.tx_records,
            matched = self.matched,
            markers = self.markers,
            rollbacks = self.rollbacks,
            resets,
            reopens,
            unique_signatures,
            "Fast-lane ring reader stopped"
        );
    }
}

async fn process_fastlane_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let owner = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;
    let log_file = if tracing::enabled!(Level::TRACE) {
        Some(Box::new(open_log_file(&endpoint_name)?) as Box<dyn Write + Send>)
    } else {
        None
    };
    let observer = Observer {
        endpoint_name: endpoint_name.clone(),
        start_wallclock_secs,
        start_instant,
        comparator,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
        shutdown_tx,
        log_file,
        accumulator: TransactionAccumulator::new(),
        tx_records: 0,
        matched: 0,
        markers: 0,
        rollbacks: 0,
    };

    // `url` is the ring's filesystem path, e.g. /dev/shm/fastlane.out.ring.
    let ring_path = endpoint.url.clone();
    if ring_path.contains("://") {
        fatal_connection_error(
            &endpoint_name,
            format!(
                "fastlane_ring url must be the ring file path (e.g. /dev/shm/fastlane.out.ring), got {ring_path}"
            ),
        );
    }
    let path = PathBuf::from(&ring_path);
    info!(endpoint = %endpoint_name, path = %ring_path, owner = %owner, "Opening fast-lane ring");
    let consumer = FastlaneRingConsumer::open(&path)
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(
        endpoint = %endpoint_name,
        data_region_bytes = consumer.data_region_size(),
        core = ?endpoint.shmem_core,
        "Opened fast-lane ring; reading from the current write position"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, mut done_rx) = oneshot::channel();
    let thread_stop = Arc::clone(&stop);
    let core = endpoint.shmem_core;
    std::thread::Builder::new()
        .name("fastlane-ring".to_string())
        .spawn(move || {
            let _ = done_tx.send(run_ring_loop(
                consumer,
                observer,
                &path,
                owner.to_bytes(),
                &thread_stop,
                core,
            ));
        })?;

    let finished = tokio::select! {
        result = &mut done_rx => Some(result),
        _ = shutdown_rx.recv() => None,
    };
    let result = match finished {
        Some(result) => result,
        None => {
            info!(endpoint = %endpoint_name, "Received stop signal");
            stop.store(true, Ordering::Release);
            done_rx.await
        }
    };
    match result {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err("fast-lane ring reader thread exited without reporting a result".into()),
    }
}

fn inode(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.ino())
}

fn run_ring_loop(
    mut consumer: FastlaneRingConsumer,
    mut observer: Observer,
    path: &Path,
    owner: [u8; 32],
    stop: &AtomicBool,
    core: Option<usize>,
) -> io::Result<()> {
    if let Some(core) = core {
        if let Err(err) = super::shredstream_shmem::pin_current_thread(core) {
            fatal_connection_error(
                &observer.endpoint_name,
                format!("failed to pin fast-lane ring reader to core {core}: {err}"),
            );
        }
        info!(endpoint = %observer.endpoint_name, core, "Pinned fast-lane ring reader thread");
    }
    let mut ring_inode = inode(path);
    let mut resets = 0u64;
    let mut reopens = 0u64;
    let mut idle_polls = 0u64;
    let mut result = Ok(());
    while !stop.load(Ordering::Relaxed) {
        match consumer.poll(&owner) {
            PollResult::Tx(tx) => {
                idle_polls = 0;
                let wallclock_unix_ns = unix_time_ns();
                let elapsed = observer.start_instant.elapsed();
                observer.tx_records += 1;
                if !tx.owner_matched {
                    continue;
                }
                let signature = bs58::encode(tx.signature).into_string();
                if let Err(err) = observer.record(
                    signature,
                    wallclock_unix_ns,
                    elapsed,
                    tx.slot,
                    tx.t_publish_ns,
                ) {
                    result = Err(err);
                    break;
                }
            }
            PollResult::Marker(kind) => {
                idle_polls = 0;
                observer.markers += 1;
                if kind == KIND_ROLLBACK {
                    observer.rollbacks += 1;
                }
            }
            PollResult::Empty => {
                idle_polls += 1;
                // About once a second of idleness: a producer restart may have replaced
                // the file (new inode) instead of reusing it.
                if idle_polls % (1 << 24) == 0 {
                    let age_ms = unix_time_ns().saturating_sub(consumer.heartbeat_ns()) / 1_000_000;
                    if age_ms > 2_000 {
                        warn!(endpoint = %observer.endpoint_name, age_ms, "Fast-lane ring heartbeat is stale");
                    }
                }
                if idle_polls % (1 << 24) == 0 && inode(path) != ring_inode {
                    match FastlaneRingConsumer::open(path) {
                        Ok(reopened) => {
                            consumer = reopened;
                            ring_inode = inode(path);
                            reopens += 1;
                            warn!(endpoint = %observer.endpoint_name, reopens, "Fast-lane ring file replaced; reopened");
                        }
                        Err(err) => {
                            warn!(endpoint = %observer.endpoint_name, error = %err, "Fast-lane ring replaced but not readable yet");
                        }
                    }
                }
                std::hint::spin_loop();
            }
            PollResult::Reset(reason) => {
                resets += 1;
                if !resets.is_power_of_two() && reason == ResetReason::ProducerRestart {
                    continue;
                }
                warn!(
                    endpoint = %observer.endpoint_name,
                    reason = reason.as_str(),
                    resets,
                    "Fast-lane ring re-synced to the producer's write position"
                );
            }
        }
    }
    observer.finish(resets, reopens);
    result
}
