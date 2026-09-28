use futures_util::stream::StreamExt;
use std::{
    error::Error,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::broadcast, task};
use tracing::{Level, error, info, warn};

use solana_pubkey::Pubkey;

use crate::{
    config::{Config, Endpoint},
    entry_decode::{DecodeError, DecodedEntry, TransactionVersion, decode_entries},
    utils::{
        Comparator, ProgressTracker, TransactionData, open_log_file, unix_ns_to_secs, unix_time_ns,
        write_log_entry,
    },
};

use super::{
    GeyserProvider, ProviderContext,
    common::{GRPC_MAX_MESSAGE_SIZE, TransactionAccumulator, fatal_connection_error},
};

#[allow(clippy::all, dead_code)]
pub mod shredstream {
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub struct ShredstreamProvider;

impl GeyserProvider for ShredstreamProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_shredstream_endpoint(endpoint, config, context).await })
    }
}

async fn process_shredstream_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();
    let (mut observer, mut shutdown_rx) =
        EntryObserver::new(endpoint_name.clone(), account_pubkey, context)?;

    let endpoint_url = endpoint.url.clone();

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client = shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(
        endpoint_url.clone(),
    )
    .await
    .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err))
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);
    info!(endpoint = %endpoint_name, "Connected");

    let request = shredstream::SubscribeEntriesRequest {};
    let mut stream = client.subscribe_entries(request).await?.into_inner();

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => match message {
                Some(Ok(slot_entry)) => {
                    let decoded = decode_entries(&slot_entry.entries);
                    observer.observe_batch(slot_entry.slot, decoded)?;
                }
                Some(Err(err)) => {
                    error!(endpoint = %endpoint_name, error = ?err, "Error receiving message from stream");
                    break;
                }
                None => {
                    info!(endpoint = %endpoint_name, "Stream closed by server");
                    break;
                }
            }
        }
    }

    observer.finish();
    Ok(())
}

/// Turns decoded shredstream micro-batches into benchmark observations. Shared by the gRPC
/// (`shredstream`) and shared-memory (`shredstream_shmem`) transports so both apply the same
/// v1-aware decoder, account filter, and timestamp definition.
pub(super) struct EntryObserver {
    endpoint_name: String,
    account: Pubkey,
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
    batches: u64,
    decode_errors: u64,
    transactions_decoded: u64,
    v1_transactions: u64,
    matched_transactions: usize,
}

impl EntryObserver {
    /// Takes the provider context and hands back its shutdown receiver for the caller's loop.
    pub(super) fn new(
        endpoint_name: String,
        account: Pubkey,
        context: ProviderContext,
    ) -> io::Result<(Self, broadcast::Receiver<()>)> {
        let ProviderContext {
            shutdown_tx,
            shutdown_rx,
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
        Ok((
            Self {
                endpoint_name,
                account,
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
                batches: 0,
                decode_errors: 0,
                transactions_decoded: 0,
                v1_transactions: 0,
                matched_transactions: 0,
            },
            shutdown_rx,
        ))
    }

    pub(super) fn endpoint_name(&self) -> &str {
        &self.endpoint_name
    }

    /// Record every transaction in one decoded micro-batch whose static keys include the
    /// configured account. A micro-batch becomes readable all at once, so its transactions
    /// share one timestamp taken as soon as decoding finished, before any per-signature
    /// bookkeeping can delay later transactions in the same batch.
    pub(super) fn observe_batch(
        &mut self,
        slot: u64,
        decoded: Result<Vec<DecodedEntry>, DecodeError>,
    ) -> io::Result<()> {
        let wallclock_unix_ns = unix_time_ns();
        let elapsed = self.start_instant.elapsed();
        self.batches += 1;

        let entries = match decoded {
            Ok(entries) => entries,
            Err(err) => {
                self.decode_errors += 1;
                // Powers of two: every failure class stays visible without flooding the log.
                if self.decode_errors.is_power_of_two() {
                    warn!(
                        endpoint = %self.endpoint_name,
                        slot,
                        error = %err,
                        decode_errors = self.decode_errors,
                        batches = self.batches,
                        "Failed to decode shred entries; micro-batch dropped"
                    );
                }
                return Ok(());
            }
        };

        for entry in entries {
            for decoded in entry.transactions {
                self.transactions_decoded += 1;
                if decoded.version == TransactionVersion::V1 {
                    self.v1_transactions += 1;
                }
                let tx = decoded.transaction;
                if !tx.message.static_account_keys().contains(&self.account) {
                    continue;
                }
                let Some(signature) = tx.signatures.first() else {
                    continue;
                };
                self.record(signature.to_string(), wallclock_unix_ns, elapsed, slot)?;
            }
        }
        Ok(())
    }

    fn record(
        &mut self,
        signature: String,
        wallclock_unix_ns: u64,
        elapsed: Duration,
        slot: u64,
    ) -> io::Result<()> {
        let wallclock = unix_ns_to_secs(wallclock_unix_ns);
        if let Some(file) = self.log_file.as_mut() {
            write_log_entry(file, wallclock, &self.endpoint_name, &signature)?;
        }
        self.matched_transactions += 1;

        let tx_data = TransactionData {
            wallclock_secs: wallclock,
            wallclock_unix_ns,
            elapsed_since_start: elapsed,
            start_wallclock_secs: self.start_wallclock_secs,
            slot: Some(slot),
            server_created_unix_ns: None,
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

    pub(super) fn finish(self) {
        let unique_signatures = self.accumulator.len();
        self.comparator
            .add_batch(&self.endpoint_name, self.accumulator.into_inner());
        info!(
            endpoint = %self.endpoint_name,
            batches = self.batches,
            decode_errors = self.decode_errors,
            transactions_decoded = self.transactions_decoded,
            v1_transactions = self.v1_transactions,
            total_transactions = self.matched_transactions,
            unique_signatures,
            "Stream closed after dispatching transactions"
        );
    }
}
