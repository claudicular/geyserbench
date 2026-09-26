//! `shredstream_shmem`: reads the shredstream-proxy shared-memory entry ring the production
//! bot consumes (`SHREDSTREAM_SHMEM_PATH`), instead of the proxy's gRPC `SubscribeEntries`.
//!
//! Like the bot's shred thread, a dedicated OS thread spin-polls the ring and decodes each
//! micro-batch in place with the same v1-aware decoder, so the observation time matches when
//! the bot could first act on a transaction. The thread occupies one CPU for the whole run;
//! `shmem_core` pins it (Linux only) away from the validator and bot cores.

use solana_pubkey::Pubkey;
use std::{
    error::Error,
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::oneshot, task};
use tracing::{info, warn};

use crate::{
    config::{Config, Endpoint},
    entry_decode::decode_entries,
    shmem_ring::{PollResult, ShmemRingConsumer},
};

use super::{
    GeyserProvider, ProviderContext, common::fatal_connection_error, shredstream::EntryObserver,
};

pub struct ShredstreamShmemProvider;

impl GeyserProvider for ShredstreamShmemProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_shmem_endpoint(endpoint, config, context).await })
    }
}

async fn process_shmem_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();
    let (observer, mut shutdown_rx) =
        EntryObserver::new(endpoint_name.clone(), account_pubkey, context)?;

    // `url` is the ring's filesystem path, e.g. /dev/shm/shredstream.ring.
    let ring_path = endpoint.url.clone();
    if ring_path.contains("://") {
        fatal_connection_error(
            &endpoint_name,
            format!(
                "shredstream_shmem url must be the ring file path (e.g. /dev/shm/shredstream.ring), got {ring_path}"
            ),
        );
    }
    info!(endpoint = %endpoint_name, path = %ring_path, "Opening shmem ring");
    let consumer = ShmemRingConsumer::open(Path::new(&ring_path))
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(
        endpoint = %endpoint_name,
        data_region_bytes = consumer.data_region_size(),
        core = ?endpoint.shmem_core,
        "Opened shmem ring; reading from the current write position"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, mut done_rx) = oneshot::channel();
    let thread_stop = Arc::clone(&stop);
    let core = endpoint.shmem_core;
    std::thread::Builder::new()
        .name("shmem-ring".to_string())
        .spawn(move || {
            let _ = done_tx.send(run_ring_loop(consumer, observer, &thread_stop, core));
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
        Err(_) => Err("shmem ring reader thread exited without reporting a result".into()),
    }
}

fn run_ring_loop(
    mut consumer: ShmemRingConsumer,
    mut observer: EntryObserver,
    stop: &AtomicBool,
    core: Option<usize>,
) -> io::Result<()> {
    if let Some(core) = core {
        if let Err(err) = pin_current_thread(core) {
            fatal_connection_error(
                observer.endpoint_name(),
                format!("failed to pin shmem reader to core {core}: {err}"),
            );
        }
        info!(endpoint = %observer.endpoint_name(), core, "Pinned shmem reader thread");
    }

    let mut resets = 0u64;
    let mut overwritten = 0u64;
    let mut result = Ok(());
    while !stop.load(Ordering::Relaxed) {
        let (slot, pos, decoded) = match consumer.poll() {
            PollResult::Entry(entry) => {
                (entry.slot, entry.pos, decode_entries(entry.entries_bytes))
            }
            PollResult::Empty => {
                std::hint::spin_loop();
                continue;
            }
            PollResult::Reset(reason) => {
                resets += 1;
                warn!(
                    endpoint = %observer.endpoint_name(),
                    reason = reason.as_str(),
                    resets,
                    "Shmem ring re-synced to the producer's write position"
                );
                continue;
            }
        };
        if !consumer.is_intact(pos) {
            overwritten += 1;
            warn!(
                endpoint = %observer.endpoint_name(),
                slot,
                overwritten,
                "Dropped a micro-batch the producer may have overwritten while it was decoded"
            );
            continue;
        }
        if let Err(err) = observer.observe_batch(slot, decoded) {
            result = Err(err);
            break;
        }
    }

    info!(
        endpoint = %observer.endpoint_name(),
        resets,
        overwritten,
        "Shmem ring reader stopped"
    );
    observer.finish();
    result
}

#[cfg(target_os = "linux")]
fn pin_current_thread(core: usize) -> io::Result<()> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "core pinning is only supported on Linux",
    ))
}
