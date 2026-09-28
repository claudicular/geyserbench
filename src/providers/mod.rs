use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Instant,
};
use tokio::sync::broadcast;

use crate::{
    config::{Config, Endpoint, EndpointKind},
    utils::{Comparator, ProgressTracker},
};

pub mod arpc;
pub mod common;
pub mod fastlane_ring;
pub mod influxdb;
pub mod jetstream;
pub mod raiden_pulse;
pub mod shreder;
pub mod shredstream;
pub mod shredstream_shmem;
pub mod thor;
pub mod yellowstone;
mod yellowstone_client;
pub mod yellowstone_deshred;
pub mod yellowstone_tx_accounts;

pub trait GeyserProvider: Send + Sync {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
}

pub fn create_provider(kind: &EndpointKind) -> Box<dyn GeyserProvider> {
    match kind {
        EndpointKind::Yellowstone => Box::new(yellowstone::YellowstoneProvider),
        EndpointKind::YellowstoneTxAccounts => {
            Box::new(yellowstone_tx_accounts::YellowstoneTxAccountsProvider)
        }
        EndpointKind::YellowstoneDeshred => {
            Box::new(yellowstone_deshred::YellowstoneDeshredProvider)
        }
        EndpointKind::Arpc => Box::new(arpc::ArpcProvider),
        EndpointKind::Thor => Box::new(thor::ThorProvider),
        EndpointKind::Shreder => Box::new(shreder::ShrederProvider),
        EndpointKind::RaidenPulse => Box::new(raiden_pulse::RaidenPulseProvider),
        EndpointKind::Shredstream => Box::new(shredstream::ShredstreamProvider),
        EndpointKind::ShredstreamShmem => Box::new(shredstream_shmem::ShredstreamShmemProvider),
        EndpointKind::FastlaneRing => Box::new(fastlane_ring::FastlaneRingProvider),
        EndpointKind::Jetstream => Box::new(jetstream::JetstreamProvider),
        EndpointKind::Influxdb => Box::new(influxdb::InfluxdbProvider),
    }
}

pub struct ProviderContext {
    pub shutdown_tx: broadcast::Sender<()>,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub start_wallclock_secs: f64,
    pub start_instant: Instant,
    pub comparator: Arc<Comparator>,
    pub shared_counter: Arc<AtomicUsize>,
    pub shared_shutdown: Arc<AtomicBool>,
    pub target_transactions: Option<usize>,
    pub total_producers: usize,
    pub progress: Option<Arc<ProgressTracker>>,
}
