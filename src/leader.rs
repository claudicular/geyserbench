//! Leader-schedule resolution (slot -> leader identity pubkey) and validator
//! location classification from an externally supplied RTT map.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::ValidatorMapSettings;

const RPC_RETRY_BACKOFF: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Leader resolver
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct EpochAlignment {
    epoch: u64,
    first_slot: u64,
    slots_in_epoch: u64,
}

impl EpochAlignment {
    /// Epoch and first absolute slot for `slot`, assuming constant epoch length
    /// (true on mainnet since the warmup period ended).
    fn locate(&self, slot: u64) -> (u64, u64) {
        if slot >= self.first_slot {
            let offset_epochs = (slot - self.first_slot) / self.slots_in_epoch;
            (
                self.epoch + offset_epochs,
                self.first_slot + offset_epochs * self.slots_in_epoch,
            )
        } else {
            let back = (self.first_slot - slot).div_ceil(self.slots_in_epoch);
            (
                self.epoch.saturating_sub(back),
                self.first_slot.saturating_sub(back * self.slots_in_epoch),
            )
        }
    }
}

struct EpochLeaders {
    first_slot: u64,
    /// Index into `leaders` per relative slot; `u32::MAX` marks unknown slots.
    slot_leader_idx: Vec<u32>,
    leaders: Vec<Arc<str>>,
}

impl EpochLeaders {
    fn leader_for(&self, slot: u64) -> Option<Arc<str>> {
        let rel = slot.checked_sub(self.first_slot)? as usize;
        let idx = *self.slot_leader_idx.get(rel)?;
        self.leaders.get(idx as usize).cloned()
    }
}

#[derive(Default)]
struct ResolverState {
    alignment: Option<EpochAlignment>,
    epochs: HashMap<u64, Arc<EpochLeaders>>,
    last_failure: Option<Instant>,
}

pub struct LeaderResolver {
    client: Client,
    rpc_url: String,
    state: RwLock<ResolverState>,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EpochInfo {
    absolute_slot: u64,
    epoch: u64,
    slot_index: u64,
    slots_in_epoch: u64,
}

impl LeaderResolver {
    pub fn new(rpc_url: String) -> Self {
        Self {
            client: Client::new(),
            rpc_url,
            state: RwLock::new(ResolverState::default()),
        }
    }

    async fn rpc_call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, String> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("{method} request failed: {err}"))?;
        if !response.status().is_success() {
            return Err(format!("{method} returned status {}", response.status()));
        }
        let parsed: RpcResponse<T> = response
            .json()
            .await
            .map_err(|err| format!("{method} response parse failed: {err}"))?;
        if let Some(error) = parsed.error {
            return Err(format!("{method} RPC error: {error}"));
        }
        parsed
            .result
            .ok_or_else(|| format!("{method} returned empty result"))
    }

    async fn fetch_alignment(&self) -> Result<EpochAlignment, String> {
        let info: EpochInfo = self.rpc_call("getEpochInfo", json!([])).await?;
        Ok(EpochAlignment {
            epoch: info.epoch,
            first_slot: info.absolute_slot - info.slot_index,
            slots_in_epoch: info.slots_in_epoch,
        })
    }

    async fn fetch_epoch_leaders(
        &self,
        epoch: u64,
        first_slot: u64,
        slots_in_epoch: u64,
    ) -> Result<EpochLeaders, String> {
        let schedule: HashMap<String, Vec<u64>> = self
            .rpc_call("getLeaderSchedule", json!([first_slot, {}]))
            .await?;
        let mut leaders: Vec<Arc<str>> = Vec::with_capacity(schedule.len());
        let mut slot_leader_idx = vec![u32::MAX; slots_in_epoch as usize];
        for (pubkey, rel_slots) in schedule {
            let idx = leaders.len() as u32;
            leaders.push(Arc::from(pubkey.as_str()));
            for rel in rel_slots {
                if let Some(cell) = slot_leader_idx.get_mut(rel as usize) {
                    *cell = idx;
                }
            }
        }
        info!(
            epoch,
            first_slot,
            leaders = leaders.len(),
            "Fetched leader schedule"
        );
        Ok(EpochLeaders {
            first_slot,
            slot_leader_idx,
            leaders,
        })
    }

    /// Warm the cache for the current epoch. Failures are logged, not fatal.
    pub async fn prefetch_current(&self) {
        if let Some(leader) = self.ensure_epoch_for_slot(None).await {
            info!(current_leader = %leader, "Leader resolver ready");
        }
    }

    /// Resolve the leader for `slot`, fetching the containing epoch's schedule
    /// on first use. Returns None if the RPC is unavailable.
    pub async fn resolve(&self, slot: u64) -> Option<Arc<str>> {
        {
            let state = self.state.read().await;
            if let Some(alignment) = state.alignment {
                let (epoch, _) = alignment.locate(slot);
                if let Some(leaders) = state.epochs.get(&epoch) {
                    return leaders.leader_for(slot);
                }
            }
        }
        self.ensure_epoch_for_slot(Some(slot)).await
    }

    /// Resolve leaders for a set of slots (used at analysis time).
    pub async fn resolve_many(
        &self,
        slots: impl IntoIterator<Item = u64>,
    ) -> HashMap<u64, Arc<str>> {
        let mut resolved = HashMap::new();
        let unique: HashSet<u64> = slots.into_iter().collect();
        for slot in unique {
            if let Some(leader) = self.resolve(slot).await {
                resolved.insert(slot, leader);
            }
        }
        resolved
    }

    /// Fetch (with backoff on failure) whatever is needed to answer `slot`;
    /// `None` means "current slot" and is used for prefetch.
    async fn ensure_epoch_for_slot(&self, slot: Option<u64>) -> Option<Arc<str>> {
        let mut state = self.state.write().await;

        if let Some(failed_at) = state.last_failure
            && failed_at.elapsed() < RPC_RETRY_BACKOFF
        {
            return None;
        }

        if state.alignment.is_none() {
            match self.fetch_alignment().await {
                Ok(alignment) => state.alignment = Some(alignment),
                Err(err) => {
                    warn!(rpc_url = %self.rpc_url, error = %err, "Leader schedule alignment fetch failed");
                    state.last_failure = Some(Instant::now());
                    return None;
                }
            }
        }

        let alignment = state.alignment?;
        let target_slot = slot.unwrap_or(alignment.first_slot);
        let (epoch, first_slot) = alignment.locate(target_slot);

        // Check-then-insert instead of the entry API: the fetch between the
        // two is async and must not hold a map borrow across the await.
        #[allow(clippy::map_entry)]
        if !state.epochs.contains_key(&epoch) {
            match self
                .fetch_epoch_leaders(epoch, first_slot, alignment.slots_in_epoch)
                .await
            {
                Ok(leaders) => {
                    state.epochs.insert(epoch, Arc::new(leaders));
                    state.last_failure = None;
                }
                Err(err) => {
                    warn!(epoch, error = %err, "Leader schedule fetch failed");
                    state.last_failure = Some(Instant::now());
                    return None;
                }
            }
        }

        state
            .epochs
            .get(&epoch)
            .and_then(|leaders| leaders.leader_for(target_slot))
    }
}

// ---------------------------------------------------------------------------
// Validator RTT map (PersistedRttMap v1) + in-region classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Region {
    In,
    Out,
    Unknown,
}

impl Region {
    pub fn as_str(&self) -> &'static str {
        match self {
            Region::In => "in",
            Region::Out => "out",
            Region::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Deserialize)]
struct PersistedRttMap {
    #[serde(default)]
    #[allow(dead_code)]
    version: Option<u32>,
    entries: HashMap<String, RttEntry>,
}

#[derive(Debug, Deserialize)]
struct RttEntry {
    #[serde(default)]
    measurement: Option<Measurement>,
    /// Optional producer-side override; when present it wins over the local
    /// RTT-threshold predicate.
    #[serde(default)]
    in_region: Option<bool>,
}

#[derive(Debug, Deserialize)]
enum Measurement {
    Icmp {
        rtt_us: u64,
    },
    QuicTpu {
        rtt_us: u64,
        #[serde(default)]
        shared_relay: bool,
        #[serde(default)]
        offbox: bool,
    },
    Unreachable,
}

#[derive(Default)]
struct MapInner {
    in_region: HashMap<String, bool>,
    loaded_at: Option<Instant>,
}

pub struct ValidatorMap {
    settings: ValidatorMapSettings,
    client: Client,
    inner: std::sync::RwLock<MapInner>,
}

impl ValidatorMap {
    pub fn new(settings: ValidatorMapSettings) -> Self {
        Self {
            settings,
            client: Client::new(),
            inner: std::sync::RwLock::new(MapInner::default()),
        }
    }

    fn entry_in_region(&self, entry: &RttEntry) -> bool {
        if let Some(flag) = entry.in_region {
            return flag;
        }
        match &entry.measurement {
            Some(Measurement::Icmp { rtt_us }) => *rtt_us < self.settings.rtt_icmp_threshold_us,
            Some(Measurement::QuicTpu {
                rtt_us,
                shared_relay,
                offbox,
            }) => *rtt_us < self.settings.quic_threshold_us() && !shared_relay && !offbox,
            Some(Measurement::Unreachable) | None => false,
        }
    }

    async fn fetch_raw(&self) -> Result<String, String> {
        let source = self.settings.source.as_str();
        if source.starts_with("http://") || source.starts_with("https://") {
            let response = self
                .client
                .get(source)
                .send()
                .await
                .map_err(|err| format!("fetch {source}: {err}"))?;
            if !response.status().is_success() {
                return Err(format!("fetch {source}: status {}", response.status()));
            }
            response
                .text()
                .await
                .map_err(|err| format!("fetch {source}: {err}"))
        } else {
            tokio::fs::read_to_string(source)
                .await
                .map_err(|err| format!("read {source}: {err}"))
        }
    }

    /// Load (or reload) the map. Failures are logged and leave the previous
    /// contents in place.
    pub async fn load(&self) -> bool {
        let raw = match self.fetch_raw().await {
            Ok(raw) => raw,
            Err(err) => {
                warn!(error = %err, "Validator map load failed");
                return false;
            }
        };
        let parsed: PersistedRttMap = match serde_json::from_str(&raw) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(error = %err, "Validator map parse failed");
                return false;
            }
        };

        let in_region: HashMap<String, bool> = parsed
            .entries
            .iter()
            .map(|(pubkey, entry)| (pubkey.clone(), self.entry_in_region(entry)))
            .collect();
        let in_count = in_region.values().filter(|flag| **flag).count();
        info!(
            validators = in_region.len(),
            in_region = in_count,
            source = %self.settings.source,
            "Validator map loaded"
        );

        let mut inner = self.inner.write().unwrap();
        inner.in_region = in_region;
        inner.loaded_at = Some(Instant::now());
        true
    }

    /// Reload if the map is older than `refresh_secs`. Used by long runs.
    pub async fn maybe_refresh(&self) {
        let stale = {
            let inner = self.inner.read().unwrap();
            match inner.loaded_at {
                Some(at) => at.elapsed() >= Duration::from_secs(self.settings.refresh_secs.max(1)),
                None => true,
            }
        };
        if stale {
            self.load().await;
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.inner.read().unwrap().loaded_at.is_some()
    }

    pub fn classify(&self, leader: &str) -> Region {
        let inner = self.inner.read().unwrap();
        if inner.loaded_at.is_none() {
            return Region::Unknown;
        }
        match inner.in_region.get(leader) {
            Some(true) => Region::In,
            Some(false) => Region::Out,
            None => Region::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> ValidatorMapSettings {
        ValidatorMapSettings {
            source: "unused".to_string(),
            rtt_icmp_threshold_us: 5_000,
            rtt_quic_threshold_us: None,
            refresh_secs: 300,
        }
    }

    fn entry(json: &str) -> RttEntry {
        serde_json::from_str(json).expect("entry parses")
    }

    #[test]
    fn epoch_alignment_locates_slots() {
        let alignment = EpochAlignment {
            epoch: 700,
            first_slot: 302_400_000,
            slots_in_epoch: 432_000,
        };
        assert_eq!(alignment.locate(302_400_000), (700, 302_400_000));
        assert_eq!(alignment.locate(302_400_001), (700, 302_400_000));
        assert_eq!(alignment.locate(302_832_000), (701, 302_832_000));
        assert_eq!(alignment.locate(302_399_999), (699, 301_968_000));
    }

    #[test]
    fn rtt_predicate_matches_policy() {
        let map = ValidatorMap::new(settings());
        assert!(map.entry_in_region(&entry(r#"{"measurement":{"Icmp":{"rtt_us":420}}}"#)));
        assert!(!map.entry_in_region(&entry(r#"{"measurement":{"Icmp":{"rtt_us":5000}}}"#)));
        assert!(map.entry_in_region(&entry(
            r#"{"measurement":{"QuicTpu":{"rtt_us":900,"shared_relay":false,"offbox":false}}}"#
        )));
        assert!(!map.entry_in_region(&entry(
            r#"{"measurement":{"QuicTpu":{"rtt_us":900,"shared_relay":true,"offbox":false}}}"#
        )));
        assert!(!map.entry_in_region(&entry(
            r#"{"measurement":{"QuicTpu":{"rtt_us":900,"shared_relay":false,"offbox":true}}}"#
        )));
        assert!(!map.entry_in_region(&entry(r#"{"measurement":"Unreachable"}"#)));
        // Producer-side override wins over the local predicate.
        assert!(map.entry_in_region(&entry(r#"{"measurement":"Unreachable","in_region":true}"#)));
    }

    /// Live-network check against mainnet RPC; run manually with
    /// `cargo test resolves_live_leader -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn resolves_live_leader() {
        let resolver = LeaderResolver::new("https://api.mainnet-beta.solana.com".to_string());
        resolver.prefetch_current().await;
        let alignment = resolver
            .state
            .read()
            .await
            .alignment
            .expect("alignment fetched");
        let leader = resolver.resolve(alignment.first_slot + 1).await;
        assert!(leader.is_some(), "leader resolved for current epoch slot");
        let previous = resolver.resolve(alignment.first_slot - 1).await;
        assert!(previous.is_some(), "leader resolved across epoch boundary");
    }

    #[test]
    fn persisted_map_parses_v1_shape() {
        let raw = r#"{
            "version": 1,
            "written_at_unix": 1753228800,
            "bot_name": "fra",
            "entries": {
                "Va1idator111111111111111111111111111111111": {
                    "gossip_ip": "1.2.3.4",
                    "tpu_quic": "1.2.3.4:8002",
                    "measurement": {"Icmp": {"rtt_us": 420}},
                    "measured_at_unix": 1753228800,
                    "prev_gossip_ip": null
                },
                "Va1idator222222222222222222222222222222222": {
                    "gossip_ip": "5.6.7.8",
                    "measurement": "Unreachable"
                }
            }
        }"#;
        let parsed: PersistedRttMap = serde_json::from_str(raw).expect("map parses");
        assert_eq!(parsed.entries.len(), 2);
    }
}
