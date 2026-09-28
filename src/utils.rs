use dashmap::{DashMap, DashSet};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub wallclock_secs: f64,
    pub elapsed_since_start: Duration,
    pub start_wallclock_secs: f64,
    pub slot: Option<u64>,
}

/// Snapshot emitted when a signature has been observed by every endpoint.
#[derive(Debug, Clone)]
pub struct CompleteObservation {
    pub signature: String,
    pub observations: HashMap<String, TransactionData>,
}

#[derive(Debug)]
pub struct Comparator {
    data: DashMap<String, HashMap<String, TransactionData>>,
    emitted: DashSet<String>,
    sink: Mutex<Option<UnboundedSender<CompleteObservation>>>,
}

impl Comparator {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            emitted: DashSet::new(),
            sink: Mutex::new(None),
        }
    }

    pub fn set_sink(&self, sender: UnboundedSender<CompleteObservation>) {
        *self.sink.lock().unwrap() = Some(sender);
    }

    /// Drops the sink sender so the receiving task can drain and exit.
    pub fn close_sink(&self) {
        self.sink.lock().unwrap().take();
    }

    pub fn add_batch(&self, from: &str, transactions: HashMap<String, TransactionData>) {
        for (signature, data) in transactions {
            let mut entry = self.data.entry(signature).or_default();
            entry.insert(from.to_owned(), data);
        }
    }

    /// Records an observation; returns true when this observation completed
    /// the signature (all producers have now reported it, first time).
    pub fn record_observation(
        &self,
        endpoint: &str,
        signature: &str,
        data: TransactionData,
        expected_producers: usize,
    ) -> bool {
        if expected_producers == 0 {
            return false;
        }

        let mut entry = self.data.entry(signature.to_owned()).or_default();

        let mut updated = false;
        entry
            .entry(endpoint.to_owned())
            .and_modify(|existing| {
                if data.elapsed_since_start < existing.elapsed_since_start {
                    *existing = data.clone();
                    updated = true;
                }
            })
            .or_insert_with(|| {
                updated = true;
                data.clone()
            });

        if !updated {
            return false;
        }

        if entry.len() != expected_producers {
            return false;
        }

        let snapshot = entry.clone();
        drop(entry);

        if self.emitted.insert(signature.to_owned()) {
            if let Some(sender) = self.sink.lock().unwrap().as_ref() {
                let _ = sender.send(CompleteObservation {
                    signature: signature.to_owned(),
                    observations: snapshot,
                });
            }
            true
        } else {
            false
        }
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, HashMap<String, TransactionData>> {
        self.data.iter()
    }
}

#[derive(Debug)]
pub struct ProgressTracker {
    target: usize,
    next_checkpoint: AtomicUsize,
}

impl ProgressTracker {
    pub fn new(target: usize) -> Self {
        Self {
            target,
            next_checkpoint: AtomicUsize::new(5),
        }
    }

    pub fn record(&self, current: usize) {
        if self.target == 0 {
            return;
        }

        let percent = (current.saturating_mul(100)) / self.target.max(1);

        loop {
            let next = self.next_checkpoint.load(Ordering::Acquire);
            if next > 100 {
                break;
            }

            if percent < next {
                break;
            }

            if self
                .next_checkpoint
                .compare_exchange(next, next + 5, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let clamped = next.min(100);
                info!(
                    progress = %format!("{}%", clamped),
                    current,
                    target = self.target,
                );
                break;
            }
        }
    }
}

/// Env var naming the optional per-(endpoint, signature) CSV written at the end of a run.
pub const SIG_CSV_ENV: &str = "GEYSERBENCH_SIG_CSV";

/// Writes every (endpoint, signature) observation held by the comparator as
/// `endpoint,signature,slot,elapsed_ns,wallclock_secs`, sorted by signature then endpoint.
/// `elapsed_ns` is the monotonic `elapsed_since_start` the comparator ranks by.
/// Called once after all providers finished, never on the receive path.
pub fn write_signature_csv(comparator: &Comparator, path: &Path) -> std::io::Result<usize> {
    let mut rows: Vec<(String, String, Option<u64>, u128, f64)> = Vec::new();
    for entry in comparator.iter() {
        for (endpoint, data) in entry.value() {
            rows.push((
                entry.key().clone(),
                endpoint.clone(),
                data.slot,
                data.elapsed_since_start.as_nanos(),
                data.wallclock_secs,
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut out = BufWriter::new(File::create(path)?);
    writeln!(out, "endpoint,signature,slot,elapsed_ns,wallclock_secs")?;
    for (signature, endpoint, slot, elapsed_ns, wallclock_secs) in &rows {
        let slot = slot.map(|slot| slot.to_string()).unwrap_or_default();
        writeln!(
            out,
            "{},{},{},{},{:.6}",
            csv_field(endpoint),
            signature,
            slot,
            elapsed_ns,
            wallclock_secs
        )?;
    }
    out.flush()?;
    Ok(rows.len())
}

fn csv_field(value: &str) -> std::borrow::Cow<'_, str> {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\"")).into()
    } else {
        value.into()
    }
}

pub fn get_current_timestamp() -> f64 {
    let now = SystemTime::now();
    let since_epoch: Duration = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(e) => {
            // System clock went backwards; log and clamp to 0
            warn!("SystemTime error (clock skew): {}", e);
            Duration::from_secs(0)
        }
    };
    since_epoch.as_secs_f64()
}

pub fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

pub fn open_log_file(name: &str) -> std::io::Result<impl Write + use<>> {
    let safe_name = sanitize_filename(name);
    let log_filename = format!("transaction_log_{}.txt", safe_name);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
}

pub fn write_log_entry(
    file: &mut impl Write,
    timestamp: f64,
    endpoint_name: &str,
    signature: &str,
) -> std::io::Result<()> {
    let log_entry = format!("[{:.3}] [{}] {}\n", timestamp, endpoint_name, signature);
    file.write_all(log_entry.as_bytes())
}

fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    let trimmed = sanitized.trim_matches('.');
    if trimmed.is_empty() {
        "endpoint".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Comparator, TransactionData, write_signature_csv};
    use std::{collections::HashMap, time::Duration};

    fn observation(elapsed_ns: u64, slot: Option<u64>) -> TransactionData {
        TransactionData {
            wallclock_secs: 1_790_000_000.25,
            elapsed_since_start: Duration::from_nanos(elapsed_ns),
            start_wallclock_secs: 1_790_000_000.0,
            slot,
        }
    }

    #[test]
    fn signature_csv_has_one_row_per_endpoint_observation() {
        let comparator = Comparator::new();
        comparator.add_batch(
            "deshred",
            HashMap::from([
                ("sigB".to_string(), observation(2_000, Some(11))),
                ("sigA".to_string(), observation(1_500, Some(10))),
            ]),
        );
        comparator.add_batch(
            "grpc, \"fra\"",
            HashMap::from([("sigA".to_string(), observation(9_000, None))]),
        );

        let path = std::env::temp_dir().join(format!(
            "geyserbench_sig_csv_test_{}.csv",
            std::process::id()
        ));
        let rows = write_signature_csv(&comparator, &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(rows, 3);
        assert_eq!(
            content,
            "endpoint,signature,slot,elapsed_ns,wallclock_secs\n\
             deshred,sigA,10,1500,1790000000.250000\n\
             \"grpc, \"\"fra\"\"\",sigA,,9000,1790000000.250000\n\
             deshred,sigB,11,2000,1790000000.250000\n"
        );
    }
}
