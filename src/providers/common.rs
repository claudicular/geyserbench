use std::collections::HashMap;

use tracing::error;

use crate::utils::TransactionData;

/// Max gRPC decode/encode message size for every tonic client built here.
/// tonic's 4 MiB default ends the stream with `OutOfRange` on large updates
/// (one grouped `transaction_accounts` message has been seen at ~8 MiB).
/// The thor provider is excluded: its external client crate does not expose the limit.
pub const GRPC_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Default)]
pub struct TransactionAccumulator {
    entries: HashMap<String, TransactionData>,
}

impl TransactionAccumulator {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn record(&mut self, signature: String, data: TransactionData) -> bool {
        use std::collections::hash_map::Entry;

        match self.entries.entry(signature) {
            Entry::Vacant(entry) => {
                entry.insert(data);
                true
            }
            Entry::Occupied(mut entry) => {
                if data.elapsed_since_start < entry.get().elapsed_since_start {
                    entry.insert(data);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn into_inner(self) -> HashMap<String, TransactionData> {
        self.entries
    }
}

/// Yellowstone `created_at` in nanoseconds since the Unix epoch. The plugin stamps it
/// (`SystemTime::now()`) when it builds the message in the geyser callback, so the
/// receive wallclock minus this value is plugin + network delivery time.
/// `None` when absent, unset (zero), or out of range.
pub fn timestamp_unix_ns(created_at: Option<&prost_types::Timestamp>) -> Option<u64> {
    let created_at = created_at?;
    let seconds = u64::try_from(created_at.seconds).ok()?;
    let nanos = u64::try_from(created_at.nanos)
        .ok()
        .filter(|&nanos| nanos < 1_000_000_000)?;
    seconds
        .checked_mul(1_000_000_000)?
        .checked_add(nanos)
        .filter(|&unix_ns| unix_ns > 0)
}

pub fn fatal_connection_error(endpoint: &str, err: impl std::fmt::Display) -> ! {
    error!(endpoint = endpoint, error = %err, "Failed to connect to endpoint");
    eprintln!("Failed to connect to endpoint {}: {}", endpoint, err);
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::timestamp_unix_ns;
    use prost_types::Timestamp;

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp { seconds, nanos }
    }

    #[test]
    fn created_at_converts_to_unix_ns() {
        assert_eq!(
            timestamp_unix_ns(Some(&ts(1_790_000_000, 123_456_789))),
            Some(1_790_000_000_123_456_789)
        );
        assert_eq!(timestamp_unix_ns(None), None);
        assert_eq!(timestamp_unix_ns(Some(&ts(0, 0))), None);
        assert_eq!(timestamp_unix_ns(Some(&ts(-1, 0))), None);
        assert_eq!(timestamp_unix_ns(Some(&ts(1, -1))), None);
        assert_eq!(timestamp_unix_ns(Some(&ts(1, 1_000_000_000))), None);
        assert_eq!(timestamp_unix_ns(Some(&ts(i64::MAX, 0))), None);
    }
}
