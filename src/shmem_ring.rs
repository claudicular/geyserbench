//! Read-only consumer of the shredstream-proxy shared-memory entry ring.
//!
//! Layout is byte-identical with the proxy's `ShmemRingProducer` (validator-benching branch)
//! and arb_bot's `src/shmem_ring.rs` consumer: a 128-byte header followed by the data region;
//! each record is `seq(8) + slot(8) + data_len(4) + pad(4)` then the bincode `Vec<Entry>`
//! bytes, 8-byte aligned, never straddling the end of the region.
//!
//! The producer never reads consumer state, so any number of processes may map the ring
//! read-only next to the production bot. Two guards are added over the bot's reader, both
//! needed because records are decoded in place while the producer keeps writing:
//! - a record header whose length would run past the data region is treated as torn;
//! - `is_intact` re-checks the published write position after decoding (seqlock style), so
//!   a record the producer may have started overwriting is discarded instead of reported.

use std::{
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    ptr,
    sync::atomic::{AtomicU64, Ordering, fence},
};

const MAGIC: u64 = 0x534852494E474246; // "SHRINGBF"
const HEADER_SIZE: usize = 128;
const ENTRY_HEADER_SIZE: usize = 24; // seq(8) + slot(8) + data_len(4) + pad(4)

#[inline]
const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

#[repr(C)]
struct RingHeader {
    magic: u64,            // 0x00
    version: u32,          // 0x08
    header_size: u32,      // 0x0C
    data_region_size: u32, // 0x10
    max_entry_size: u32,   // 0x14
    producer_pid: u64,     // 0x18
    created_epoch_ns: u64, // 0x20
    _pad0: [u8; 24],       // 0x28..0x3F
    write_pos: AtomicU64,  // 0x40
    write_seq: AtomicU64,  // 0x48
    _pad1: [u8; 48],       // 0x50..0x7F
}

const _: () = assert!(std::mem::size_of::<RingHeader>() == HEADER_SIZE);

pub struct ShmemRingConsumer {
    mmap_ptr: *const u8,
    mmap_len: usize,
    data_region_size: usize,
    /// Largest distance the producer can write past its published position while a record
    /// is in flight: one wrap skip plus one maximum-size record.
    overwrite_margin: u64,
    local_read_pos: u64,
    local_read_seq: u64,
    created_epoch_ns: u64,
}

// SAFETY: the consumer only reads the mapping and is used from one thread.
unsafe impl Send for ShmemRingConsumer {}

pub struct EntryRef<'a> {
    pub slot: u64,
    /// Absolute stream position of the record; pass to `is_intact` after decoding.
    pub pos: u64,
    pub entries_bytes: &'a [u8],
}

pub enum PollResult<'a> {
    Entry(EntryRef<'a>),
    Empty,
    /// Producer restarted, the consumer was lapped, or a torn record header was seen.
    /// The consumer has re-synced to the current write position.
    Reset(ResetReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetReason {
    ProducerRestart,
    Lapped,
    TornHeader,
}

impl ResetReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProducerRestart => "producer_restart",
            Self::Lapped => "lapped",
            Self::TornHeader => "torn_header",
        }
    }
}

impl ShmemRingConsumer {
    /// Map an existing ring read-only, starting at the current write position (no history).
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for ring header",
            ));
        }

        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                file_len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mmap_ptr = mmap_ptr as *const u8;
        let unmap = || unsafe {
            libc::munmap(mmap_ptr as *mut libc::c_void, file_len);
        };

        let header = unsafe { &*(mmap_ptr as *const RingHeader) };
        // Copy out before any early return: `header` dangles once the mapping is gone.
        let magic = header.magic;
        let data_region_size = header.data_region_size as usize;
        if magic != MAGIC {
            unmap();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid magic: expected {:#x}, got {:#x}", MAGIC, magic),
            ));
        }
        if data_region_size == 0 || HEADER_SIZE + data_region_size > file_len {
            unmap();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data region {} B does not fit a {} B file",
                    data_region_size, file_len
                ),
            ));
        }
        let max_record = align8(ENTRY_HEADER_SIZE + header.max_entry_size as usize) as u64;

        Ok(Self {
            mmap_ptr,
            mmap_len: file_len,
            data_region_size,
            overwrite_margin: 2 * max_record,
            local_read_pos: header.write_pos.load(Ordering::Acquire),
            local_read_seq: header.write_seq.load(Ordering::Acquire),
            created_epoch_ns: header.created_epoch_ns,
        })
    }

    pub fn data_region_size(&self) -> usize {
        self.data_region_size
    }

    /// The header lives in the mapping, not in `self`; the unbounded lifetime lets callers
    /// keep reading it while updating the consumer's own cursor.
    #[inline]
    fn header<'a>(&self) -> &'a RingHeader {
        unsafe { &*(self.mmap_ptr as *const RingHeader) }
    }

    fn resync(&mut self, reason: ResetReason) -> PollResult<'_> {
        let header = self.header();
        let write_pos = header.write_pos.load(Ordering::Acquire);
        let write_seq = header.write_seq.load(Ordering::Acquire);
        self.local_read_pos = write_pos;
        self.local_read_seq = write_seq;
        PollResult::Reset(reason)
    }

    pub fn poll(&mut self) -> PollResult<'_> {
        let header = self.header();
        let created_epoch_ns = header.created_epoch_ns;
        if created_epoch_ns != self.created_epoch_ns {
            self.created_epoch_ns = created_epoch_ns;
            return self.resync(ResetReason::ProducerRestart);
        }

        let current_write_pos = header.write_pos.load(Ordering::Acquire);
        if self.local_read_pos >= current_write_pos {
            return PollResult::Empty;
        }
        if current_write_pos - self.local_read_pos > self.data_region_size as u64 {
            return self.resync(ResetReason::Lapped);
        }

        let region_offset = (self.local_read_pos as usize) % self.data_region_size;
        if region_offset + ENTRY_HEADER_SIZE > self.data_region_size {
            // Too close to the end for any record: the producer skipped to the boundary.
            self.local_read_pos += (self.data_region_size - region_offset) as u64;
            return self.poll();
        }
        let entry_ptr = unsafe { self.mmap_ptr.add(HEADER_SIZE + region_offset) };
        let entry_seq = unsafe { ptr::read_unaligned(entry_ptr as *const u64) };
        if entry_seq != self.local_read_seq + 1 {
            // Wrap padding: the next record starts at the region boundary.
            self.local_read_pos += (self.data_region_size - region_offset) as u64;
            return self.poll();
        }

        let slot = unsafe { ptr::read_unaligned(entry_ptr.add(8) as *const u64) };
        let data_len = unsafe { ptr::read_unaligned(entry_ptr.add(16) as *const u32) } as usize;
        if region_offset + ENTRY_HEADER_SIZE + data_len > self.data_region_size {
            return self.resync(ResetReason::TornHeader);
        }
        let entries_bytes =
            unsafe { std::slice::from_raw_parts(entry_ptr.add(ENTRY_HEADER_SIZE), data_len) };

        let pos = self.local_read_pos;
        self.local_read_pos += align8(ENTRY_HEADER_SIZE + data_len) as u64;
        self.local_read_seq = entry_seq;

        PollResult::Entry(EntryRef {
            slot,
            pos,
            entries_bytes,
        })
    }

    /// True when the producer cannot have begun overwriting the record at `pos` before the
    /// caller finished reading it. Call after decoding; a `false` result means the decoded
    /// data may be torn and must be dropped.
    #[inline]
    pub fn is_intact(&self, pos: u64) -> bool {
        fence(Ordering::Acquire);
        let write_pos = self.header().write_pos.load(Ordering::Acquire);
        write_pos.saturating_add(self.overwrite_margin) <= pos + self.data_region_size as u64
    }
}

impl Drop for ShmemRingConsumer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    const TEST_REGION: usize = 4096;
    const TEST_MAX_ENTRY: usize = 512;

    /// Minimal writer mirroring the proxy's `ShmemRingProducer::publish`.
    struct TestProducer {
        path: PathBuf,
        file: fs::File,
        write_pos: u64,
        write_seq: u64,
    }

    impl TestProducer {
        fn create() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "geyserbench_shmem_ring_{}_{}",
                std::process::id(),
                NEXT.fetch_add(1, AtomicOrdering::Relaxed)
            ));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            let mut header = vec![0u8; HEADER_SIZE + TEST_REGION];
            header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
            header[8..12].copy_from_slice(&1u32.to_le_bytes());
            header[12..16].copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
            header[16..20].copy_from_slice(&(TEST_REGION as u32).to_le_bytes());
            header[20..24].copy_from_slice(&(TEST_MAX_ENTRY as u32).to_le_bytes());
            header[32..40].copy_from_slice(&42u64.to_le_bytes());
            file.write_all(&header).unwrap();
            file.flush().unwrap();
            Self {
                path,
                file,
                write_pos: 0,
                write_seq: 0,
            }
        }

        fn write_at(&mut self, offset: usize, bytes: &[u8]) {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(bytes, offset as u64).unwrap();
        }

        fn publish(&mut self, slot: u64, data: &[u8]) {
            let total = align8(ENTRY_HEADER_SIZE + data.len());
            let offset = (self.write_pos as usize) % TEST_REGION;
            if offset + total > TEST_REGION {
                self.write_pos += (TEST_REGION - offset) as u64;
            }
            let offset = HEADER_SIZE + (self.write_pos as usize) % TEST_REGION;
            self.write_seq += 1;
            let mut record = Vec::with_capacity(total);
            record.extend_from_slice(&self.write_seq.to_le_bytes());
            record.extend_from_slice(&slot.to_le_bytes());
            record.extend_from_slice(&(data.len() as u32).to_le_bytes());
            record.extend_from_slice(&0u32.to_le_bytes());
            record.extend_from_slice(data);
            self.write_at(offset, &record);
            self.write_pos += total as u64;
            self.write_at(0x48, &self.write_seq.to_le_bytes());
            self.write_at(0x40, &self.write_pos.to_le_bytes());
        }
    }

    impl Drop for TestProducer {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn next_entry(consumer: &mut ShmemRingConsumer) -> Option<(u64, Vec<u8>)> {
        match consumer.poll() {
            PollResult::Entry(entry) => Some((entry.slot, entry.entries_bytes.to_vec())),
            PollResult::Empty => None,
            PollResult::Reset(reason) => panic!("unexpected reset: {reason:?}"),
        }
    }

    #[test]
    fn reads_records_in_order_across_the_wrap_boundary() {
        let mut producer = TestProducer::create();
        producer.publish(1, b"before-open");
        let mut consumer = ShmemRingConsumer::open(&producer.path).unwrap();
        // History before open is skipped.
        assert!(next_entry(&mut consumer).is_none());

        let mut expected = Vec::new();
        for i in 0..40u64 {
            let data = vec![i as u8; 100 + (i as usize * 37) % 300];
            producer.publish(1000 + i, &data);
            expected.push((1000 + i, data));
            // Drain every few records so the consumer never falls a full region behind.
            if i % 3 == 2 {
                for (slot, data) in expected.drain(..) {
                    let (got_slot, got) = next_entry(&mut consumer).unwrap();
                    assert_eq!(got_slot, slot);
                    assert_eq!(got, data);
                }
            }
        }
        for (slot, data) in expected.drain(..) {
            let (got_slot, got) = next_entry(&mut consumer).unwrap();
            assert_eq!(got_slot, slot);
            assert_eq!(got, data);
        }
        assert!(next_entry(&mut consumer).is_none());
    }

    #[test]
    fn lapped_consumer_resyncs_and_intact_check_rejects_overwritten_records() {
        let mut producer = TestProducer::create();
        let mut consumer = ShmemRingConsumer::open(&producer.path).unwrap();
        producer.publish(7, &[1u8; 200]);
        let pos = match consumer.poll() {
            PollResult::Entry(entry) => entry.pos,
            _ => panic!("expected an entry"),
        };
        assert!(consumer.is_intact(pos));
        // Advance the producer far enough that the record's bytes may be rewritten.
        for i in 0..20 {
            producer.publish(8 + i, &[2u8; 300]);
        }
        assert!(!consumer.is_intact(pos));
        assert!(matches!(
            consumer.poll(),
            PollResult::Reset(ResetReason::Lapped)
        ));
        producer.publish(99, b"fresh");
        let (slot, data) = next_entry(&mut consumer).unwrap();
        assert_eq!((slot, data.as_slice()), (99, &b"fresh"[..]));
    }

    #[test]
    fn producer_restart_is_detected() {
        let mut producer = TestProducer::create();
        let mut consumer = ShmemRingConsumer::open(&producer.path).unwrap();
        producer.write_at(32, &43u64.to_le_bytes());
        assert!(matches!(
            consumer.poll(),
            PollResult::Reset(ResetReason::ProducerRestart)
        ));
        producer.publish(5, b"after-restart");
        assert_eq!(next_entry(&mut consumer).unwrap().0, 5);
    }

    #[test]
    fn rejects_files_that_are_not_rings() {
        let path =
            std::env::temp_dir().join(format!("geyserbench_not_a_ring_{}", std::process::id()));
        fs::write(&path, vec![0u8; HEADER_SIZE + 64]).unwrap();
        assert!(ShmemRingConsumer::open(&path).is_err());
        let _ = fs::remove_file(&path);
    }
}
