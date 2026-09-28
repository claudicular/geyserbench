//! Read-only consumer of the agave fast lane's phase-3 output ring
//! (`/dev/shm/fastlane.out.ring`; agave fork branch `fast-lane`, `fast-lane/src/out_ring.rs`).
//!
//! Layout (little-endian): a 256-byte file header, then the data region. Header fields:
//! `magic "FLOUTv01"` at 0x00, `abi u32` at 0x08, `data_region_size u64` at 0x10,
//! `max_record u64` at 0x18, `generation u64` at 0x20 (changes on producer restart),
//! `write_pos u64` at 0x40, `write_seq u64` at 0x48, `heartbeat_ns u64` at 0x80.
//! Each record starts with a 64-byte header: `seq u64 | kind u16 | flags u16 | len u32 |
//! slot u64 | parent_slot u64 | tx_ordinal u32 | n_accounts u16 | incarnations u16 |
//! fork_id u64 | t_publish_ns u64 | t_source_ns u64`. A TX record (kind 1) continues with
//! `signature [64] | err u32 | cu u32` and `n_accounts` accounts of `pubkey [32] | owner [32]
//! | lamports u64 | data_len u32 | flags u8 | pad [3] | data`, each padded to 8 bytes.
//! Records are 8-aligned and never straddle the region end: when a record does not fit, the
//! producer writes `seq = 0` (if 8 bytes fit) and restarts at offset 0.
//!
//! The consumer only reads the header, signature and account owners in place; after that
//! it re-checks the record's sequence and the producer's position (seqlock), so a record the
//! producer may have started overwriting is reported as a reset, never as an observation.

use std::{
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    ptr,
    sync::atomic::{AtomicU64, Ordering, fence},
};

pub const MAGIC: u64 = u64::from_le_bytes(*b"FLOUTv01");
pub const ABI_VERSION: u32 = 1;
pub const HEADER_SIZE: usize = 256;
pub const RECORD_HEADER_SIZE: usize = 64;
pub const TX_BODY_FIXED: usize = 72;
pub const ACCOUNT_HEADER_SIZE: usize = 80;

pub const KIND_TX: u16 = 1;
#[allow(dead_code)]
pub const KIND_SLOT_BEGIN: u16 = 2;
#[allow(dead_code)]
pub const KIND_SLOT_END: u16 = 3;
pub const KIND_ROLLBACK: u16 = 4;

#[allow(dead_code)]
pub const FLAG_OK: u16 = 1;

const OFF_MAGIC: usize = 0x00;
const OFF_ABI: usize = 0x08;
const OFF_DATA_REGION_SIZE: usize = 0x10;
const OFF_MAX_RECORD: usize = 0x18;
const OFF_GENERATION: usize = 0x20;
const OFF_WRITE_POS: usize = 0x40;
const OFF_WRITE_SEQ: usize = 0x48;
const OFF_HEARTBEAT: usize = 0x80;

#[inline]
const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// One TX record, as far as the benchmark needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub slot: u64,
    pub tx_ordinal: u32,
    pub flags: u16,
    pub signature: [u8; 64],
    /// The fast lane's CLOCK_REALTIME when it wrote the record (like Yellowstone `created_at`).
    pub t_publish_ns: u64,
    /// When the fast lane received the transaction's data (proxy ring publish time, or
    /// agave's data-set completion).
    pub t_source_ns: u64,
    /// Some account of the record is owned by the consumer's owner filter.
    pub owner_matched: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetReason {
    /// The producer restarted (new generation) or the file is being re-created.
    ProducerRestart,
    /// The consumer fell more than a ring behind the producer.
    Lapped,
    /// The producer overwrote the record while it was read, or it was malformed.
    Torn,
}

impl ResetReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ResetReason::ProducerRestart => "producer_restart",
            ResetReason::Lapped => "lapped",
            ResetReason::Torn => "torn",
        }
    }
}

pub enum PollResult {
    Tx(TxRecord),
    /// SLOT_BEGIN / SLOT_END / ROLLBACK (the kind).
    Marker(u16),
    Empty,
    /// Positions were lost; the consumer re-synced to the producer's write position.
    Reset(ResetReason),
}

pub struct FastlaneRingConsumer {
    ptr: *const u8,
    len: usize,
    region: usize,
    max_record: u64,
    generation: u64,
    read_pos: u64,
    read_seq: u64,
}

// SAFETY: the consumer only reads the mapping and is used from one thread.
unsafe impl Send for FastlaneRingConsumer {}

impl FastlaneRingConsumer {
    /// Map the ring read-only; reading starts at the producer's current write position.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let len = file.metadata()?.len() as usize;
        if len < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fastlane ring too small",
            ));
        }
        // SAFETY: read-only shared mapping of a regular file; unmapped in Drop.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mut consumer = Self {
            ptr: ptr as *const u8,
            len,
            region: 0,
            max_record: 0,
            generation: 0,
            read_pos: 0,
            read_seq: 0,
        };
        if consumer.atomic(OFF_MAGIC).load(Ordering::Acquire) != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a fast-lane output ring (bad magic)",
            ));
        }
        let abi = consumer.read_u32(OFF_ABI);
        let region = consumer.read_u64(OFF_DATA_REGION_SIZE) as usize;
        if abi != ABI_VERSION || region == 0 || HEADER_SIZE + region > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported fast-lane ring (abi {abi}, region {region}, file {len})"),
            ));
        }
        consumer.region = region;
        consumer.max_record = consumer.read_u64(OFF_MAX_RECORD);
        consumer.generation = consumer.atomic(OFF_GENERATION).load(Ordering::Acquire);
        consumer.jump_to_head();
        Ok(consumer)
    }

    pub fn data_region_size(&self) -> usize {
        self.region
    }

    /// Producer liveness: CLOCK_REALTIME ns of its last record or idle tick (~1 ms).
    pub fn heartbeat_ns(&self) -> u64 {
        self.atomic(OFF_HEARTBEAT).load(Ordering::Relaxed)
    }

    fn atomic(&self, off: usize) -> &AtomicU64 {
        // SAFETY: 8-aligned offsets inside the mapping.
        unsafe { &*(self.ptr.add(off) as *const AtomicU64) }
    }

    fn read_u64(&self, off: usize) -> u64 {
        // SAFETY: in bounds of the mapping (callers pass header or checked record offsets).
        unsafe { ptr::read_unaligned(self.ptr.add(off) as *const u64) }
    }

    fn read_u32(&self, off: usize) -> u32 {
        // SAFETY: as above.
        unsafe { ptr::read_unaligned(self.ptr.add(off) as *const u32) }
    }

    fn read_u16(&self, off: usize) -> u16 {
        // SAFETY: as above.
        unsafe { ptr::read_unaligned(self.ptr.add(off) as *const u16) }
    }

    fn jump_to_head(&mut self) {
        self.read_seq = self.atomic(OFF_WRITE_SEQ).load(Ordering::Acquire);
        self.read_pos = self.atomic(OFF_WRITE_POS).load(Ordering::Acquire);
    }

    fn reset(&mut self, reason: ResetReason) -> PollResult {
        if reason == ResetReason::ProducerRestart {
            if self.atomic(OFF_MAGIC).load(Ordering::Acquire) != MAGIC {
                // Being re-created: try again later.
                return PollResult::Reset(reason);
            }
            self.generation = self.atomic(OFF_GENERATION).load(Ordering::Acquire);
        }
        self.jump_to_head();
        PollResult::Reset(reason)
    }

    /// Next record. `owner` is matched against the owners of a TX record's accounts.
    pub fn poll(&mut self, owner: &[u8; 32]) -> PollResult {
        if self.atomic(OFF_GENERATION).load(Ordering::Acquire) != self.generation
            || self.atomic(OFF_MAGIC).load(Ordering::Acquire) != MAGIC
        {
            return self.reset(ResetReason::ProducerRestart);
        }
        let write_pos = self.atomic(OFF_WRITE_POS).load(Ordering::Acquire);
        if self.read_pos >= write_pos {
            return PollResult::Empty;
        }
        if write_pos - self.read_pos > self.region as u64 {
            return self.reset(ResetReason::Lapped);
        }
        for _ in 0..2 {
            let off = (self.read_pos % self.region as u64) as usize;
            if off + RECORD_HEADER_SIZE > self.region {
                self.read_pos += (self.region - off) as u64;
                continue;
            }
            let base = HEADER_SIZE + off;
            let seq = self.atomic(base).load(Ordering::Acquire);
            if seq != self.read_seq + 1 {
                // The producer restarted at the region start here.
                self.read_pos += (self.region - off) as u64;
                continue;
            }
            let kind = self.read_u16(base + 8);
            let flags = self.read_u16(base + 10);
            let len = self.read_u32(base + 12) as usize;
            if len < RECORD_HEADER_SIZE || len % 8 != 0 || off + len > self.region {
                return self.reset(ResetReason::Torn);
            }
            let slot = self.read_u64(base + 16);
            let result = if kind == KIND_TX {
                match self.read_tx(base, len, owner) {
                    Some(mut tx) => {
                        tx.slot = slot;
                        tx.flags = flags;
                        PollResult::Tx(tx)
                    }
                    None => return self.reset(ResetReason::Torn),
                }
            } else {
                PollResult::Marker(kind)
            };
            fence(Ordering::Acquire);
            let seq_after = self.atomic(base).load(Ordering::Acquire);
            let write_pos_after = self.atomic(OFF_WRITE_POS).load(Ordering::Acquire);
            if seq_after != seq
                || write_pos_after.saturating_add(self.max_record)
                    > self.read_pos + self.region as u64
            {
                return self.reset(ResetReason::Torn);
            }
            self.read_pos += len as u64;
            self.read_seq = seq;
            return result;
        }
        PollResult::Empty
    }

    fn read_tx(&self, base: usize, len: usize, owner: &[u8; 32]) -> Option<TxRecord> {
        if len < RECORD_HEADER_SIZE + TX_BODY_FIXED {
            return None;
        }
        let body = base + RECORD_HEADER_SIZE;
        let end = base + len;
        let mut signature = [0u8; 64];
        // SAFETY: `body + 64 <= end`, inside the region (checked by the caller).
        unsafe { ptr::copy_nonoverlapping(self.ptr.add(body), signature.as_mut_ptr(), 64) };
        let n_accounts = self.read_u16(base + 36);
        let mut owner_matched = false;
        let mut o = body + TX_BODY_FIXED;
        for _ in 0..n_accounts {
            if o + ACCOUNT_HEADER_SIZE > end {
                return None;
            }
            let data_len = self.read_u32(o + 72) as usize;
            // SAFETY: `o + 64 <= end`.
            let account_owner = unsafe { std::slice::from_raw_parts(self.ptr.add(o + 32), 32) };
            if account_owner == owner {
                owner_matched = true;
            }
            o += ACCOUNT_HEADER_SIZE + align8(data_len);
            if o > end {
                return None;
            }
        }
        Some(TxRecord {
            slot: 0,
            tx_ordinal: self.read_u32(base + 32),
            flags: 0,
            signature,
            t_publish_ns: self.read_u64(base + 48),
            t_source_ns: self.read_u64(base + 56),
            owner_matched,
        })
    }
}

impl Drop for FastlaneRingConsumer {
    fn drop(&mut self) {
        // SAFETY: mapping created in `open`.
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
    }
}

#[cfg(test)]
pub(crate) mod test_writer {
    //! A writer with the fast lane's layout (`fast-lane/src/out_ring.rs`), for tests.
    use super::*;
    use std::io::Write;

    pub struct TestWriter {
        ptr: *mut u8,
        len: usize,
        region: usize,
        pos: u64,
        seq: u64,
    }

    impl TestWriter {
        pub fn create(path: &Path, region: usize, generation: u64) -> Self {
            let len = HEADER_SIZE + region;
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap();
            f.write_all(&vec![0u8; len]).unwrap();
            // SAFETY: shared mapping of the file just written.
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    f.as_raw_fd(),
                    0,
                )
            } as *mut u8;
            let w = Self {
                ptr,
                len,
                region,
                pos: 0,
                seq: 0,
            };
            w.set_header(generation);
            w
        }

        pub fn set_header(&self, generation: u64) {
            // SAFETY: header offsets inside the mapping.
            unsafe {
                ptr::write_unaligned(self.ptr.add(OFF_ABI) as *mut u32, ABI_VERSION);
                ptr::write_unaligned(self.ptr.add(0x0c) as *mut u32, HEADER_SIZE as u32);
                ptr::write_unaligned(
                    self.ptr.add(OFF_DATA_REGION_SIZE) as *mut u64,
                    self.region as u64,
                );
                ptr::write_unaligned(
                    self.ptr.add(OFF_MAX_RECORD) as *mut u64,
                    (self.region / 8) as u64,
                );
                (*(self.ptr.add(OFF_GENERATION) as *const AtomicU64))
                    .store(generation, Ordering::SeqCst);
                (*(self.ptr.add(OFF_MAGIC) as *const AtomicU64)).store(MAGIC, Ordering::SeqCst);
            }
        }

        /// Publish raw record bytes (header included, `seq` field overwritten).
        pub fn publish(&mut self, record: &[u8]) {
            assert_eq!(record.len() % 8, 0);
            let mut off = (self.pos % self.region as u64) as usize;
            if off + record.len() > self.region {
                if off + 8 <= self.region {
                    // SAFETY: in bounds.
                    unsafe {
                        (*(self.ptr.add(HEADER_SIZE + off) as *const AtomicU64))
                            .store(0, Ordering::Relaxed)
                    };
                }
                self.pos += (self.region - off) as u64;
                off = 0;
            }
            self.seq += 1;
            // SAFETY: [off, off + len) inside the region.
            unsafe {
                let p = self.ptr.add(HEADER_SIZE + off);
                (*(p as *const AtomicU64)).store(0, Ordering::Relaxed);
                ptr::copy_nonoverlapping(record.as_ptr().add(8), p.add(8), record.len() - 8);
                (*(p as *const AtomicU64)).store(self.seq, Ordering::Release);
                self.pos += record.len() as u64;
                (*(self.ptr.add(OFF_WRITE_SEQ) as *const AtomicU64))
                    .store(self.seq, Ordering::Release);
                (*(self.ptr.add(OFF_WRITE_POS) as *const AtomicU64))
                    .store(self.pos, Ordering::Release);
            }
        }
    }

    impl Drop for TestWriter {
        fn drop(&mut self) {
            // SAFETY: mapping created in `create`.
            unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
        }
    }

    /// A TX record like the fast lane writes, with accounts of the given owners.
    pub fn tx_record(slot: u64, ordinal: u32, sig: [u8; 64], owners: &[[u8; 32]]) -> Vec<u8> {
        let mut r = vec![0u8; RECORD_HEADER_SIZE];
        r[8..10].copy_from_slice(&KIND_TX.to_le_bytes());
        r[10..12].copy_from_slice(&FLAG_OK.to_le_bytes());
        r[16..24].copy_from_slice(&slot.to_le_bytes());
        r[32..36].copy_from_slice(&ordinal.to_le_bytes());
        r[36..38].copy_from_slice(&(owners.len() as u16).to_le_bytes());
        r[48..56].copy_from_slice(&7u64.to_le_bytes());
        r.extend_from_slice(&sig);
        r.extend_from_slice(&[0u8; 8]);
        for owner in owners {
            let mut a = vec![0u8; ACCOUNT_HEADER_SIZE];
            a[32..64].copy_from_slice(owner);
            a[72..76].copy_from_slice(&165u32.to_le_bytes());
            r.extend_from_slice(&a);
            r.extend_from_slice(&[0u8; 168]);
        }
        let len = r.len() as u32;
        r[12..16].copy_from_slice(&len.to_le_bytes());
        r
    }
}

#[cfg(test)]
mod tests {
    use super::{test_writer::*, *};

    const TOKEN: [u8; 32] = [
        6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133,
        237, 95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
    ];

    /// Bytes of one TX record exactly as the fast lane writes it (`test_abi_golden` in
    /// agave `fast-lane/src/out_ring.rs`; t_publish_ns zeroed): slot 12345, parent 12344,
    /// ordinal 5, flags OK|FROM_RING, incarnations 2, fork 7, t_source 42, signature
    /// [1; 64], cu 1234, one written Token-program account [2; 32] with 128 lamports and
    /// data aa bb cc.
    const GOLDEN_TX: &str = concat!(
        "010000000000000001000900e000000039300000000000003830000000000000",
        "0500000001000200070000000000000000000000000000002a00000000000000",
        "0101010101010101010101010101010101010101010101010101010101010101",
        "0101010101010101010101010101010101010101010101010101010101010101",
        "00000000d2040000020202020202020202020202020202020202020202020202",
        "020202020202020206ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b3791",
        "3a8cf5857eff00a980000000000000000300000001000000aabbcc0000000000",
    );

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn parses_the_fast_lane_golden_record() {
        let dir = std::env::temp_dir().join(format!("fl_ring_golden_{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        let mut w = TestWriter::create(&dir, 1 << 16, 1);
        let mut c = FastlaneRingConsumer::open(&dir).unwrap();
        assert!(matches!(c.poll(&TOKEN), PollResult::Empty));
        let golden = hex(GOLDEN_TX);
        assert_eq!(golden.len(), 224);
        w.publish(&golden);
        match c.poll(&TOKEN) {
            PollResult::Tx(tx) => {
                assert_eq!(tx.slot, 12345);
                assert_eq!(tx.tx_ordinal, 5);
                assert_eq!(tx.flags, 9);
                assert_eq!(tx.signature, [1; 64]);
                assert_eq!(tx.t_source_ns, 42);
                assert!(tx.owner_matched);
            }
            _ => panic!("tx expected"),
        }
        w.publish(&golden);
        match c.poll(&[9; 32]) {
            PollResult::Tx(tx) => assert!(!tx.owner_matched),
            _ => panic!("tx expected"),
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn reads_in_order_across_wraps_and_resets_when_lapped() {
        let path = std::env::temp_dir().join(format!("fl_ring_wrap_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut w = TestWriter::create(&path, 1 << 16, 1);
        let mut c = FastlaneRingConsumer::open(&path).unwrap();
        let mut got = 0u32;
        for i in 0..5000u32 {
            let owners: Vec<[u8; 32]> = (0..(i % 5))
                .map(|j| if j == 1 { TOKEN } else { [3; 32] })
                .collect();
            let mut sig = [0u8; 64];
            sig[..4].copy_from_slice(&i.to_le_bytes());
            w.publish(&tx_record(100, i, sig, &owners));
            if i % 7 == 3 {
                let mut marker = vec![0u8; RECORD_HEADER_SIZE];
                marker[8..10].copy_from_slice(&KIND_SLOT_END.to_le_bytes());
                marker[12..16].copy_from_slice(&(RECORD_HEADER_SIZE as u32).to_le_bytes());
                w.publish(&marker);
            }
            loop {
                match c.poll(&TOKEN) {
                    PollResult::Tx(tx) => {
                        assert_eq!(tx.tx_ordinal, got);
                        assert_eq!(&tx.signature[..4], &got.to_le_bytes());
                        assert_eq!(tx.owner_matched, got % 5 >= 2);
                        got += 1;
                    }
                    PollResult::Marker(kind) => assert_eq!(kind, KIND_SLOT_END),
                    PollResult::Empty => break,
                    PollResult::Reset(reason) => panic!("reset {reason:?}"),
                }
            }
        }
        assert_eq!(got, 5000);
        // Lapped: publish more than a ring's worth without reading.
        for i in 0..2000u32 {
            w.publish(&tx_record(101, i, [0; 64], &[TOKEN]));
        }
        assert!(matches!(
            c.poll(&TOKEN),
            PollResult::Reset(ResetReason::Lapped)
        ));
        assert!(matches!(c.poll(&TOKEN), PollResult::Empty));
        // Producer restart: new generation.
        w.set_header(2);
        assert!(matches!(
            c.poll(&TOKEN),
            PollResult::Reset(ResetReason::ProducerRestart)
        ));
        let _ = std::fs::remove_file(&path);
    }
}
