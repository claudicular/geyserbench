//! Version-tolerant decoder for shredstream micro-batches (`Vec<Entry>` in bincode layout).
//!
//! Ported from arb_bot `src/integrations/shredstream/entry_decode.rs` so both shred providers
//! decode exactly like the production bot's shred thread. The only difference is that
//! signature verification (`verify_with_results`) is omitted; the benchmark never needs it.
//!
//! Why this exists: `solana-entry`'s bincode/wincode schemas only know legacy and v0 messages.
//! Since SIMD-0385 (transaction v1, mainnet epoch 1035, 2026-09-15 01:04 UTC) a single v1
//! transaction anywhere in a micro-batch made `bincode::deserialize::<Vec<Entry>>` fail, and
//! the whole batch was dropped, every legacy/v0 transaction in it included. This module
//! decodes legacy, v0, and v1 transactions from the same byte stream.
//!
//! Wire layout (byte-exact with Agave 4.x `solana-transaction` / `solana-entry`):
//! - `Vec<Entry>`: u64 LE count; each entry = u64 LE `num_hashes`, 32-byte hash, u64 LE
//!   transaction count, then the transactions back to back.
//! - Legacy/v0 transaction: ShortU16 signature count (always one byte `< 0x80`), the
//!   64-byte signatures, then the message. The first message byte is `num_required_signatures`
//!   (`< 0x80`) for legacy or the `0x80` version prefix for v0.
//! - V1 transaction: `0x81`, the SIMD-0385 message (legacy header, u32 LE config mask, 32-byte
//!   lifetime specifier, u8 instruction count, u8 address count, addresses, config values,
//!   4-byte instruction headers, instruction payloads), then exactly
//!   `num_required_signatures` 64-byte signatures with no length prefix. Each signature covers
//!   the bytes from the `0x81` prefix through the last instruction payload.
//!
//! Representation: legacy/v0 decode to exactly the `VersionedTransaction` the old path
//! produced. A v1 transaction has no address-table lookups, so it is re-expressed as a
//! `VersionedMessage::Legacy` carrying the same header, inline keys, lifetime specifier and
//! compiled instructions; every existing consumer (static keys, instruction decoding, LUT
//! resolution, signature-based routing) keeps working unchanged. The original signed byte
//! range and the v1 compute config are retained next to it because re-serializing the
//! synthesized legacy message would not reproduce the signed bytes.

// Kept identical to the bot's decoder; the benchmark reads only part of the decoded output.
#![allow(dead_code)]

use std::fmt;
use std::ops::Range;

use solana_hash::Hash;
use solana_message::compiled_instruction::CompiledInstruction;
use solana_message::v0::MessageAddressTableLookup;
use solana_message::{MessageHeader, VersionedMessage, legacy, v0};
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;

/// High bit of the first message byte marks a versioned message.
const MESSAGE_VERSION_PREFIX: u8 = 0x80;
const V0_PREFIX: u8 = MESSAGE_VERSION_PREFIX;
/// SIMD-0385 transaction/message version byte (decimal 129).
pub const V1_PREFIX: u8 = MESSAGE_VERSION_PREFIX | 1;
const SIGNATURE_BYTES: usize = 64;
const KEY_BYTES: usize = 32;
const HASH_BYTES: usize = 32;
/// Smallest possible entry: num_hashes + hash + zero transactions.
const MIN_ENTRY_BYTES: usize = 8 + HASH_BYTES + 8;

/// Priority fee occupies two bits and both must be set (u64 LE value).
const V1_MASK_PRIORITY_FEE: u32 = 0b11;
const V1_MASK_COMPUTE_UNIT_LIMIT: u32 = 0b100;
const V1_MASK_LOADED_ACCOUNTS_DATA_SIZE: u32 = 0b1000;
const V1_MASK_HEAP_SIZE: u32 = 0b1_0000;
const V1_MASK_KNOWN_BITS: u32 = V1_MASK_PRIORITY_FEE
    | V1_MASK_COMPUTE_UNIT_LIMIT
    | V1_MASK_LOADED_ACCOUNTS_DATA_SIZE
    | V1_MASK_HEAP_SIZE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionVersion {
    Legacy,
    V0,
    V1,
}

impl TransactionVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::V0 => "v0",
            Self::V1 => "v1",
        }
    }
}

/// Compute configuration carried in a v1 message header instead of ComputeBudget instructions.
/// `priority_fee` is a total in lamports, not a per-compute-unit price.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V1TransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct DecodedTransaction {
    /// Legacy/v0: the transaction exactly as serialized. V1: signatures as serialized plus a
    /// synthesized legacy message (inline keys, no lookups) with the same content.
    pub transaction: VersionedTransaction,
    pub version: TransactionVersion,
    /// Byte range of the signed message inside the buffer the transaction was decoded from.
    /// Legacy/v0: the message bytes. V1: the `0x81` prefix through the instruction payloads.
    pub signed_message: Range<usize>,
    /// Present only for v1 transactions.
    pub v1_config: Option<V1TransactionConfig>,
}

#[derive(Clone, Debug)]
pub struct DecodedEntry {
    pub num_hashes: u64,
    pub hash: Hash,
    pub transactions: Vec<DecodedTransaction>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof {
        at: usize,
        needed: usize,
        remaining: usize,
    },
    ShortU16 {
        at: usize,
        reason: &'static str,
    },
    /// First transaction byte is neither a one-byte signature count nor the v1 prefix.
    InvalidDiscriminator {
        at: usize,
        byte: u8,
    },
    /// A versioned message prefix this decoder does not know (or v1 in the legacy/v0 slot).
    UnsupportedMessageVersion {
        at: usize,
        version: u8,
    },
    /// Unknown config bits (their payload size is unknowable) or a partial priority-fee pair.
    InvalidV1ConfigMask {
        at: usize,
        mask: u32,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof {
                at,
                needed,
                remaining,
            } => write!(
                f,
                "unexpected end of entry bytes at {at}: needed {needed}, remaining {remaining}"
            ),
            Self::ShortU16 { at, reason } => write!(f, "invalid ShortU16 at {at}: {reason}"),
            Self::InvalidDiscriminator { at, byte } => {
                write!(f, "invalid transaction discriminator {byte:#04x} at {at}")
            }
            Self::UnsupportedMessageVersion { at, version } => {
                write!(f, "unsupported message version {version} at {at}")
            }
            Self::InvalidV1ConfigMask { at, mask } => {
                write!(f, "invalid v1 transaction config mask {mask:#010b} at {at}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    #[inline(always)]
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        match self.pos.checked_add(n) {
            Some(end) if end <= self.bytes.len() => {
                let slice = &self.bytes[self.pos..end];
                self.pos = end;
                Ok(slice)
            }
            _ => Err(DecodeError::UnexpectedEof {
                at: self.pos,
                needed: n,
                remaining: self.remaining(),
            }),
        }
    }

    #[inline(always)]
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let slice = self.take(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(slice);
        Ok(out)
    }

    #[inline(always)]
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    #[inline(always)]
    fn u32_le(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array::<4>()?))
    }

    #[inline(always)]
    fn u64_le(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array::<8>()?))
    }

    /// Solana `ShortU16` (compact-u16): up to three bytes, seven payload bits each,
    /// little-endian; the same validation as `solana-short-vec` (no non-minimal encodings,
    /// third byte must terminate, value must fit in u16).
    fn short_u16(&mut self) -> Result<usize, DecodeError> {
        let at = self.pos;
        let mut value: u32 = 0;
        for nth in 0..3usize {
            let byte = self.u8()?;
            if nth != 0 && byte == 0 {
                return Err(DecodeError::ShortU16 {
                    at,
                    reason: "non-minimal encoding",
                });
            }
            let done = byte & 0x80 == 0;
            if nth == 2 && !done {
                return Err(DecodeError::ShortU16 {
                    at,
                    reason: "third byte continues",
                });
            }
            value |= u32::from(byte & 0x7f) << (nth * 7);
            if value > u32::from(u16::MAX) {
                return Err(DecodeError::ShortU16 {
                    at,
                    reason: "value exceeds u16",
                });
            }
            if done {
                return Ok(value as usize);
            }
        }
        unreachable!("ShortU16 loop always returns on the third byte")
    }

    #[inline(always)]
    fn short_bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.short_u16()?;
        Ok(self.take(len)?.to_vec())
    }

    /// Preallocation guard: never trust a declared count beyond what the remaining bytes
    /// could possibly hold.
    #[inline(always)]
    fn bounded_capacity(&self, count: usize, min_elem_bytes: usize) -> usize {
        count.min(self.remaining() / min_elem_bytes.max(1))
    }
}

#[inline(always)]
fn read_header_after_first(
    cursor: &mut Cursor<'_>,
    num_required_signatures: u8,
) -> Result<MessageHeader, DecodeError> {
    Ok(MessageHeader {
        num_required_signatures,
        num_readonly_signed_accounts: cursor.u8()?,
        num_readonly_unsigned_accounts: cursor.u8()?,
    })
}

/// One bounds check for the whole fixed-width array, then a straight chunked copy into the
/// output (the compiler lowers this to a memcpy-like loop, no per-element cursor bookkeeping).
#[inline(always)]
fn read_keys(cursor: &mut Cursor<'_>, count: usize) -> Result<Vec<Pubkey>, DecodeError> {
    let raw = cursor.take(count * KEY_BYTES)?;
    let mut keys = Vec::with_capacity(count);
    keys.extend(raw.chunks_exact(KEY_BYTES).map(|chunk| {
        let mut bytes = [0u8; KEY_BYTES];
        bytes.copy_from_slice(chunk);
        Pubkey::new_from_array(bytes)
    }));
    Ok(keys)
}

#[inline(always)]
fn read_signatures(cursor: &mut Cursor<'_>, count: usize) -> Result<Vec<Signature>, DecodeError> {
    let raw = cursor.take(count * SIGNATURE_BYTES)?;
    let mut signatures = Vec::with_capacity(count);
    signatures.extend(raw.chunks_exact(SIGNATURE_BYTES).map(|chunk| {
        let mut bytes = [0u8; SIGNATURE_BYTES];
        bytes.copy_from_slice(chunk);
        Signature::from(bytes)
    }));
    Ok(signatures)
}

/// ShortU16-prefixed `CompiledInstruction` list (legacy and v0 layout).
fn read_short_vec_instructions(
    cursor: &mut Cursor<'_>,
) -> Result<Vec<CompiledInstruction>, DecodeError> {
    let count = cursor.short_u16()?;
    let mut instructions = Vec::with_capacity(cursor.bounded_capacity(count, 3));
    for _ in 0..count {
        let program_id_index = cursor.u8()?;
        let accounts = cursor.short_bytes()?;
        let data = cursor.short_bytes()?;
        instructions.push(CompiledInstruction {
            program_id_index,
            accounts,
            data,
        });
    }
    Ok(instructions)
}

fn read_legacy_message(
    cursor: &mut Cursor<'_>,
    num_required_signatures: u8,
) -> Result<legacy::Message, DecodeError> {
    let header = read_header_after_first(cursor, num_required_signatures)?;
    let key_count = cursor.short_u16()?;
    let account_keys = read_keys(cursor, key_count)?;
    let recent_blockhash = Hash::new_from_array(cursor.array::<HASH_BYTES>()?);
    let instructions = read_short_vec_instructions(cursor)?;
    Ok(legacy::Message {
        header,
        account_keys,
        recent_blockhash,
        instructions,
    })
}

fn read_v0_message(cursor: &mut Cursor<'_>) -> Result<v0::Message, DecodeError> {
    let first = cursor.u8()?;
    let header = read_header_after_first(cursor, first)?;
    let key_count = cursor.short_u16()?;
    let account_keys = read_keys(cursor, key_count)?;
    let recent_blockhash = Hash::new_from_array(cursor.array::<HASH_BYTES>()?);
    let instructions = read_short_vec_instructions(cursor)?;
    let lookup_count = cursor.short_u16()?;
    let mut address_table_lookups =
        Vec::with_capacity(cursor.bounded_capacity(lookup_count, KEY_BYTES + 2));
    for _ in 0..lookup_count {
        let account_key = Pubkey::new_from_array(cursor.array::<KEY_BYTES>()?);
        let writable_indexes = cursor.short_bytes()?;
        let readonly_indexes = cursor.short_bytes()?;
        address_table_lookups.push(MessageAddressTableLookup {
            account_key,
            writable_indexes,
            readonly_indexes,
        });
    }
    Ok(v0::Message {
        header,
        account_keys,
        recent_blockhash,
        instructions,
        address_table_lookups,
    })
}

/// SIMD-0385 message body (everything after the `0x81` prefix, before the signatures),
/// returned as an equivalent legacy message plus the header-carried compute config.
fn read_v1_message(
    cursor: &mut Cursor<'_>,
) -> Result<(legacy::Message, V1TransactionConfig), DecodeError> {
    let first = cursor.u8()?;
    let header = read_header_after_first(cursor, first)?;
    let mask_at = cursor.pos;
    let mask = cursor.u32_le()?;
    let priority_fee_bits = mask & V1_MASK_PRIORITY_FEE;
    if mask & !V1_MASK_KNOWN_BITS != 0
        || (priority_fee_bits != 0 && priority_fee_bits != V1_MASK_PRIORITY_FEE)
    {
        return Err(DecodeError::InvalidV1ConfigMask { at: mask_at, mask });
    }
    let lifetime_specifier = Hash::new_from_array(cursor.array::<HASH_BYTES>()?);
    let num_instructions = usize::from(cursor.u8()?);
    let num_addresses = usize::from(cursor.u8()?);
    let account_keys = read_keys(cursor, num_addresses)?;

    let mut config = V1TransactionConfig::default();
    if priority_fee_bits == V1_MASK_PRIORITY_FEE {
        config.priority_fee = Some(cursor.u64_le()?);
    }
    if mask & V1_MASK_COMPUTE_UNIT_LIMIT != 0 {
        config.compute_unit_limit = Some(cursor.u32_le()?);
    }
    if mask & V1_MASK_LOADED_ACCOUNTS_DATA_SIZE != 0 {
        config.loaded_accounts_data_size_limit = Some(cursor.u32_le()?);
    }
    if mask & V1_MASK_HEAP_SIZE != 0 {
        config.heap_size = Some(cursor.u32_le()?);
    }

    // Instruction headers come first as a packed (u8 program index, u8 account count,
    // u16 LE data length) table, then every payload in the same order.
    let headers = cursor.take(num_instructions * 4)?;
    let mut instructions = Vec::with_capacity(num_instructions);
    for chunk in headers.chunks_exact(4) {
        let program_id_index = chunk[0];
        let num_accounts = usize::from(chunk[1]);
        let data_len = usize::from(u16::from_le_bytes([chunk[2], chunk[3]]));
        let accounts = cursor.take(num_accounts)?.to_vec();
        let data = cursor.take(data_len)?.to_vec();
        instructions.push(CompiledInstruction {
            program_id_index,
            accounts,
            data,
        });
    }

    Ok((
        legacy::Message {
            header,
            account_keys,
            recent_blockhash: lifetime_specifier,
            instructions,
        },
        config,
    ))
}

fn read_transaction(cursor: &mut Cursor<'_>) -> Result<DecodedTransaction, DecodeError> {
    let start = cursor.pos;
    let discriminator = cursor.u8()?;
    if discriminator & MESSAGE_VERSION_PREFIX == 0 {
        // Legacy or v0: the byte is the canonical one-byte ShortU16 signature count.
        let signatures = read_signatures(cursor, usize::from(discriminator))?;
        let message_start = cursor.pos;
        let first = cursor.u8()?;
        let (message, version) = if first & MESSAGE_VERSION_PREFIX == 0 {
            (
                VersionedMessage::Legacy(read_legacy_message(cursor, first)?),
                TransactionVersion::Legacy,
            )
        } else if first == V0_PREFIX {
            (
                VersionedMessage::V0(read_v0_message(cursor)?),
                TransactionVersion::V0,
            )
        } else {
            return Err(DecodeError::UnsupportedMessageVersion {
                at: message_start,
                version: first & !MESSAGE_VERSION_PREFIX,
            });
        };
        Ok(DecodedTransaction {
            transaction: VersionedTransaction {
                signatures,
                message,
            },
            version,
            signed_message: message_start..cursor.pos,
            v1_config: None,
        })
    } else if discriminator == V1_PREFIX {
        let (message, config) = read_v1_message(cursor)?;
        let signed_end = cursor.pos;
        let signatures =
            read_signatures(cursor, usize::from(message.header.num_required_signatures))?;
        Ok(DecodedTransaction {
            transaction: VersionedTransaction {
                signatures,
                message: VersionedMessage::Legacy(message),
            },
            version: TransactionVersion::V1,
            signed_message: start..signed_end,
            v1_config: Some(config),
        })
    } else {
        Err(DecodeError::InvalidDiscriminator {
            at: start,
            byte: discriminator,
        })
    }
}

/// Decode one serialized transaction (legacy, v0 or v1). Returns the decoded transaction and
/// the number of bytes consumed; `signed_message` is relative to `bytes`.
pub fn decode_transaction(bytes: &[u8]) -> Result<(DecodedTransaction, usize), DecodeError> {
    let mut cursor = Cursor::new(bytes);
    let transaction = read_transaction(&mut cursor)?;
    Ok((transaction, cursor.pos))
}

/// Decode a proxy micro-batch (`Vec<Entry>` layout). Trailing bytes after the last entry are
/// ignored; every `signed_message` range is relative to `bytes`.
pub fn decode_entries(bytes: &[u8]) -> Result<Vec<DecodedEntry>, DecodeError> {
    let mut cursor = Cursor::new(bytes);
    let entry_count = usize::try_from(cursor.u64_le()?).unwrap_or(usize::MAX);
    let mut entries = Vec::with_capacity(cursor.bounded_capacity(entry_count, MIN_ENTRY_BYTES));
    for _ in 0..entry_count {
        let num_hashes = cursor.u64_le()?;
        let hash = Hash::new_from_array(cursor.array::<HASH_BYTES>()?);
        let transaction_count = usize::try_from(cursor.u64_le()?).unwrap_or(usize::MAX);
        let mut transactions = Vec::with_capacity(cursor.bounded_capacity(transaction_count, 1));
        for _ in 0..transaction_count {
            transactions.push(read_transaction(&mut cursor)?);
        }
        entries.push(DecodedEntry {
            num_hashes,
            hash,
            transactions,
        });
    }
    Ok(entries)
}

/// Real mainnet transactions from slot 447963115 (2026-09-18), fetched over RPC with
/// `encoding: base64, maxSupportedTransactionVersion: 1`, for tests across the crate.
#[cfg(test)]
pub(crate) mod test_fixtures {
    use base64::Engine;

    // Real mainnet transactions from slot 447963115 (2026-09-18), fetched over RPC with
    // `encoding: base64, maxSupportedTransactionVersion: 1`.
    //
    // V1_SMALL: sig 3oVC7FMfsFzXumALXXvBh2LKsHZbP4Fdxc7K9SKGjPq46HHAkcvUiqDDJbiDK9gUwvmiV5WPNaSg3k46FqPHaAHx
    //   1132 bytes, 8 keys, 3 instructions, header (1,0,5), config priorityFee 54,
    //   computeUnitLimit 26800, loadedAccountsDataSizeLimit 10000000, heapSize none.
    pub(crate) const V1_SMALL_B64: &str = "gQEABQ8AAABkUKHlJex71sD7WHa4drbcof35nNf39ic7UTuwELW4eAMIDQQy4ipPfDn3XTBx5zQxW7jPYOraWNnQXVAjsr4neADIgkaK+mgFQmpMbN0yUcXnMMgv83TppzMO+FRHvUalWPUzWqeGSqnxhO3xsWEIiO+R0A2OMLIMRJcNNgpgpy/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGp9UXGSxWjuCKhF9z0peIzwNcMUWyGrNE2AYuqUAAALlIs5Rf9vPrrCv+PPKR0viTOMMUVIjqhy6DG6uCSAy5znDW+1aOQ+v8lkqQFWw76VY7CBcGfKtpTryD792zfoboVybAsrW8+vZmi3wbg2n7AcmYX8wWfwtCmb8dfR7CpTYAAAAAAAAAsGgAAICWmAADAwQABgUrAAYFqgICBAAEAAAABgcABQEtm8g0Jda7jIgoAAAAAAAAAABteoOyoAEAAGUAEwCAAIAAgG16g7KgAQAABgcABQHxW4uqEinLFgoAAAABAAAAAAAAAAIAAAAAAAAAYhoAAGFLAAAPAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAA5XgAAIuIAAA8AAAAAAAAAAwAAAAAAAAAEAAAAAAAAAM38AQAAAAAADwAAAAAAAAAHAAAAAAAAAAgAAAAAAAAAPeoCAAAAAAAPAAAAAAAAAAsAAAAAAAAADAAAAAAAAAA9CQIAAAAAAA8AAAAAAAAADwAAAAAAAAAQAAAAAAAAAGRyAgAAAAAADwAAAAAAAAAUAAAAAAAAABUAAAAAAAAA8dkDAAAAAAAPAAAAAAAAABgAAAAAAAAAGQAAAAAAAACiIwoAAAAAAA8AAAAAAAAAHgAAAAAAAAAoAAAAAAAAAOpSAAAAAAAADwAAAAAAAACsAAAAAAAAAAkBAAAAAAAAOi0AAAAAAAA8AAAAAAAAAAoAAAABAAAAAAAAAAIAAAAAAAAAYhoAAGFLAAAPAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAA5XgAAIuIAAA8AAAAAAAAAAwAAAAAAAAAEAAAAAAAAAM38AQAAAAAADwAAAAAAAAAHAAAAAAAAAAgAAAAAAAAAPeoCAAAAAAAPAAAAAAAAAAsAAAAAAAAADAAAAAAAAAA9CQIAAAAAAA8AAAAAAAAADwAAAAAAAAAQAAAAAAAAAGRyAgAAAAAADwAAAAAAAAAUAAAAAAAAABUAAAAAAAAA8dkDAAAAAAAPAAAAAAAAABgAAAAAAAAAGQAAAAAAAACiIwoAAAAAAA8AAAAAAAAAHgAAAAAAAAAoAAAAAAAAAOpSAAAAAAAADwAAAAAAAACsAAAAAAAAAAkBAAAAAAAAOi0AAAAAAAA8AAAAAAAAAABteoOyoAEAAGUAEwCAAIAAgG16g7KgAQAAjB8u/qXS8JEVuJ0n8nvvFNYngsJSqPZT+utylRbnYchJwuxgDnz9huI5SmP+/r/TIEE8nwmr0uRdMCfK7XjkAw==";
    // V1_LARGE: sig 5G4uJ6AYCieRbQ7jxYszJUWwfjyBE4NaWmcjn669CQXoS8QBsdYnj7ATGCMZKJ2Q5R7rdE54gixdKYmv1XY5W8wb
    //   2231 bytes (above the old 1232-byte limit), 63 keys, 2 instructions, header (1,0,53),
    //   config priorityFee 71017, computeUnitLimit 87966, loadedAccountsDataSizeLimit 67108864.
    pub(crate) const V1_LARGE_B64: &str = "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";
    // V0: sig 3LiBzCSWyEVceLmcQZpMfx7sA7Rq6GB5A3wdeTjhEtJFjn4xwtziVT2ro7Mq2nAMAarPnpqmUjBRsYYsMpLJxbz7
    //   724 bytes, 9 static keys, 4 instructions, uses address lookup tables.
    pub(crate) const V0_B64: &str = "AXUIAsrNGj8AE+TE//rHvIc+UkH5YfYtexcUfRHrS1rSRmK9wIaIi2+6UvUGP0a9d6N3rw5zo5fpgb1jT40M/wCAAQADCUuYF3gC2Os11et6KniuH+M11xrGCGWn4EY4BVI7+9HAICYQHsIDKJZKMqurE2xUBbkfOuOO5PZMtr3oebhoONJWmQKWFXfOipQt6Q+t/iUZxp089sFeqI8h7Ikq3UdqpyrRa1ravYjC9jBC/GDi4uvG6q5BRrV2uUxgyFNPgyoWtkkLuX89UJmlw5qHL3flwTNsfe74zctiArD0um6a4fXIKnojqYdjPSp5fO9vA5uqlVMm0PWeRMkgQ/oZPo/OOQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwZGb+UhFzL/7K26csOb57yM5bvF9xJrLEObOkAAAAD4VRO0ZkTnkh10Fh0gn10wGVxzrU+f3g1DV/NLl09+GEdxUWUTLdW9QmP/zzt2XmX3unLxYsg8cUmtyEWoYTRxBAcABQIyewUABgIAAQwCAAAAD2sAAAAAAAAIWgACJygGKQMqKywJLQoLLgwGKS8NMDEEMjM0NQ42DxAREjcXFxc4NhMUFRY3GBgYOAU5KywZLRobLgwGKS8cOjEEMjM7NQ42HR4fIDchISE4NiIjJCU3JiYmOBMUAAIAAAAAAAAAAAMAAgIDAAICBwAFBP0m9QAESXR7CT/e2jxBsfVw/eZBvctZBRlaGIppIljX1viZhjoO5OflIOI86ero6+7s7e8SAQIc3gwUGx0j4x8YFeY3AAgL1/4T1BoLiiijqdSmQnTGNkf2J6JsQ3gqnvA1tH1ON1gBCAD66T76s8pYF5g0PYxhfiEu3QUSzB2Lp85yb3Ux2oP/FQGLAAS2n2ALsA7jsb89Uaz2t4HevztYWJJ4TJdaQinOWRyeDgsQDAkWGRgUFwcKDggGAxIREw==";
    // LEGACY: sig 5Y7aF6Rch6Rw1tZcWCtw1WuZr9i8yY3aS126r8DFqFEwACvc6f281RynppcG4D3F8ZLbY7JHKDF3fuPEf4heVa5J
    //   352 bytes, 3 keys, 1 instruction.
    pub(crate) const LEGACY_B64: &str = "AeLmIX2Ihypz2IabgSoxqNcuiIW8Vd8F39k4nCFs1/RcuwtUQeRb/CGocbtFqQbk/4K6QFmQvLO2EEALmGRG/g0BAAEDg5vp2y2IjjFPrINJADiGTV3x0uH2+7x+j/pAXM1Ijbn4biDRQ+ZqOQ7kwmGBMSsSX/0s4kyf0D79E6/YZ0bSYQdhSB01dHS7fE12JOvTvbPYNV5z0RBD/A2jU4AAAAAAlw1ugdJPBuyET54b6AsiamUckT2DL9Hp1PAbohz0Ko4BAgIBAJQBDgAAAMtfsxoAAAAAHwEfAR4BHQEcARsBGgEZARgBFwEWARUBFAETARIBEQEQAQ8BDgENAQwBCwEKAQkBCAEHAQYBBQEEAQMBAgEBC+vMV0JVAAmNIXlPA9cE9rkR4N0Q7V7eeP7U64XTlZQB7KysagAAAAATH2IF/0JJdJhMNQaDxofGqLEdVgJlHh1GRrzuowaAOQ==";

    pub(crate) fn decode(b64: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap()
    }
    pub(crate) fn v1_small() -> Vec<u8> {
        decode(V1_SMALL_B64)
    }
    pub(crate) fn v1_large() -> Vec<u8> {
        decode(V1_LARGE_B64)
    }
    pub(crate) fn v0() -> Vec<u8> {
        decode(V0_B64)
    }
    pub(crate) fn legacy() -> Vec<u8> {
        decode(LEGACY_B64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use solana_entry::entry::Entry;
    use std::str::FromStr;

    use super::test_fixtures::{LEGACY_B64, V0_B64, V1_LARGE_B64, V1_SMALL_B64};

    fn b64(s: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(s).unwrap()
    }

    fn pk(s: &str) -> Pubkey {
        Pubkey::from_str(s).unwrap()
    }

    /// Wrap raw transaction byte strings into one `Vec<Entry>` micro-batch (bincode layout).
    fn batch_of(entries: &[(u64, [u8; 32], &[&[u8]])]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (num_hashes, hash, txs) in entries {
            out.extend_from_slice(&num_hashes.to_le_bytes());
            out.extend_from_slice(hash);
            out.extend_from_slice(&(txs.len() as u64).to_le_bytes());
            for tx in *txs {
                out.extend_from_slice(tx);
            }
        }
        out
    }

    fn sample_legacy_tx(seed: u8) -> VersionedTransaction {
        let keys: Vec<Pubkey> = (0..4u8)
            .map(|i| Pubkey::new_from_array([seed ^ i; 32]))
            .collect();
        VersionedTransaction {
            signatures: vec![Signature::from([seed; 64])],
            message: VersionedMessage::Legacy(legacy::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: keys,
                recent_blockhash: Hash::new_from_array([seed.wrapping_add(7); 32]),
                instructions: vec![
                    CompiledInstruction {
                        program_id_index: 3,
                        accounts: vec![0, 1, 2],
                        data: vec![seed; 200], // > 127 bytes forces a two-byte ShortU16
                    },
                    CompiledInstruction {
                        program_id_index: 2,
                        accounts: vec![],
                        data: vec![],
                    },
                ],
            }),
        }
    }

    fn sample_v0_tx(seed: u8) -> VersionedTransaction {
        let keys: Vec<Pubkey> = (0..3u8)
            .map(|i| Pubkey::new_from_array([seed ^ (i + 9); 32]))
            .collect();
        VersionedTransaction {
            signatures: vec![Signature::from([seed; 64]), Signature::from([seed ^ 1; 64])],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 2,
                    num_readonly_signed_accounts: 1,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: keys,
                recent_blockhash: Hash::new_from_array([seed.wrapping_add(3); 32]),
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 3, 4, 5],
                    data: vec![1, 2, 3],
                }],
                address_table_lookups: vec![
                    MessageAddressTableLookup {
                        account_key: Pubkey::new_from_array([seed ^ 0x55; 32]),
                        writable_indexes: vec![7, 8],
                        readonly_indexes: vec![9],
                    },
                    MessageAddressTableLookup {
                        account_key: Pubkey::new_from_array([seed ^ 0x66; 32]),
                        writable_indexes: vec![],
                        readonly_indexes: vec![1, 2, 3],
                    },
                ],
            }),
        }
    }

    #[test]
    fn legacy_and_v0_batches_match_the_old_decoder_exactly() {
        let entries = vec![
            Entry {
                num_hashes: 12,
                hash: Hash::new_from_array([1; 32]),
                transactions: vec![sample_legacy_tx(1), sample_v0_tx(2), sample_legacy_tx(3)],
            },
            Entry {
                num_hashes: 0,
                hash: Hash::new_from_array([2; 32]),
                transactions: vec![],
            },
            Entry {
                num_hashes: 99,
                hash: Hash::new_from_array([3; 32]),
                transactions: vec![sample_v0_tx(4)],
            },
        ];
        let bytes = bincode::serialize(&entries).unwrap();
        let old: Vec<Entry> = bincode::deserialize(&bytes).unwrap();
        let new = decode_entries(&bytes).unwrap();
        assert_eq!(old.len(), new.len());
        for (o, n) in old.iter().zip(new.iter()) {
            assert_eq!(o.num_hashes, n.num_hashes);
            assert_eq!(o.hash, n.hash);
            assert_eq!(o.transactions.len(), n.transactions.len());
            for (ot, nt) in o.transactions.iter().zip(n.transactions.iter()) {
                assert_eq!(*ot, nt.transaction);
                assert!(nt.v1_config.is_none());
                let expected_version = match ot.message {
                    VersionedMessage::Legacy(_) => TransactionVersion::Legacy,
                    VersionedMessage::V0(_) => TransactionVersion::V0,
                };
                assert_eq!(nt.version, expected_version);
                // The retained signed range is exactly the serialized message.
                assert_eq!(
                    &bytes[nt.signed_message.clone()],
                    bincode::serialize(&ot.message).unwrap().as_slice()
                );
            }
        }
    }

    #[test]
    fn real_legacy_and_v0_transactions_decode() {
        for (raw, version, key_count) in [
            (b64(LEGACY_B64), TransactionVersion::Legacy, 3usize),
            (b64(V0_B64), TransactionVersion::V0, 9usize),
        ] {
            let (decoded, consumed) = decode_transaction(&raw).unwrap();
            assert_eq!(consumed, raw.len());
            assert_eq!(decoded.version, version);
            assert_eq!(
                decoded.transaction.message.static_account_keys().len(),
                key_count
            );
            let reference: VersionedTransaction = bincode::deserialize(&raw).unwrap();
            assert_eq!(decoded.transaction, reference);
        }
    }

    #[test]
    fn real_v1_transactions_decode_to_inline_legacy_messages() {
        let raw = b64(V1_SMALL_B64);
        let (decoded, consumed) = decode_transaction(&raw).unwrap();
        assert_eq!(consumed, raw.len());
        assert_eq!(decoded.version, TransactionVersion::V1);
        assert_eq!(decoded.transaction.signatures.len(), 1);
        assert_eq!(
            decoded.transaction.signatures[0],
            Signature::from_str("3oVC7FMfsFzXumALXXvBh2LKsHZbP4Fdxc7K9SKGjPq46HHAkcvUiqDDJbiDK9gUwvmiV5WPNaSg3k46FqPHaAHx").unwrap()
        );
        let VersionedMessage::Legacy(message) = &decoded.transaction.message else {
            panic!("v1 must be surfaced as an inline legacy message");
        };
        assert_eq!(
            message.header,
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 5,
            }
        );
        assert_eq!(message.account_keys.len(), 8);
        assert_eq!(
            message.account_keys[0],
            pk("sp1nLqqjVZ1kTusFjJmHp1xHDpH6M4F7KXCr1iJ3xpX")
        );
        assert_eq!(
            message.recent_blockhash,
            Hash::from_str("7kb5j76XNZ5zu9zKjX8FsPiiSHhgk4Yb2zDUpjbxKNzT").unwrap()
        );
        assert_eq!(message.instructions.len(), 3);
        assert_eq!(message.instructions[0].program_id_index, 3);
        assert_eq!(message.instructions[0].accounts, vec![2, 4, 0]);
        assert_eq!(
            decoded.v1_config,
            Some(V1TransactionConfig {
                priority_fee: Some(54),
                compute_unit_limit: Some(26_800),
                loaded_accounts_data_size_limit: Some(10_000_000),
                heap_size: None,
            })
        );
        assert!(
            decoded
                .transaction
                .message
                .address_table_lookups()
                .is_none()
        );
        // The signed range starts at the 0x81 prefix and stops right before the signatures.
        assert_eq!(decoded.signed_message, 0..raw.len() - 64);
        assert_eq!(raw[decoded.signed_message.start], V1_PREFIX);
    }

    #[test]
    fn real_v1_transaction_above_legacy_size_limit_decodes() {
        let raw = b64(V1_LARGE_B64);
        assert!(raw.len() > 1232);
        let (decoded, consumed) = decode_transaction(&raw).unwrap();
        assert_eq!(consumed, raw.len());
        assert_eq!(decoded.version, TransactionVersion::V1);
        let keys = decoded.transaction.message.static_account_keys();
        assert_eq!(keys.len(), 63);
        assert_eq!(keys[0], pk("2KP9miexioqgNbjPwysggcbi3sKqhH8JsPaDmFxRPURD"));
        assert_eq!(
            decoded
                .transaction
                .message
                .header()
                .num_readonly_unsigned_accounts,
            53
        );
        let instructions = decoded.transaction.message.instructions();
        assert_eq!(instructions.len(), 2);
        assert_eq!(instructions[0].program_id_index, 10);
        assert_eq!(instructions[0].accounts, vec![0, 2]);
        assert_eq!(
            decoded.v1_config,
            Some(V1TransactionConfig {
                priority_fee: Some(71_017),
                compute_unit_limit: Some(87_966),
                loaded_accounts_data_size_limit: Some(67_108_864),
                heap_size: None,
            })
        );
    }

    #[test]
    fn mixed_batch_decodes_where_the_old_decoder_failed() {
        let legacy = b64(LEGACY_B64);
        let v1 = b64(V1_SMALL_B64);
        let v0 = b64(V0_B64);
        let bytes = batch_of(&[
            (
                5,
                [9; 32],
                &[legacy.as_slice(), v1.as_slice(), v0.as_slice()],
            ),
            (6, [8; 32], &[legacy.as_slice()]),
        ]);

        // This is the production failure: one v1 transaction poisoned the whole micro-batch.
        assert!(bincode::deserialize::<Vec<Entry>>(&bytes).is_err());

        let entries = decode_entries(&bytes).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].num_hashes, 5);
        assert_eq!(entries[0].hash, Hash::new_from_array([9; 32]));
        let versions: Vec<_> = entries[0].transactions.iter().map(|t| t.version).collect();
        assert_eq!(
            versions,
            vec![
                TransactionVersion::Legacy,
                TransactionVersion::V1,
                TransactionVersion::V0
            ]
        );
        assert_eq!(entries[1].transactions.len(), 1);
        assert_eq!(
            entries[1].transactions[0].version,
            TransactionVersion::Legacy
        );
        // Ranges are relative to the batch buffer: the v1 range starts at its 0x81 prefix.
        let v1_range = &entries[0].transactions[1].signed_message;
        assert_eq!(bytes[v1_range.start], V1_PREFIX);
        assert_eq!(v1_range.len(), v1.len() - 64);
        // Trailing bytes after the last entry are tolerated.
        let mut padded = bytes.clone();
        padded.extend_from_slice(&[0xAA; 3]);
        assert_eq!(decode_entries(&padded).unwrap().len(), 2);
    }

    #[test]
    fn malformed_input_errors_instead_of_panicking() {
        let v1 = b64(V1_SMALL_B64);
        // Every truncation of the v1 transaction is an error, never a panic.
        for cut in 0..v1.len() {
            assert!(decode_transaction(&v1[..cut]).is_err(), "cut at {cut}");
        }
        // Unknown transaction discriminator / message version.
        let mut bad = v1.clone();
        bad[0] = 0x82;
        assert!(matches!(
            decode_transaction(&bad),
            Err(DecodeError::InvalidDiscriminator { byte: 0x82, .. })
        ));
        let mut v0_with_v1_message = b64(V0_B64);
        v0_with_v1_message[65] = V1_PREFIX; // sig count byte 1, signature 64 bytes, then prefix
        assert!(matches!(
            decode_transaction(&v0_with_v1_message),
            Err(DecodeError::UnsupportedMessageVersion { version: 1, .. })
        ));
        // Config mask with an unknown bit, and a partial priority-fee pair.
        let mut unknown_bit = v1.clone();
        unknown_bit[4] |= 0b10_0000;
        assert!(matches!(
            decode_transaction(&unknown_bit),
            Err(DecodeError::InvalidV1ConfigMask { .. })
        ));
        let mut partial_fee = v1.clone();
        partial_fee[4] &= !0b10;
        assert!(matches!(
            decode_transaction(&partial_fee),
            Err(DecodeError::InvalidV1ConfigMask { .. })
        ));
        // Non-minimal ShortU16 in a legacy key count.
        let mut legacy = b64(LEGACY_B64);
        // 1 sig count + 64 sig + 3 header bytes = key count at 68.
        assert_eq!(legacy[68], 3);
        legacy[68] = 0x83;
        legacy.insert(69, 0x00);
        assert!(matches!(
            decode_transaction(&legacy),
            Err(DecodeError::ShortU16 { .. })
        ));
        // Absurd declared counts fail on EOF without allocating.
        let mut huge = Vec::new();
        huge.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_entries(&huge).is_err());
        assert!(decode_entries(&[]).is_err());
        assert_eq!(decode_entries(&0u64.to_le_bytes()).unwrap().len(), 0);
    }

    #[test]
    fn short_u16_matches_solana_encoding() {
        fn read(bytes: &[u8]) -> Result<(usize, usize), DecodeError> {
            let mut c = Cursor::new(bytes);
            let v = c.short_u16()?;
            Ok((v, c.pos))
        }
        assert_eq!(read(&[0x00]).unwrap(), (0, 1));
        assert_eq!(read(&[0x7f]).unwrap(), (127, 1));
        assert_eq!(read(&[0x80, 0x01]).unwrap(), (128, 2));
        assert_eq!(read(&[0xff, 0x7f]).unwrap(), (16_383, 2));
        assert_eq!(read(&[0x80, 0x80, 0x01]).unwrap(), (16_384, 3));
        assert_eq!(read(&[0xff, 0xff, 0x03]).unwrap(), (65_535, 3));
        assert!(read(&[0xff, 0xff, 0x04]).is_err()); // > u16::MAX
        assert!(read(&[0x80, 0x00]).is_err()); // alias of 0
        assert!(read(&[0x80, 0x80, 0x80]).is_err()); // third byte continues
        assert!(read(&[0x80]).is_err()); // truncated
    }

    /// Timing only: `cargo test --release entry_decode::tests::bench -- --ignored --nocapture`.
    /// Compares this decoder with the previous `bincode` path on a legacy/v0-only batch
    /// (the only kind the old path can decode) and reports the v1-bearing batch on its own.
    #[test]
    #[ignore = "timing only; run with --ignored --nocapture"]
    fn bench_decode_vs_bincode() {
        use std::hint::black_box;
        use std::time::Instant;
        let legacy = b64(LEGACY_B64);
        let v0 = b64(V0_B64);
        let v1 = b64(V1_SMALL_B64);
        let mut old_style: Vec<&[u8]> = Vec::new();
        let mut mixed: Vec<&[u8]> = Vec::new();
        for i in 0..40 {
            old_style.push(if i % 2 == 0 { &legacy } else { &v0 });
            mixed.push(match i % 5 {
                0 => &v1,
                1 | 2 => &legacy,
                _ => &v0,
            });
        }
        let old_batch = batch_of(&[(1, [1; 32], &old_style)]);
        let mixed_batch = batch_of(&[(1, [1; 32], &mixed)]);
        assert!(bincode::deserialize::<Vec<Entry>>(&mixed_batch).is_err());
        let iterations = 20_000u32;
        let time = |f: &dyn Fn()| {
            for _ in 0..500 {
                f();
            }
            let start = Instant::now();
            for _ in 0..iterations {
                f();
            }
            start.elapsed().as_nanos() as f64 / f64::from(iterations)
        };
        let bincode_ns = time(&|| {
            black_box(bincode::deserialize::<Vec<Entry>>(black_box(&old_batch)).unwrap());
        });
        let new_ns = time(&|| {
            black_box(decode_entries(black_box(&old_batch)).unwrap());
        });
        let mixed_ns = time(&|| {
            black_box(decode_entries(black_box(&mixed_batch)).unwrap());
        });
        println!(
            "40-tx legacy/v0 batch ({} B): bincode {:.0} ns, entry_decode {:.0} ns; \
             40-tx batch with 8 v1 ({} B): entry_decode {:.0} ns (bincode: error)",
            old_batch.len(),
            bincode_ns,
            new_ns,
            mixed_batch.len(),
            mixed_ns
        );
    }

    /// Corpus check against live mainnet: `ENTRY_DECODE_CORPUS` points at a JSON array of
    /// `{"slot", "version": "legacy"|"v0"|"v1", "b64"}` (every transaction of some recent
    /// blocks, fetched with `maxSupportedTransactionVersion: 1`). Every transaction must
    /// decode, consume exactly its bytes, and report the RPC's version.
    #[test]
    #[ignore = "needs ENTRY_DECODE_CORPUS; run with --ignored --nocapture"]
    fn decode_recent_block_corpus() {
        let path = std::env::var("ENTRY_DECODE_CORPUS").expect("ENTRY_DECODE_CORPUS not set");
        let corpus: Vec<serde_json::Value> =
            serde_json::from_slice(&std::fs::read(&path).expect("read corpus"))
                .expect("parse corpus");
        let mut counts = std::collections::BTreeMap::<&str, usize>::new();
        let mut failures = Vec::new();
        for (i, tx) in corpus.iter().enumerate() {
            let raw = b64(tx["b64"].as_str().unwrap());
            let expected = tx["version"].as_str().unwrap();
            match decode_transaction(&raw) {
                Ok((decoded, consumed)) => {
                    let got = decoded.version.as_str();
                    if consumed != raw.len() || got != expected {
                        failures.push(format!(
                            "#{i} slot {} expected {expected} got {got} consumed {consumed}/{}",
                            tx["slot"],
                            raw.len()
                        ));
                    }
                    *counts.entry(decoded.version.as_str()).or_default() += 1;
                }
                Err(error) => failures.push(format!(
                    "#{i} slot {} expected {expected}: {error} ({} bytes)",
                    tx["slot"],
                    raw.len()
                )),
            }
        }
        println!("decoded {counts:?}; failures {}", failures.len());
        for f in failures.iter().take(20) {
            println!("  {f}");
        }
        assert!(failures.is_empty());
    }
}
