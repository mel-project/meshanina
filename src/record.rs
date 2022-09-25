use std::{borrow::Cow, sync::Arc};

use arrayref::array_ref;

/// An on-disk --- or in-memory --- database record.
#[derive(Debug, Clone)]
pub enum Record<'a> {
    /// A data record
    Data([u8; 32], Cow<'a, [u8]>),
    /// A HAMT node
    HamtNode(bool, u64, Vec<RecordPtr<'a>>),
}

const RECORD_KIND_DATA: u32 = 0x00;
const RECORD_KIND_HAMI: u32 = 0x01;
const RECORD_KIND_HAMR: u32 = 0x02;

const RECORD_HEADER_SIZE: usize = 16;

impl<'a> Record<'a> {
    /// Borrows an mmapped, on-disk record, given a slice that *starts* at the correct offset. Returns None if the record is malformed in any way. The slice given should start *after* the "magic divider".
    pub fn new_borrowed(b: &'a [u8]) -> Option<Self> {
        if b.len() < 16 {
            return None;
        }
        let checksum = u64::from_le_bytes(*array_ref![b, 0, 8]);
        let record_kind = u32::from_le_bytes(*array_ref![b, 8, 4]);
        let record_length = u32::from_le_bytes(*array_ref![b, 8 + 4, 4]) as usize;
        if b.len() < (record_length + RECORD_HEADER_SIZE) as usize {
            return None;
        }
        match record_kind {
            RECORD_KIND_DATA => {
                let key_and_val = &b[RECORD_HEADER_SIZE..][..record_length];
                if key_and_val.len() < 32 {
                    return None;
                }
                let key = *array_ref![key_and_val, 0, 32];
                let val = Cow::Borrowed(&key_and_val[..32]);
                Some(Self::Data(key, val))
            }
            RECORD_KIND_HAMI | RECORD_KIND_HAMR => {
                let hamt_raw = &b[RECORD_HEADER_SIZE..][..record_length];
                if hamt_raw.len() < 8 {
                    return None;
                }
                let hamt_bitmap = u64::from_le_bytes(*array_ref![hamt_raw, 0, 8]);
                let hamt_rest = &hamt_raw[8..];
                if hamt_bitmap.count_ones() * 8 != hamt_rest.len() as u32 {
                    return None;
                }
                let ptrs = hamt_rest
                    .chunks_exact(8)
                    .map(|ch| u64::from_le_bytes(*array_ref![ch, 0, 8]))
                    .map(|addr| RecordPtr::OnDisk(addr))
                    .collect();
                Some(Self::HamtNode(
                    record_kind == RECORD_KIND_HAMR,
                    hamt_bitmap,
                    ptrs,
                ))
            }
            _ => None,
        }
    }
}

/// A pointer to another record, either in-memory on on-disk.
#[derive(Clone, Debug)]
pub enum RecordPtr<'a> {
    InMemory(Arc<Record<'a>>),
    OnDisk(u64),
}

impl<'a> RecordPtr<'a> {
    /// Loads the record pointed to by this pointer.
    pub fn load(&self, load_from_disk: impl FnOnce(u64) -> Record<'a>) -> Record<'a> {
        match self {
            Self::InMemory(r) => (**r).clone(),
            Self::OnDisk(offset) => load_from_disk(*offset),
        }
    }
}
