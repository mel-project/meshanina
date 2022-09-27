use std::{borrow::Cow, hash::Hasher, io::Write, sync::Arc};

use arrayref::array_ref;
use siphasher::sip::SipHasher13;

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
    /// Borrows an mmapped, on-disk record, given a slice that *starts* at the correct offset. Returns None if the record is malformed in any way. The slice given should start *at* the "magic divider", which must be passed in.
    pub fn new_borrowed(b: &'a [u8], divider: u128) -> Option<Self> {
        if b.len() < 16 + 16 {
            return None;
        }
        if u128::from_le_bytes(*array_ref![b, 0, 16]) != divider {
            return None;
        }
        let _checksum = u64::from_le_bytes(*array_ref![b, 0, 8]);
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
                    .map(RecordPtr::OnDisk)
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

    /// Writes the bytes representation of this record, returning how many bytes were written. Must provide a u128 divider.
    ///
    /// Will panic if this is a HAMT node with in-memory children!
    pub fn write_bytes(
        &self,
        divider: u128,
        mut out: impl std::io::Write,
    ) -> std::io::Result<usize> {
        let mut null_checksum_buffer = Vec::with_capacity(256);
        // write a DUMMY checksum
        null_checksum_buffer.write_all(&[0u8; 8])?;
        // write the kind
        let kind = match self {
            Record::Data(_, _) => RECORD_KIND_DATA,
            Record::HamtNode(true, _, _) => RECORD_KIND_HAMR,
            Record::HamtNode(false, _, _) => RECORD_KIND_HAMI,
        };
        null_checksum_buffer.write_all(&kind.to_le_bytes())?;
        // write the length
        let length = match self {
            Record::Data(_, v) => v.len() + 32,
            Record::HamtNode(_, _, ptrs) => ptrs.len() * 8 + 8,
        };
        null_checksum_buffer.write_all(&(length as u32).to_le_bytes())?;
        // write the record
        match self {
            Record::Data(k, v) => {
                null_checksum_buffer.write_all(k)?;
                null_checksum_buffer.write_all(v)?;
            }
            Record::HamtNode(_, bmap, ptrs) => {
                null_checksum_buffer.write_all(&bmap.to_le_bytes())?;
                for ptr in ptrs.iter() {
                    match ptr {
                        RecordPtr::InMemory(_) => {
                            panic!("cannot serialize a HAMT node that has in-memory children")
                        }
                        RecordPtr::OnDisk(ptr) => {
                            null_checksum_buffer.write_all(&ptr.to_le_bytes())?
                        }
                    }
                }
            }
        }
        // compute checksum
        let checksum = {
            let mut h = SipHasher13::new_with_key(&divider.to_le_bytes());
            h.write(&null_checksum_buffer[8..]);
            h.finish()
        };
        null_checksum_buffer[0..8].copy_from_slice(&checksum.to_le_bytes());
        // return
        out.write_all(&null_checksum_buffer)?;
        Ok(null_checksum_buffer.len())
    }

    /// Fully own the record.
    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::Data(k, v) => Record::Data(k, Cow::Owned(v.to_vec())),
            Record::HamtNode(a, b, c) => Record::HamtNode(
                a,
                b,
                c.into_iter()
                    .map(|c| match c {
                        RecordPtr::InMemory(r) => {
                            let v: Record<'static> = (*r).clone().into_owned();
                            let p: RecordPtr<'static> = RecordPtr::InMemory(Arc::new(v));
                            p
                        }
                        RecordPtr::OnDisk(u) => RecordPtr::<'static>::OnDisk(u),
                    })
                    .collect(),
            ),
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
