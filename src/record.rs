use arrayref::array_ref;
use crc::{Crc, CRC_32_ISO_HDLC};
use ethnum::U256;

/// Max size of a record body
pub const MAX_RECORD_BODYLEN: usize = 728;

/// Record size
pub const RECORD_SIZE: usize = 768;

/// Write a record to a particular byte slice.
pub fn new_record(key: U256, length: usize, value: &[u8]) -> [u8; RECORD_SIZE] {
    assert!(value.len() <= length);
    let mut dest = [0u8; RECORD_SIZE];
    // write everything except the checksum
    {
        let (header, body) = dest.split_at_mut(4 + 32 + 4);
        // write the body first
        body[..value.len()].copy_from_slice(value);
        // then write the header
        header[4..][32..][..4].copy_from_slice(&(length as u32).to_le_bytes());
        header[4..][..32].copy_from_slice(&key.to_le_bytes());
    }
    let chksum = crc32fast::hash(&dest[4..]);
    dest[..4].copy_from_slice(&chksum.to_le_bytes());
    dest
}

/// A single on-disk, memory-mapped record.
pub struct Record<'a>(pub &'a [u8]);

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

impl<'a> Record<'a> {
    /// Gets the record checksum.
    fn checksum(&self) -> u32 {
        u32::from_le_bytes(*array_ref![self.0, 0, 4])
    }

    /// Compute the checksum
    fn legacy_crc32(&self) -> u32 {
        CRC32.checksum(&self.0[4..])
    }

    /// Compute the new checksum
    fn new_crc32(&self) -> u32 {
        crc32fast::hash(&self.0[4..])
    }

    /// Validates the checksum of the record.
    pub fn validate(self) -> Option<Self> {
        let csum = self.checksum();
        if csum > 0 && (csum == self.new_crc32() || csum == self.legacy_crc32()) {
            Some(self)
        } else {
            None
        }
    }

    /// Gets the key of the record.
    pub fn key(&self) -> U256 {
        U256::from_le_bytes(*array_ref![self.0[4..], 0, 32])
    }

    /// Get the length of the record.
    pub fn length(&self) -> usize {
        u32::from_le_bytes(*array_ref![self.0[4..][32..], 0, 4]) as usize
    }

    /// Get the value of the record.
    pub fn value(&self) -> &[u8] {
        let length = self.length();
        let v = &self.0[4..][32..][4..];
        if v.len() > length {
            &v[..length]
        } else {
            v
        }
    }
}
