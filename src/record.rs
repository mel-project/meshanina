use crc::{Crc, CRC_32_ISO_HDLC};
use ethnum::U256;
use zeroize::Zeroize;

/// Write a record to a particular byte slice.
pub fn write_record(dest: &mut [u8], key: U256, length: usize, value: &[u8]) {
    assert!(dest.len() == 512);
    assert!(value.len() <= length);
    dest.zeroize();
    // write everything except the checksum
    {
        let (header, body) = dest.split_at_mut(4 + 32 + 4);
        // write the body first
        body[..value.len()].copy_from_slice(value);
        // then write the header
        header[4..][32..][..4].copy_from_slice(&(length as u32).to_le_bytes());
        header[4..][..32].copy_from_slice(&key.to_le_bytes());
    }
    let chksum = CRC32.checksum(&dest[4..]);
    dest[..4].copy_from_slice(&chksum.to_le_bytes());
}

/// A single on-disk, memory-mapped record.
pub struct Record<'a>(pub &'a [u8]);

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

impl<'a> Record<'a> {
    /// Gets the record checksum.
    pub fn crc32(&self) -> u32 {
        u32::from_le_bytes(self.0[0..4].try_into().unwrap())
    }

    /// Compute the checksum
    pub fn correct_crc32(&self) -> u32 {
        CRC32.checksum(&self.0[4..])
    }

    /// Validates the checksum of the record.
    pub fn validate(self) -> Option<Self> {
        if self.crc32() == self.correct_crc32() {
            Some(self)
        } else {
            None
        }
    }

    /// Gets the key of the record.
    pub fn key(&self) -> U256 {
        U256::from_le_bytes(self.0[4..][..32].try_into().unwrap())
    }

    /// Get the length of the record.
    pub fn length(&self) -> usize {
        u32::from_le_bytes(self.0[4..][32..][..4].try_into().unwrap()) as usize
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
