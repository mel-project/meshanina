/// A single on-disk, memory-mapped record.
pub struct Record<'a>(&'a [u8]);

impl<'a> Record<'a> {
    /// Gets the record checksum.
    pub fn crc32(&self) -> u32 {}
}
