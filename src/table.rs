use crate::record::RECORD_SIZE;

/// A highly concurrent table of records, the core datastructure in a database. This implements the low-level table structure, without any logic for lookup or insertion.
pub struct Table {
    /// Mmapped file
    file: memmap::MmapMut,
}

impl Table {
    /// Creates a table, given a memmapped file.
    pub fn new(file: memmap::MmapMut) -> Self {
        Self { file }
    }

    /// Gets the given record out of the table
    pub fn get(&self, recno: usize) -> Option<&[u8]> {
        if recno * RECORD_SIZE >= self.file.len() {
            None
        } else {
            Some(&self.file[recno * RECORD_SIZE..][..RECORD_SIZE])
        }
    }

    /// Gets the given record out of the table, mutably
    pub fn get_mut(&mut self, recno: usize) -> Option<&mut [u8]> {
        if recno * RECORD_SIZE >= self.file.len() {
            None
        } else {
            Some(&mut self.file[recno * RECORD_SIZE..][..RECORD_SIZE])
        }
    }

    /// Gets the number of records in the table.
    pub fn len(&self) -> usize {
        self.file.len() / RECORD_SIZE
    }

    /// Flushes the database, blocking until all data is stably on disk.
    pub fn flush(&self) {
        self.file.flush().expect("flushing mmap somehow failed");
    }
}
