use std::{
    borrow::Cow,
    fs::File,
    io::{Seek, SeekFrom, Write},
};

use memmap::MmapOptions;

use crate::record::RECORD_SIZE;

/// A highly concurrent table of records, the core datastructure in a database. This implements the low-level table structure, without any logic for lookup or insertion.
pub struct Table {
    /// handle
    handle: File,
    /// Mmapped file
    file: memmap::MmapMut,
    /// Offset
    offset: u64,
}

impl Table {
    /// Creates a table, given a memmapped file.
    pub fn new(handle: File, offset: u64) -> Self {
        let mut table_mmap = unsafe { MmapOptions::new().offset(offset).map_mut(&handle).unwrap() };
        #[cfg(target_os = "linux")]
        unsafe {
            use libc::MADV_RANDOM;
            libc::madvise(
                &mut table_mmap[0] as *mut u8 as _,
                table_mmap.len(),
                MADV_RANDOM,
            );
        }
        Self {
            handle,
            file: table_mmap,
            offset,
        }
    }

    /// Gets the given record out of the table
    pub fn get(&self, recno: usize) -> Option<Cow<[u8]>> {
        if recno * RECORD_SIZE >= self.file.len() {
            None
        } else {
            Some(Cow::Borrowed(
                &self.file[recno * RECORD_SIZE..][..RECORD_SIZE],
            ))
        }
    }

    /// Inserts the given record into the table
    pub fn insert(&mut self, recno: usize, rec: &[u8]) {
        assert_eq!(rec.len(), RECORD_SIZE);
        self.handle
            .seek(SeekFrom::Start((recno * RECORD_SIZE) as u64 + self.offset))
            .unwrap();
        self.handle.write_all(rec).unwrap();
        self.handle.flush().unwrap();
    }

    // /// Gets the number of records in the table.
    // pub fn len(&self) -> usize {
    //     self.file.len() / RECORD_SIZE
    // }

    /// Flushes the database, blocking until all data is stably on disk.
    pub fn flush(&self) {
        self.handle.sync_data().unwrap();
        // self.file.flush().expect("flushing mmap somehow failed");
    }
}
