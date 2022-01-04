use std::ops::{Deref, DerefMut};

use cache_padded::CachePadded;
use parking_lot::lock_api::RawRwLock;
use parking_lot::RawRwLock as RrLock;

/// A highly concurrent table of records, the core datastructure in a database. This implements the low-level table structure, without any logic for lookup or insertion.
pub struct Table {
    /// Sharded locks
    locks: Vec<CachePadded<RrLock>>,
    /// Mmapped file
    file: memmap::MmapMut,
    /// Pointer to first element of mmapped file
    ptr: *mut u8,
}

impl Table {
    /// Creates a table, given a memmapped file.
    pub fn new(mut file: memmap::MmapMut) -> Self {
        let ptr = &mut file[0] as *mut u8;
        Self {
            locks: std::iter::repeat_with(|| CachePadded::new(RrLock::INIT))
                .take(128)
                .collect(),
            file,
            ptr,
        }
    }

    /// Gets the given record out of the table, as a record guard that can be read- or write-locked.
    pub fn get(&self, recno: usize) -> Option<RecordGuard<'_>> {
        let lock = self.get_lock(recno);
        if recno * 512 >= self.file.len() {
            None
        } else {
            Some(RecordGuard {
                ptr: unsafe { self.ptr.add(recno * 512) },
                lock,
            })
        }
    }

    /// Gets the number of records in the table.
    pub fn len(&self) -> usize {
        self.file.len() / 512
    }

    /// Flushes the database, blocking until all data is stably on disk.
    pub fn flush(&self) {
        self.file.flush().expect("flushing mmap somehow failed");
    }

    fn get_lock(&self, recno: usize) -> &RrLock {
        &self.locks[recno % self.locks.len()]
    }
}

pub struct RecordGuard<'a> {
    ptr: *mut u8,
    lock: &'a RrLock,
}

impl<'a> RecordGuard<'a> {
    /// Returns a guard that read-locks the record.
    pub fn read(&self) -> impl Deref<Target = [u8]> + '_ {
        self.lock.lock_shared();
        let slice = unsafe { std::slice::from_raw_parts(self.ptr, 512) };
        RecordReadGuard {
            lock: self.lock,
            inner: slice,
        }
    }

    /// Returns a guard that write-locks the record.
    pub fn write(&self) -> impl DerefMut<Target = [u8]> + '_ {
        self.lock.lock_exclusive();
        let slice = unsafe { std::slice::from_raw_parts_mut(self.ptr, 512) };
        RecordWriteGuard {
            lock: self.lock,
            inner: slice,
        }
    }
}

pub struct RecordReadGuard<'a> {
    lock: &'a RrLock,
    inner: &'a [u8],
}

impl<'a> Deref for RecordReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a> Drop for RecordReadGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            self.lock.unlock_shared();
        }
    }
}

pub struct RecordWriteGuard<'a> {
    lock: &'a RrLock,
    inner: &'a mut [u8],
}

impl<'a> Deref for RecordWriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a> DerefMut for RecordWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<'a> Drop for RecordWriteGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            self.lock.unlock_exclusive();
        }
    }
}
