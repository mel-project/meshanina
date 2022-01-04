use std::{
    borrow::Cow,
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use ethnum::U256;
use fs2::FileExt;

use crate::{
    record::{write_record, Record, MAX_RECORD_BODYLEN},
    table::Table,
};

/// Concurrent hashtable that represents the database.
pub struct Mapping {
    inner: Table,
}

impl Mapping {
    /// Opens a mapping, given a filename.
    pub fn open(fname: &Path) -> std::io::Result<Self> {
        let mut handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fname)?;
        handle.try_lock_exclusive()?;
        // Create at least a 247.9 GB sparse file.
        handle.seek(SeekFrom::Start(1 << 38))?;
        handle.write(&[0])?;
        handle.seek(SeekFrom::Start(0))?;
        // Now it's safe to memmap the file, because it's EXCLUSIVELY locked to this process.
        let memmap = unsafe { memmap::MmapMut::map_mut(&handle)? };
        Ok(Mapping {
            inner: Table::new(memmap),
        })
    }

    /// Gets a key-value pair.
    pub fn get<'a>(&'a self, key: U256) -> Option<Cow<'a, [u8]>> {
        log::trace!("getting key {}", key);
        let init_posn = hash(key, self.inner.len());
        // Linear probing
        for offset in 0.. {
            let attempt = self.inner.get(init_posn + offset)?;
            let read_lock = attempt.read();
            if let Some(record) = Record(&read_lock).validate() {
                if record.key() == key {
                    log::trace!(
                        "found key {}, bound to value of length {}",
                        key,
                        record.length()
                    );
                    // SAFETY: once a record is safely on-disk, there's no way it can ever change again.
                    // Therefore, we can let go of the read-lock and return a unlocked byteslice reference.
                    unsafe {
                        return Some(Cow::Borrowed(extend_lifetime(record.value())));
                    }
                }
            } else {
                return None;
            }
        }
        unreachable!()
    }

    /// Inserts a key-value pair. Violating a one-to-one correspondence between keys and values is a **logic error** that may corrupt the database (though it will not cause memory safety failures)
    pub fn insert(&self, key: U256, value: &[u8]) {
        assert!(value.len() <= MAX_RECORD_BODYLEN);
        let init_posn = hash(key, self.inner.len());
        // Linear probing, but with write-locks.
        for offset in 0.. {
            let attempt = self
                .inner
                .get(init_posn + offset)
                .expect("ran out of slots");
            let mut write_lock = attempt.write();
            if let Some(record) = Record(&write_lock).validate() {
                if record.key() == key {
                    return;
                }
            } else {
                // This means that we found an empty slot of some sort. We can therefore write a record.
                write_record(&mut write_lock, key, value.len(), value);
                debug_assert!(Record(&write_lock).validate().is_some());
                return;
            }
        }
    }
}

unsafe fn extend_lifetime<'b, T: ?Sized>(r: &'b T) -> &'static T {
    std::mem::transmute(r)
}

fn hash(key: U256, modulo: usize) -> usize {
    (key % U256::from(modulo as u64)).as_usize()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::Mapping;

    #[test]
    fn simple_insert_get() {
        let fname = PathBuf::from("/tmp/test.db");
        let mapping = Mapping::open(&fname).unwrap();
        mapping.insert(123u8.into(), b"hello world");
        dbg!(mapping.get(123u8.into()).unwrap());
    }
}
