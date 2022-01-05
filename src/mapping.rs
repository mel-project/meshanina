use std::{
    borrow::Cow,
    fs::File,
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
    _file: File,
}

unsafe impl Sync for Mapping {}
unsafe impl Send for Mapping {}

impl Mapping {
    /// Opens a mapping, given a filename.
    pub fn open(fname: &Path) -> std::io::Result<Self> {
        let mut handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fname)?;
        handle.try_lock_exclusive()?;
        // Create at least 274.9 GB of empty space.
        handle.seek(SeekFrom::Start(1 << 38))?;
        handle.write(&[0])?;
        handle.seek(SeekFrom::Start(0))?;
        // Now it's safe to memmap the file, because it's EXCLUSIVELY locked to this process.
        let memmap = unsafe { memmap::MmapMut::map_mut(&handle)? };
        Ok(Mapping {
            inner: Table::new(memmap),
            _file: handle,
        })
    }

    /// Gets a key-value pair.
    pub fn get<'a>(&'a self, key: U256) -> Option<Cow<'a, [u8]>> {
        log::trace!("getting key {}", key);
        let (top, top_length) = self.get_atomic(atomic_key(key))?;
        if top_length <= MAX_RECORD_BODYLEN {
            Some(Cow::Borrowed(top))
        } else {
            let mut toret = vec![0u8; top_length];
            for (i, chunk) in toret.chunks_mut(MAX_RECORD_BODYLEN).enumerate() {
                let (db_chunk, _) = self.get_atomic(chunk_key(key, i))?;
                chunk.copy_from_slice(db_chunk);
            }
            Some(Cow::Owned(toret))
        }
    }

    /// Gets an atomic key-value pair.
    fn get_atomic<'a>(&'a self, key: U256) -> Option<(&'a [u8], usize)> {
        // Linear probing
        for posn in probe_sequence(key) {
            let posn = posn % self.inner.len();
            let attempt = self.inner.get(posn)?;
            let read_lock = attempt.read();
            if let Some(record) = Record(&read_lock).validate() {
                if record.key() == key {
                    log::trace!(
                        "found atomic key {}, bound to value of length {}",
                        key,
                        record.length()
                    );
                    // SAFETY: once a record is safely on-disk, there's no way it can ever change again.
                    // Therefore, we can let go of the read-lock and return a unlocked byteslice reference.
                    unsafe {
                        return Some((extend_lifetime(record.value()), record.length()));
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
        log::trace!("inserting key {}, value of length {}", key, value.len());
        if value.len() <= MAX_RECORD_BODYLEN {
            self.insert_atomic(atomic_key(key), value, None)
                .expect("database is full")
        } else {
            // insert the "top" key
            self.insert_atomic(atomic_key(key), &[], Some(value.len()));
            // insert the chunks
            for (i, chunk) in value.chunks(MAX_RECORD_BODYLEN).enumerate() {
                self.insert_atomic(chunk_key(key, i), chunk, None);
            }
        }
    }

    /// Inserts an atomic key-value pair.
    fn insert_atomic(&self, key: U256, value: &[u8], value_length: Option<usize>) -> Option<()> {
        log::trace!(
            "atomic-inserting key {}, value of length {}",
            key,
            value.len()
        );
        assert!(value.len() <= MAX_RECORD_BODYLEN);
        // Linear probing, but with write-locks.
        for posn in probe_sequence(key) {
            let posn = posn % self.inner.len();
            let attempt = self.inner.get(posn)?;
            let mut write_lock = attempt.write();
            let can_overwrite = if let Some(record) = Record(&write_lock).validate() {
                if record.key() == key {
                    return Some(());
                } else {
                    false
                }
            } else {
                true
            };
            if can_overwrite {
                // This means that we found an empty slot of some sort. We can therefore write a record.
                write_record(
                    &mut write_lock,
                    key,
                    value_length.unwrap_or_else(|| value.len()),
                    value,
                );
                debug_assert!(Record(&write_lock).validate().is_some());
                return Some(());
            }
        }
        None
    }
}

unsafe fn extend_lifetime<'b, T: ?Sized>(r: &'b T) -> &'static T {
    std::mem::transmute(r)
}

/// A probe sequence that tries smaller "subdatabases" first, to try to make things more compact.
fn probe_sequence(key: U256) -> impl Iterator<Item = usize> {
    (4..10)
        .map(|p| 1u64 << (p * 4))
        .cycle()
        .enumerate()
        .map(move |(offset, modulo)| ((key.as_u64() + offset as u64) % modulo) as usize)
        .take(10000)
}

// Atomic key
fn atomic_key(key: U256) -> U256 {
    U256::from_le_bytes(*blake3::hash(&key.to_le_bytes()).as_bytes())
}

// Non-atomic chunk key
fn chunk_key(parent: U256, index: usize) -> U256 {
    U256::from_le_bytes(
        *blake3::keyed_hash(&parent.to_le_bytes(), &(index as u64).to_le_bytes()).as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::Mapping;

    #[test]
    fn simple_insert_get() {
        let test_vector = b"Respondeo dicendum sacram doctrinam esse scientiam. Sed sciendum est quod duplex est scientiarum genus. Quaedam enim sunt, quae procedunt ex principiis notis lumine naturali intellectus, sicut arithmetica, geometria, et huiusmodi. Quaedam vero sunt, quae procedunt ex principiis notis lumine superioris scientiae, sicut perspectiva procedit ex principiis notificatis per geometriam, et musica ex principiis per arithmeticam notis. Et hoc modo sacra doctrina est scientia, quia procedit ex principiis notis lumine superioris scientiae, quae scilicet est scientia Dei et beatorum. Unde sicut musica credit principia tradita sibi ab arithmetico, ita doctrina sacra credit principia revelata sibi a Deo.";
        let fname = PathBuf::from("/tmp/test.db");
        let mapping = Mapping::open(&fname).unwrap();
        // first test a composite value
        mapping.insert(0u32.into(), test_vector);
        assert_eq!(mapping.get(0u32.into()).unwrap().as_ref(), test_vector);
        // then try to fill the db
        for i in 1u32..100 {
            mapping.insert(i.into(), b"hello world");
            assert_eq!(mapping.get(i.into()).unwrap().as_ref(), b"hello world");
        }
    }
}
