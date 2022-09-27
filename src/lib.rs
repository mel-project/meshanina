use std::{path::Path, sync::Arc, time::Duration};

use bytes::Bytes;
use parking_lot::RwLock;
use table::Table;

pub mod legacy;
mod record;
mod table;

/// An on-disk, append-only Meshanina database.
pub struct Mapping {
    inner: Arc<RwLock<Table>>,
}

impl Mapping {
    /// Opens a mapping, given a filename.
    pub fn open(fname: impl AsRef<Path>) -> std::io::Result<Self> {
        let table = Table::open(fname.as_ref())?;
        let inner = Arc::new(RwLock::new(table));
        let inner_weak = Arc::downgrade(&inner);
        // TODO a better, "batch-timer" approach
        std::thread::Builder::new()
            .name("mesh-flush".into())
            .spawn(move || loop {
                if let Some(inner) = inner_weak.upgrade() {
                    inner.write().flush();
                    std::thread::sleep(Duration::from_secs(30))
                } else {
                    return;
                }
            })
            .unwrap();
        Ok(Mapping { inner })
    }

    /// Flushes the mapping to disk.
    pub fn flush(&self) {
        // TODO blocking reader is probably not too nice
        self.inner.write().flush();
    }

    /// Gets a key-value pair.
    pub fn get(&self, key: [u8; 32]) -> Option<Bytes> {
        Some(Bytes::from(
            lz4_flex::decompress_size_prepended(&self.inner.read().lookup(key)?).expect("db fail"),
        ))
    }

    /// Inserts a key-value pair.
    pub fn insert(&self, key: [u8; 32], value: &[u8]) {
        self.inner
            .write()
            .insert(key, &lz4_flex::compress_prepend_size(value));
    }
}

#[cfg(test)]
mod tests {
    use arrayref::array_ref;

    use super::*;

    #[test]
    fn db_simple() {
        let tab = Mapping::open(Path::new("/tmp/test_meshanina.db")).unwrap();
        for ctr in 0u64..100 {
            let k = *blake3::hash(format!("key{}", ctr).as_bytes()).as_bytes();
            tab.insert(k, &ctr.to_le_bytes());
            let b = tab.get(k).unwrap();
            assert_eq!(array_ref![&b, 0, 8], &ctr.to_le_bytes());
        }
    }
}
