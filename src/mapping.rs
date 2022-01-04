use ethnum::U256;

use crate::table::Table;

/// Concurrent hashtable that represents the database.
pub struct Mapping {
    inner: Table,
}

impl Mapping {
    /// Gets a key-value pair.
    pub fn get(&self, key: U256) -> Option<&[u8]> {
        let init_posn = hash(key, self.inner.len());
    }
}

fn hash(key: U256, modulo: usize) -> usize {
    (key % U256::from(modulo as u64)).as_usize()
}
