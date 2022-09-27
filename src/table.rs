use std::{borrow::Cow, sync::Arc};

use arrayref::array_ref;
use memmap::MmapMut;

use crate::record::{Record, RecordPtr};

/// Low-level interface to the database.
pub struct Table {
    /// Root record. Must be a HAMT!
    root: Record<'static>,
    // /// Dirty or not
    // dirty: bool,
    // /// Mmap of the file
    // mmap: MmapMut,
    // /// Append-writer
    // writer: std::fs::File,
}

impl Table {
    /// Looks up a key, returning the value if possible.
    pub fn lookup(&self, key: [u8; 32]) -> Option<Cow<'_, [u8]>> {
        let mut ptr = self.root.clone();
        // TODO use all the bits
        let mut ikey = u128::from_le_bytes(*array_ref![&key, 0, 16]);
        loop {
            match ptr {
                Record::Data(d_key, d_v) => {
                    if key != d_key {
                        return None;
                    } else {
                        return Some(d_v.clone());
                    }
                }
                Record::HamtNode(_, bitmap, ptrs) => {
                    let hindex = (ikey & 0b111111) as u32;
                    if (bitmap >> hindex) & 1 == 1 {
                        let idx = (bitmap & ((1 << hindex) - 1)).count_ones();
                        let p = ptrs[idx as usize].clone();
                        ptr = p.load(|p| todo!());
                        ikey >>= 6;
                    } else {
                        return None;
                    }
                }
            }
        }
    }

    /// Inserts a key. Does nothing if the key already exists
    pub fn insert(&mut self, key: [u8; 32], value: &[u8]) {
        if self.lookup(key).is_none() {
            // insert from root
            self.root = self.insert_helper(
                0,
                self.root.clone(),
                u128::from_le_bytes(*array_ref![&key, 0, 16]),
                key,
                value,
            )
        }
    }

    fn insert_helper<'a>(
        &mut self,
        depth: usize,
        hamt: Record<'a>,
        ikey: u128,
        key: [u8; 32],
        value: &[u8],
    ) -> Record<'a> {
        match hamt {
            Record::Data(existing_k, existing_v) => {
                let a =
                    self.insert_helper(depth, Record::HamtNode(false, 0, vec![]), ikey, key, value);
                let existing_ikey = u128::from_le_bytes(*array_ref![&existing_k, 0, 16]);
                self.insert_helper(
                    depth,
                    a,
                    existing_ikey >> (6 * depth),
                    existing_k,
                    &existing_v,
                )
            }
            Record::HamtNode(r, mut bitmap, mut ptrs) => {
                let hindex = (ikey & 0b111111) as u32;
                log::trace!("depth={depth}, hindex={hindex}, bitmap={:b}", bitmap);
                if (bitmap >> hindex) & 1 == 1 {
                    let idx = (bitmap & ((1 << hindex) - 1)).count_ones();
                    let p = ptrs[idx as usize].clone();
                    // recurse down
                    let c =
                        self.insert_helper(depth + 1, p.load(|p| todo!()), ikey >> 6, key, value);
                    ptrs[idx as usize] = RecordPtr::InMemory(Arc::new(c));
                } else {
                    // nothing here. this means we need to expand
                    bitmap |= 1 << hindex;
                    let idx = (bitmap & ((1 << hindex) - 1)).count_ones();
                    log::trace!("depth={depth} idx={idx}");
                    ptrs.insert(
                        idx as usize,
                        RecordPtr::InMemory(Arc::new(Record::Data(key, value.to_vec().into()))),
                    );
                }
                Record::HamtNode(r, bitmap, ptrs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hamt_simple() {
        let mut tab = Table {
            root: Record::HamtNode(true, 0, vec![]),
        };
        for ctr in 0u64..1000 {
            let k = *blake3::hash(format!("key{}", ctr).as_bytes()).as_bytes();
            tab.insert(k, &ctr.to_le_bytes());
            let b = tab.lookup(k).unwrap();
            assert_eq!(array_ref![&b, 0, 8], &ctr.to_le_bytes());
        }
    }
}
