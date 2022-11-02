use std::{
    borrow::Cow,
    io::{BufWriter, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use arrayref::array_ref;
use fs2::FileExt;
use itertools::Itertools;
use memmap::{MmapMut, MmapOptions};
use rand::Rng;

use crate::record::{Record, RecordPtr};

/// Low-level interface to the database.
pub struct Table {
    /// Root record. Must be a HAMT!
    root: Record<'static>,
    /// Dirty or not
    dirty: bool,
    /// The secret divider
    divider: u128,
    /// Mmap of the file
    mmap: MmapMut,
    /// Append-writer
    writer: BufWriter<std::fs::File>,
    /// Pointer
    ptr: u64,
}

impl Table {
    /// Opens a new file, doing recovery as needed.
    pub fn open(fname: &Path) -> std::io::Result<Self> {
        let mut handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fname)?;
        handle.try_lock_exclusive()?;
        // ensure the existence of the reserved region
        if handle.seek(SeekFrom::End(0))? < 4096 {
            handle.set_len(4096)?;
            handle.seek(SeekFrom::Start(0))?;
            handle.write_all(b"meshanina2")?;
            let random_divider: u128 = rand::thread_rng().gen();
            handle.write_all(&random_divider.to_le_bytes())?;
        }
        // mmap everything
        let mut mmap = unsafe { MmapOptions::new().len(1 << 39).map_mut(&handle).unwrap() };
        // when possible (on linux), advise the OS that we're gonna read from the mmap pretty randomly, so tricks like readahead aren't gonna help at all
        #[cfg(target_os = "linux")]
        unsafe {
            use libc::MADV_RANDOM;
            libc::madvise(&mut mmap[0] as *mut u8 as _, mmap.len(), MADV_RANDOM);
        }
        let divider = u128::from_le_bytes(*array_ref![&mmap, 10, 16]);
        handle.seek(SeekFrom::Start(0))?;
        let file_len = handle.seek(SeekFrom::End(0))?;
        // if the file is long, we attempt to find the last valid HAMT root node.
        if file_len > 4096 {
            log::debug!("file length {file_len}, finding last HAMT node");
            // find candidates by searching the last 1 MiB for the magic divider
            let search_space = &mmap[4096..file_len as usize];
            let search_space =
                &search_space[search_space.len() - (100_000_000).min(search_space.len())..];
            let posn_in_space = search_space
                .windows(16)
                .positions(|window| window == divider.to_le_bytes())
                .collect_vec();
            if posn_in_space.is_empty() {
                panic!("db corruption: no dividers found in the last part of db")
            }
            for posn in posn_in_space.into_iter().rev() {
                if let Ok(rec) = Record::new_borrowed(&search_space[posn..], divider) {
                    if rec.is_root() {
                        let ptr = handle.stream_position()?;
                        return Ok(Table {
                            root: rec.into_owned(),
                            dirty: false,
                            divider,
                            mmap,
                            writer: BufWriter::with_capacity(1_000_000, handle),
                            ptr,
                        });
                    }
                }
            }
            panic!("db corruption: dividers found but none of the elements were valid roots")
        }
        let ptr = handle.stream_position()?;
        Ok(Table {
            root: Record::HamtNode(true, 0, vec![]),
            dirty: false,
            divider,
            mmap,
            writer: BufWriter::with_capacity(1_000_000, handle),
            ptr,
        })
    }

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
                        ptr = p.load(|p| self.load_record(p));
                        ikey >>= 6;
                    } else {
                        return None;
                    }
                }
            }
        }
    }

    /// Looks up a single record.
    fn load_record(&self, ptr: u64) -> Record<'_> {
        Record::new_borrowed(&self.mmap[(ptr as usize)..], self.divider)
            .expect("db corruption: dangling ptr")
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
            );

            self.dirty = true;
            if fastrand::usize(0..1000) == 0 {
                self.flush(false)
            }
        }
    }

    /// Flushes everything to disk. The caller specifies whether or not to actually fully fsync
    pub fn flush(&mut self, fsync: bool) {
        if self.dirty {
            let (_, new_root) = self.flush_helper(self.root.clone());
            self.writer.flush().expect("flush failed");
            if fsync {
                self.writer.get_ref().sync_all().expect("fs fail");
            }
            self.dirty = false;
            self.root = new_root;
        }
    }

    fn flush_helper<'a>(&mut self, ptr: Record<'a>) -> (u64, Record<'a>) {
        // first, replace everything with flushed stuff
        let ptr = match ptr {
            Record::HamtNode(r, b, pp) => Record::HamtNode(
                r,
                b,
                pp.into_iter()
                    .map(|p| match p {
                        RecordPtr::InMemory(m) => {
                            RecordPtr::OnDisk(self.flush_helper((*m).clone()).0)
                        }
                        p => p,
                    })
                    .collect(),
            ),
            p => p,
        };
        let curr_posn = self.ptr;
        let n = ptr
            .write_bytes(self.divider, &mut self.writer)
            .expect("fs fail");
        self.ptr += n as u64;
        (curr_posn, ptr)
    }

    fn insert_helper(
        &mut self,
        depth: usize,
        hamt: Record<'static>,
        ikey: u128,
        key: [u8; 32],
        value: &[u8],
    ) -> Record<'static> {
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
                // eprintln!("depth={depth}, hindex={hindex}, bitmap={:b}", bitmap);
                if (bitmap >> hindex) & 1 == 1 {
                    let idx = (bitmap & ((1 << hindex) - 1)).count_ones();
                    let p = ptrs[idx as usize].clone();
                    let ptr = p.load(|p| self.load_record(p).into_owned());
                    // recurse down
                    let c = self.insert_helper(depth + 1, ptr, ikey >> 6, key, value);
                    ptrs[idx as usize] = RecordPtr::InMemory(Arc::new(c));
                } else {
                    // nothing here. this means we need to expand
                    bitmap |= 1 << hindex;
                    let idx = (bitmap & ((1 << hindex) - 1)).count_ones();
                    log::trace!("depth={depth} idx={idx}");
                    let record = Record::Data(key, value.to_vec().into());
                    // let (addr, _) = self.flush_helper(record);
                    ptrs.insert(idx as usize, RecordPtr::InMemory(Arc::new(record)));
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
        let mut tab = Table::open(Path::new("/tmp/test_meshanina.db")).unwrap();
        for ctr in 0u64..100 {
            let k = *blake3::hash(format!("key{}", ctr).as_bytes()).as_bytes();
            tab.insert(k, &ctr.to_le_bytes());
            if ctr % 17 == 0 {
                tab.flush(false);
            }
            let b = tab.lookup(k).unwrap();
            assert_eq!(array_ref![&b, 0, 8], &ctr.to_le_bytes());
        }
    }
}
