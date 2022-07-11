use std::{
    borrow::Cow,
    fs::File,
    io::{Seek, SeekFrom, Write},
    time::{Duration, Instant},
};

use flume::Sender;
use memmap::MmapOptions;

use crate::record::RECORD_SIZE;

/// A highly concurrent table of records, the core datastructure in a database. This implements the low-level table structure, without any logic for lookup or insertion.
pub struct Table {
    /// handle
    handle: File,
    /// Offset
    offset: u64,

    /// Sender for read request scheduling
    send_req: Sender<(usize, oneshot::Sender<&'static [u8]>)>,
}

impl Table {
    /// Creates a table, given a memmapped file.
    pub fn new(handle: File, offset: u64) -> Self {
        let mut table_mmap = unsafe {
            MmapOptions::new()
                .offset(offset)
                .len(1 << 38)
                .map_mut(&handle)
                .unwrap()
        };
        #[cfg(target_os = "linux")]
        unsafe {
            use libc::MADV_RANDOM;
            libc::madvise(
                &mut table_mmap[0] as *mut u8 as _,
                table_mmap.len(),
                MADV_RANDOM,
            );
        }

        let (send_req, recv_req) = flume::unbounded::<(usize, oneshot::Sender<&'static [u8]>)>();
        // make a thread that sorts stuff
        std::thread::Builder::new()
            .name("mesh-sched".into())
            .spawn::<_, Option<()>>(move || {
                let mut queue = vec![];
                loop {
                    let first = recv_req.recv().ok()?;
                    queue.push(first);
                    while let Ok(n) = recv_req.try_recv() {
                        queue.push(n);
                    }
                    queue.sort_unstable_by_key(|d| d.0);
                    // if queue.len() > 1 {
                    //     log::debug!("batch of length {}: {:?}", queue.len(), queue);
                    // }
                    for (recno, _) in queue.iter() {
                        let r = &table_mmap[recno * RECORD_SIZE..][..RECORD_SIZE];
                        let start = Instant::now();
                        let sum = r.iter().fold(0, |a, b| a ^ b);
                        log::trace!("xor {} took {:?}", sum, start.elapsed());
                    }
                    for (recno, send_resp) in queue.drain(..) {
                        let r = &table_mmap[recno * RECORD_SIZE..][..RECORD_SIZE];
                        let _ = send_resp.send(unsafe { std::mem::transmute(r) });
                    }
                }
            })
            .unwrap();

        Self {
            handle,
            offset,
            send_req,
        }
    }

    /// Gets the given record out of the table
    pub fn get(&self, recno: usize) -> Option<Cow<[u8]>> {
        let (s, r) = oneshot::channel();
        self.send_req.send((recno, s)).unwrap();
        let r = r.recv().unwrap();
        Some(Cow::Borrowed(r))
    }

    /// Inserts the given record into the table
    pub fn insert(&mut self, recno: usize, rec: &[u8]) {
        assert_eq!(rec.len(), RECORD_SIZE);
        self.handle
            .seek(SeekFrom::Start((recno * RECORD_SIZE) as u64 + self.offset))
            .unwrap();
        self.handle.write_all(rec).unwrap();
        // self.handle.flush().unwrap();
    }

    // /// Gets the number of records in the table.
    // pub fn len(&self) -> usize {
    //     self.file.len() / RECORD_SIZE
    // }

    /// Flushes the database, blocking until all data is stably on disk.
    pub fn flush(&self) {
        let start = Instant::now();
        self.handle.sync_data().unwrap();
        log::debug!("data sync took {:?}", start.elapsed());
        // self.file.flush().expect("flushing mmap somehow failed");
    }
}
