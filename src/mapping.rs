use std::{
    borrow::Cow,
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
    time::Instant,
};

use arrayref::{array_mut_ref, array_ref};
use ethnum::U256;
use fs2::FileExt;
use memmap::{MmapMut, MmapOptions};
use parking_lot::RwLock;

use crate::{
    record::{new_record, Record, MAX_RECORD_BODYLEN},
    table::Table,
};

/// Concurrent hashtable that represents the database.
pub struct Mapping {
    inner: RwLock<MappingInner>,
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
        handle.write_all(&[0])?;
        handle.seek(SeekFrom::Start(0))?;

        let mut alloc_mmap = unsafe { MmapOptions::new().len(1 << 30).map_mut(&handle)? };
        // #[cfg(target_os = "linux")]
        // unsafe {
        //     use libc::MADV_RANDOM;
        //     libc::madvise(
        //         &mut alloc_mmap[0] as *mut u8 as _,
        //         alloc_mmap.len(),
        //         MADV_RANDOM,
        //     );
        // }
        if std::env::var("MESHANINA_PRELOAD").is_ok() {
            let mut sum = 0u8;
            for (count, chunk) in alloc_mmap.chunks(1048576).enumerate() {
                eprintln!("MESHANINA_PRELOAD {}/{}", count, alloc_mmap.len() / 1048576);
                for i in chunk {
                    sum = sum.wrapping_add(*i)
                }
            }
            log::warn!("sum {}", sum);
        }

        Ok(Mapping {
            inner: MappingInner {
                table: Table::new(handle.try_clone().unwrap(), 1 << 30),
                alloc_mmap,
            }
            .into(),
            _file: handle,
        })
    }

    /// Flushes the mapping to disk.
    pub fn flush(&self) {
        self.inner.read().flush();
    }

    /// Gets a key-value pair.
    pub fn get<'a>(&'a self, key: U256) -> Option<Cow<'a, [u8]>> {
        log::trace!("getting key {}", key);
        let inner = self.inner.read();
        let (top, top_length) = inner.get_atomic(atomic_key(key))?;
        if top_length <= MAX_RECORD_BODYLEN {
            Some(match top {
                Cow::Borrowed(s) => Cow::Borrowed(unsafe { extend_lifetime(s) }),
                Cow::Owned(o) => Cow::Owned(o),
            })
        } else {
            let mut toret = vec![0u8; top_length];
            for (i, chunk) in toret.chunks_mut(MAX_RECORD_BODYLEN).enumerate() {
                let (db_chunk, _) = inner.get_atomic(chunk_key(key, i))?;
                chunk.copy_from_slice(&db_chunk);
            }
            Some(Cow::Owned(toret))
        }
    }

    // /// Gets the allocation pointer, returning a potentially free allocation point.
    // fn get_alloc_ptr(&self) -> usize {
    //     let mm = self.alloc_mmap.read();
    //     u64::from_le_bytes(*array_ref![mm, 0, 8]) as usize
    // }

    // /// Sets the allocation pointer, given something known to be free.
    // fn set_alloc_ptr(&self, ptr: usize) {
    //     let mut mm = self.alloc_mmap.write();
    //     mm[0..8].copy_from_slice(&(ptr as u64).to_le_bytes());
    // }

    /// Inserts a key-value pair. Violating a one-to-one correspondence between keys and values is a **logic error** that may corrupt the database (though it will not cause memory safety failures)
    pub fn insert(&self, key: U256, value: &[u8]) {
        log::trace!("inserting key {}, value of length {}", key, value.len());
        let mut inner = self.inner.write();
        if value.len() <= MAX_RECORD_BODYLEN {
            inner
                .insert_atomic(atomic_key(key), value, None)
                .expect("database is full");
        } else {
            // insert the "top" key
            inner.insert_atomic(atomic_key(key), &[], Some(value.len()));
            // insert the chunks
            for (i, chunk) in value.chunks(MAX_RECORD_BODYLEN).enumerate() {
                inner.insert_atomic(chunk_key(key, i), chunk, None);
            }
        }
    }
}

struct MappingInner {
    table: Table,
    alloc_mmap: MmapMut,
}

impl MappingInner {
    /// Flush all.
    fn flush(&self) {
        self.alloc_mmap.flush().unwrap();
        self.table.flush()
    }

    /// Gets an atomic key-value pair.
    fn get_atomic<'a>(&'a self, key: U256) -> Option<(Cow<'a, [u8]>, usize)> {
        let start = Instant::now();
        for posn in probe_sequence(key) {
            let offset = (posn % (self.alloc_mmap.len() / 8)) * 8;
            // workaround for garbage bug
            if offset == 0 {
                continue;
            }

            let offset = u64::from_le_bytes(*array_ref![self.alloc_mmap, offset, 8]) as usize;
            if offset == 0 {
                break;
            }
            let record = self.table.get(offset)?;
            let is_borrowed = matches!(record, Cow::Borrowed(_));
            let record = Record(&record).validate()?;
            if record.key() != key {
                continue;
            }
            let elapsed = start.elapsed();
            if elapsed.as_millis() > 1 {
                log::debug!("get took {:?}", elapsed)
            }
            if is_borrowed {
                unsafe {
                    return Some((
                        Cow::Borrowed(extend_lifetime(record.value())),
                        record.length(),
                    ));
                }
            } else {
                return Some((Cow::Owned(record.value().to_vec()), record.length()));
            }
        }
        None
    }

    /// Inserts an atomic key-value pair.
    fn insert_atomic(
        &mut self,
        key: U256,
        value: &[u8],
        value_length: Option<usize>,
    ) -> Option<()> {
        // if self.get_atomic(key).is_some() {
        //     eprintln!("already existent key {}", key);
        //     // return Some(());
        // }
        self.really_insert_atomic(key, value, value_length)
    }

    fn really_insert_atomic(
        &mut self,
        key: U256,
        value: &[u8],
        value_length: Option<usize>,
    ) -> Option<()> {
        let start = Instant::now();
        // let start = Instant::now();
        // scopeguard::defer!(eprintln!("atomic insert took {:?}", start.elapsed()));
        let ptr = u64::from_le_bytes(*array_ref![self.alloc_mmap, 0, 8]) as usize;
        let pre_loop = start.elapsed();
        for (i, posn) in probe_sequence(key).enumerate() {
            // dbg!(posn);
            if i > 0 {
                dbg!(i);
            }
            let offset = (posn % (self.alloc_mmap.len() / 8)) * 8;
            if offset == 0 {
                continue;
            }

            let offset_ptr = array_mut_ref![self.alloc_mmap, offset, 8];
            let should_overwrite = if *offset_ptr == [0; 8] {
                true
            } else {
                let offset = u64::from_le_bytes(*offset_ptr) as usize;
                let record = self.table.get(offset).expect("wtf");
                if let Some(record) = Record(&record).validate() {
                    // eprintln!("validating existing record ({}, {})", i, offset);
                    if record.key() == key {
                        // eprintln!("okay let's just stop now ({})", key);
                        return Some(());
                    }
                    false
                } else {
                    true
                }
            };
            let pre_overwrite = start.elapsed();
            if should_overwrite {
                for offset in 0.. {
                    if ptr + offset == 0 {
                        continue;
                    }
                    let rec = new_record(key, value_length.unwrap_or(value.len()), value);
                    self.table.insert(ptr + offset, &rec);
                    *offset_ptr = ((ptr + offset) as u64).to_le_bytes();
                    let to_write = ptr + offset + 1;
                    self.alloc_mmap[0..8].copy_from_slice(&(to_write as u64).to_le_bytes());
                    let elapsed = start.elapsed();
                    // if elapsed.as_millis() > 1 {
                    log::debug!(
                        "insert took {:?} (pre_loop {:?}, pre_overwrite {:?}, i = {}, offset = {})",
                        elapsed,
                        pre_loop,
                        pre_overwrite,
                        i,
                        offset
                    );
                    // }
                    return Some(());
                }
            }
        }
        None
    }
}

unsafe fn extend_lifetime<'b, T: ?Sized>(r: &'b T) -> &'static T {
    std::mem::transmute(r)
}

/// A probe sequence.
fn probe_sequence(key: U256) -> impl Iterator<Item = usize> {
    let i = key.as_u64() as usize;
    (0..).map(move |v| i + v)
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
        let test_vector = b"[63] Super Sent., lib. 1 d. 1 q. 1 pr. Finito prooemio, hoc est initium praesentis operis in quo Magister divinorum nobis doctrinam tradere intendit quantum ad inquisitionem veritatis et destructionem erroris: unde et argumentativo modo procedit in toto opere: et praecipue argumentis ex auctoritatibus sumptis. Dividitur autem in duas partes: in quarum prima inquirit ea de quibus agendum est, et ordinem agendi; in secunda prosequitur suam intentionem: et in duas partes dividitur. Secunda ibi: hic considerandum est utrum virtutibus sit utendum, an fruendum. Ea autem de quibus in hac doctrina considerandum est, cadunt in considerationem hujus doctrinae, secundum quod ad aliquid unum referuntur, scilicet Deum, a quo et ad quem sunt. Et ideo ea de quibus agendum est dividit per absolutum et relatum: unde dividitur in partes duas. In prima ponit divisionem eorum de quibus agendum est per absolutum et relatum secundum cognitionem, in secunda secundum desiderium, ibi: id ergo in rebus considerandum. Circa primum duo facit. Primo ponit divisionem eorum de quibus agendum est, in res et signa, quae ad cognitionem rerum ducunt; secundo concludit ordinem agendi, ibi: cumque his intenderit theologorum speculatio studiosa atque modesta, divinam Scripturam formam praescriptam in doctrina tenere advertet. In primo tria facit. Primo ponit divisionem; secundo probat per auctoritatem, ibi: ut enim egregius doctor Augustinus ait; tertio ponit membrorum divisionis expositionem, ibi: proprie autem hic res appellantur quae non ad significandum aliquid adhibentur: ubi primo exponit quid sit res; secundo quid sit signum, ibi: signa vero quorum usus est in significando; tertio utriusque comparationem, ibi: omne igitur signum etiam res aliqua est. Id ergo in rebus considerandum est. Hic, dimissis signis, subdividit res per absolutum et relatum ex parte desiderii, scilicet per fruibile, quod propter se desideratur, et utibile, cujus desiderium ad aliud refertur: et dividitur in partes duas. Primo ponit divisionem; secundo epilogat et concludit intentionem et ordinem, ibi: omnium igitur quae dicta sunt, ex quo de rebus specialiter tractavimus, haec summa est. Prima in tres. Primo ponit divisionem; secundo partium manifestationem, ibi: illa quibus fruendum est, nos beatos faciunt; tertio movet dubitationes, ibi: cum autem homines, qui fruuntur et utuntur aliis rebus, res aliquae sint, quaeritur utrum se frui debeant, an uti, an utrumque. In secunda duo facit. Primo manifestat divisionem; secundo ponit quamdam contrarietatem, et solvit, ibi: notandum vero, quod idem Augustinus (...) sic dicit. Circa primum duo facit. Primo manifestat partes divisionis per definitiones; secundo quantum ad supposita, ibi: res igitur quibus fruendum est, sunt pater, et filius, et spiritus sanctus. Circa primum quatuor facit. Primo definit fruibilia per effectum; secundo utibilia, ibi: istis quibus utendum est, tendentes ad beatitudinem adjuvamur; tertio definit utentia, et fruentia ibi: res vero quae fruuntur et utuntur, nos sumus; quarto definit uti et frui ad probationem totius: frui autem est amore alicui rei inhaerere propter seipsam. Et eodem ordine procedit manifestando secundum supposita. Notandum vero, quod idem Augustinus (...) aliter quam supra accipiens frui et uti, sic dicit. Hic ponit contrarietatem ad haec tria. Primo ponit diversam assignationem uti et frui; secundo concludit contrarietatem ad praedicta, ibi: et attende, quod videtur Augustinus dicere illos frui tantum qui in re gaudent; tertio ponit solutionem, ibi: haec ergo quae sibi contradicere videntur, sic determinamus. Et primo solvit per divisionem; secundo per interemptionem, ibi: potest etiam dici, quod qui fruitur etiam in hac vita non tantum habet gaudium spei, sed etiam rei. Cum autem homines, qui fruuntur et utuntur aliis rebus, res aliquae sint, quaeritur, utrum se frui debeant, an uti, an utrumque. Hic movet dubitationes de habitudine eorum quae pertinent ad invicem: et primo quaerit de utentibus et fruentibus, an sint utibilia vel fruibilia; secundo de fruibilibus, scilicet de Deo, utrum sit utens nobis vel fruens, ibi: sed cum Deus diligat nos (...) quaerit Augustinus quomodo diligat, an ut utens, an ut fruens; tertio de quibusdam utibilibus, utrum sint fruibilia, ibi: hic considerandum est, utrum virtutibus sit utendum, an fruendum. Quaelibet harum partium dividitur in quaestionem et solutionem. Hic quaeruntur tria: primo, de uti et frui. Secundo, de utibilibus et fruibilibus. Tertio, de utentibus et fruentibus. Circa primum quaeruntur duo: 1 quid sit frui secundum rem; 2 quid sit uti secundum rem.
.";
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
