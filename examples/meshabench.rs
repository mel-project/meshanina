use std::{collections::BTreeMap, path::Path, time::Instant};

use ethnum::U256;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use rand::RngCore;

static DB_SIZE: Lazy<u64> = Lazy::new(|| std::env::var("DB_SIZE").unwrap().parse().unwrap());
const VALUE_SIZE: usize = 600;

trait BenchTarget {
    fn name(&self) -> &'static str;
    fn insert(&self, k: [u8; 32], v: &[u8]);
    fn get(&self, k: [u8; 32]) -> Option<Vec<u8>>;
}

impl BenchTarget for RwLock<BTreeMap<[u8; 32], Vec<u8>>> {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn insert(&self, k: [u8; 32], v: &[u8]) {
        self.write().insert(k, v.to_vec());
    }

    fn get(&self, k: [u8; 32]) -> Option<Vec<u8>> {
        self.read().get(&k).map(|v| v.to_vec())
    }
}

impl BenchTarget for meshanina::Mapping {
    fn name(&self) -> &'static str {
        "meshanina"
    }

    fn insert(&self, k: [u8; 32], v: &[u8]) {
        self.insert(k, v)
    }

    fn get(&self, k: [u8; 32]) -> Option<Vec<u8>> {
        self.get(k).map(|b| b.to_vec())
    }
}

fn main() {
    let m2 = meshanina::Mapping::open("v2.db").unwrap();

    let in_mem = RwLock::new(BTreeMap::new());
    run_once(&m2);

    run_once(&in_mem);
}

fn run_once(target: &impl BenchTarget) {
    let name = target.name();
    let start = Instant::now();
    for ctr in 0..*DB_SIZE {
        let ctr = ctr.to_le_bytes();
        let key = blake3::hash(&ctr);
        let mut value = vec![0u8; VALUE_SIZE];
        rand::thread_rng().fill_bytes(&mut value);
        target.insert(*key.as_bytes(), &value);
    }
    eprintln!("{name}: {} create: {:?}", *DB_SIZE, start.elapsed());
    let start = Instant::now();
    for _ in 0..*DB_SIZE {
        let ctr = fastrand::u64(0..*DB_SIZE);
        let ctr = ctr.to_le_bytes();
        let key = blake3::hash(&ctr);
        let _ = target.get(*key.as_bytes());
    }
    eprintln!("{name}: {} access: {:?}", *DB_SIZE, start.elapsed());
}
