use std::time::Instant;

use rand::RngCore;

const DB_SIZE: u64 = 1_000_000;
const VALUE_SIZE: usize = 200;

trait BenchTarget {
    fn name(&self) -> &'static str;
    fn insert(&self, k: [u8; 32], v: &[u8]);
    fn get(&self, k: [u8; 32]) -> Option<Vec<u8>>;
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
    run_once(&m2);
}

fn run_once(target: &impl BenchTarget) {
    let start = Instant::now();
    for ctr in 0..DB_SIZE {
        let ctr = ctr.to_le_bytes();
        let key = blake3::hash(&ctr);
        let mut value = vec![0u8; VALUE_SIZE];
        rand::thread_rng().fill_bytes(&mut value);
        target.insert(*key.as_bytes(), &value);
    }
    eprintln!("{DB_SIZE} create: {:?}", start.elapsed())
}
