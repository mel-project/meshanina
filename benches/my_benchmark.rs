use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meshanina::Mapping;
use once_cell::sync::Lazy;

static DATABASE: Lazy<Mapping> = Lazy::new(|| {
    let fname = PathBuf::from("/tmp/bench.db");
    let mapping = Mapping::open(&fname).unwrap();
    for i in 0u32..50 {
        mapping.insert(
            i.into(),
            &std::iter::repeat_with(|| fastrand::u8(..))
                .take(400)
                .collect::<Vec<_>>(),
        );
    }
    mapping
});

fn bench_gets(n: u64) {
    for i in 0..n {
        let _ = black_box(DATABASE.get(i.into()));
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("get 50", |b| b.iter(|| bench_gets(black_box(50))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
