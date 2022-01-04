use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meshanina::Mapping;
use once_cell::sync::Lazy;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

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

fn bench_gets(n: u64, parallel: bool) {
    if !parallel {
        for i in 0..n {
            let _ = black_box(DATABASE.get((i % 50).into()));
        }
    } else {
        (0..n).into_par_iter().for_each(|i| {
            let _ = black_box(DATABASE.get((i % 50).into()));
        })
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("get 50 seq", |b| {
        b.iter(|| bench_gets(black_box(50), false))
    });
    c.bench_function("get 50 par", |b| b.iter(|| bench_gets(black_box(50), true)));
    c.bench_function("get 5000 seq", |b| {
        b.iter(|| bench_gets(black_box(5000), false))
    });
    c.bench_function("get 5000 par", |b| {
        b.iter(|| bench_gets(black_box(5000), true))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
