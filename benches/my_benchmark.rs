use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use meshanina::Mapping;
use once_cell::sync::Lazy;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

const COUNT: u64 = 1000000;

static DATABASE: Lazy<Mapping> = Lazy::new(|| {
    let fname = PathBuf::from("/tmp/bench.db");
    let mapping = Mapping::open(&fname).unwrap();
    for i in 0..COUNT {
        mapping.insert(
            i.into(),
            &std::iter::repeat_with(|| rand::thread_rng().gen::<u8>())
                .take(400)
                .collect::<Vec<_>>(),
        );
    }
    mapping
});

fn bench_gets(n: u64, parallel: bool) {
    if !parallel {
        for i in 0..n {
            let _ = black_box(DATABASE.get((i % COUNT).into()));
        }
    } else {
        (0..n).into_par_iter().for_each(|i| {
            let _ = black_box(DATABASE.get((i % COUNT).into()));
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
    std::env::set_var("MESHANINA_OWNED_CACHE", "1");
    c.bench_function("get 5000 par cached", |b| {
        b.iter(|| bench_gets(black_box(5000), true))
    });
    // c.bench_function("get 50000 seq", |b| {
    //     b.iter(|| bench_gets(black_box(50000), false))
    // });
    // c.bench_function("get 50000 par", |b| {
    //     b.iter(|| bench_gets(black_box(50000), true))
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
