use std::io::Read;

fn main() {
    let fname = std::env::args().nth(1).expect("must provide filename");
    let mut handle = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(fname)
        .unwrap();
    let mut chunk = [0; 1024 * 128];
    let mut count = 0;
    while handle.read_exact(&mut chunk).is_ok() {
        count += 1;
        if chunk == [0; 1024 * 128] {
            continue;
        }
        eprintln!();
        eprint!("{}\t", count * 128);
        for chunk in chunk.chunks(1024) {
            if chunk == [0; 1024] {
                eprint!(" ");
            } else {
                eprint!("-")
            }
        }
    }
}
