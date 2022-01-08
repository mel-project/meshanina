use std::io::Read;

fn main() {
    let fname = std::env::args().nth(1).expect("must provide filename");
    let mut handle = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(fname)
        .unwrap();
    let mut chunk = [0; 65536];
    let mut count = 0;
    while handle.read_exact(&mut chunk).is_ok() {
        if chunk == [0; 65536] {
            eprint!(" ");
        } else {
            eprint!("-")
        }
        if count % 100 == 0 {
            eprintln!();
        }
        count += 1;
    }
}
