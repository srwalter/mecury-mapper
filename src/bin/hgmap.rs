use std::env::{self, Args};

use mercury_mapper::SuperPartition;

fn adopt(mut args: Args) {
    let device = args.next().expect("no device provided");
    let name = args.next().expect("no name provided");
    let size_bytes = args.next().expect("no size provided");
    let size_bytes: u64 = size_bytes.parse().expect("size not a number");

    let mut sp = SuperPartition::adopt(device, name, size_bytes).expect("adopt");
    sp.commit().expect("commit");
}

fn open(mut args: Args) {
    let device = args.next().expect("no device provided");

    SuperPartition::open(device).expect("open");
}

fn create(mut args: Args) {
    let device = args.next().expect("no device provided");
    let name = args.next().expect("no name provided");
    let size_bytes = args.next().expect("no size provided");
    let size_bytes: u64 = size_bytes.parse().expect("size not a number");

    let mut sp = SuperPartition::open(device).expect("open");
    sp.create_subvol(name, size_bytes).expect("create");
    sp.commit().expect("commit");
}

fn delete(mut args: Args) {
    let device = args.next().expect("no device provided");
    let name = args.next().expect("no name provided");

    let mut sp = SuperPartition::open(device).expect("open");
    if let Some(sv) = sp.subvols.get(&name) {
        sp.delete_subvol(sv.clone()).expect("failed to delete");
        sp.commit().expect("commit");
    } else {
        eprintln!("No such subvolume");
    }
}

pub fn main () {
    let mut args = env::args();
    let _argv0 = args.next().unwrap();
    let command = args.next().expect("no command provided");

    match command.as_ref() {
        "adopt" => adopt(args),
        "open" => open(args),
        "create" => create(args),
        "delete" => delete(args),
        _ => eprintln!("Unknown command: {}", command)
    }
}