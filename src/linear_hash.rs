const EMPTY: u8 = 0;
const OCCUPIED: u8 = 1;
const TOMBSTONE: u8 = 2;

#[derive(Clone, Copy)]
struct Slot {
    state: u8,
    key: u64,
    value: usize,
}
