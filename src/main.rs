mod linear_hash;
mod storage;
mod sync;

use std::io::Result;
use storage::{arena::PageArena, disk::DiskManager};

fn main() -> Result<()> {
    let mut disk = DiskManager::open("test.db")?;

    let page_id = disk.allocate_page()?;

    let arena = PageArena::new(16)?;
    let frame = arena.frame_ptr(0);

    unsafe {
        disk.read_page(page_id, frame)?;
        (*frame).data[0] = 42;

        println!("{}", (*frame).data[0]);
    }

    Ok(())
}
