use crate::storage::{
    arena::PageArena,
    disk::{self, DiskManager},
    page::{FrameMeta, PageData},
};
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::io::{Error, ErrorKind, Result};

pub struct BufferPool {
    disk: DiskManager,
    //mmap buffer_pool所需的内存区域，也能返回某一页的指针
    arena: PageArena,
    metadata: Box<[FrameMeta]>,
    free_frames: Vec<usize>,
    page_table: FxHashMap<u64, usize>,
}

impl BufferPool {
    pub fn new(disk: DiskManager, frame_count: usize) -> Result<Self> {
        let arena = PageArena::new(frame_count)?;

        //提前分配，禁止扩容
        let metadata = (0..frame_count)
            .map(|_| FrameMeta::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        //翻转顺序，确保物理内存地址由低到高依次弹出
        let free_frames = (0..frame_count).rev().collect();

        Ok(Self {
            disk,
            arena,
            metadata,
            free_frames,
            page_table: FxHashMap::with_capacity_and_hasher(frame_count, FxBuildHasher::default()),
        })
    }

    pub fn fetch_page(&mut self, page_id: u64) -> Result<*mut PageData> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.metadata[frame_id].pin_count += 1;
            return Ok(self.arena.frame_ptr(frame_id));
        }

        let frame_id = self
            .free_frames
            .pop()
            .ok_or_else(|| Error::new(ErrorKind::Other, "缓冲池已满"))?;

        let ptr = self.arena.frame_ptr(frame_id);

        unsafe {
            self.disk.read_page(page_id, ptr)?;
        }

        self.metadata[frame_id] = FrameMeta {
            page_id: Some(page_id),
            is_dirty: false,
            pin_count: 1,
        };

        self.page_table.insert(page_id, frame_id);

        Ok(ptr)
    }
}
