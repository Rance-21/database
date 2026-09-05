use crate::storage::PAGE_SIZE;

#[repr(C, align(4096))]
// 对齐一页，防止Prefetcher很可能不跨页导致的不命中
pub struct PageData {
    pub data: [u8; PAGE_SIZE],
}

#[repr(C, align(64))]
// 对齐缓存行
pub struct FrameMeta {
    pub page_id: Option<u64>,
    // 脏页
    pub is_dirty: bool,
    // 使用计数
    pub pin_count: usize,
}

impl FrameMeta {
    pub fn empty() -> Self {
        Self {
            page_id: None,
            is_dirty: false,
            pin_count: 0,
        }
    }
}
