use crate::storage::{
    PAGE_SIZE,
    buffer_pool::BufferPool,
    page::{INVALID_PAGE_ID, PageData},
};
use std::io::{Error, ErrorKind, Result};
use std::mem::{MaybeUninit, size_of};
use std::ptr;

const NODE_LEAF: u8 = 1;
const NODE_INTERNAL: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LeafEntry {
    pub key: u64,
    pub value: u64,
}

// Page 开头数据
#[repr(C)]
pub struct LeafHeader {
    pub node_type: u8, // 叶子还是内部节点
    pub _padding: [u8; 3],
    pub len: u32, // 当前叶子有多少个entry
    pub next_page_id: u64,
}

const LEAF_CAPACITY: usize = (PAGE_SIZE - size_of::<LeafHeader>()) / size_of::<LeafEntry>();

pub struct LeafPage {
    pub header: LeafHeader,

    //只有 entries[0..header.len] 是有效数据
    entries: [MaybeUninit<LeafEntry>; LEAF_CAPACITY],
}

impl LeafPage {
    // 把一张普通 Page 初始化成 B+ 树叶子节点
    pub unsafe fn init(page: *mut PageData) -> *mut Self {
        let leaf = page.cast::<Self>();

        unsafe {
            ptr::addr_of_mut!((*leaf).header).write(LeafHeader {
                node_type: NODE_LEAF,
                _padding: [0; 3],
                len: 0,
                next_page_id: INVALID_PAGE_ID,
            });
        }

        leaf
    }

    pub fn len(&self) -> usize {
        self.header.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.header.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == LEAF_CAPACITY
    }

    // 取得当前已经初始化的 Entry
    fn entries(&self) -> &[LeafEntry] {
        let len = self.len();
        assert!(len <= LEAF_CAPACITY);

        unsafe { std::slice::from_raw_parts(self.entries.as_ptr().cast::<LeafEntry>(), len) }
    }

    // 找到第一个 key >= target 的位置。
    pub fn lower_bound(&self, target: u64) -> usize {
        self.entries().partition_point(|entry| entry.key < target)
    }

    // 在当前 Leaf Page 中查找一个 key。
    pub fn get(&self, key: u64) -> Option<u64> {
        let index = self.lower_bound(key);
        let entries = self.entries();

        if index < entries.len() && entries[index].key == key {
            Some(entries[index].value)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: u64, value: u64) -> bool {
        if self.is_full() {
            return false;
        }

        let len = self.len();

        // 插到相同 key 的第一个位置。
        let index = self.lower_bound(key);

        unsafe {
            let base = self.entries.as_mut_ptr();

            // [index..len) 整体向右移动一格。
            // ptr::copy 支持源和目标区域重叠。
            ptr::copy(base.add(index), base.add(index + 1), len - index);

            base.add(index)
                .write(MaybeUninit::new(LeafEntry { key, value }));
        }

        self.header.len += 1;
        true
    }

    /// 当前 Leaf 已满时，把一半数据移动到 right，
    /// 再插入新的 key/value。
    /// 返回父节点需要保存的 separator key。
    fn split_insert(
        &mut self,
        right: &mut LeafPage,
        right_page_id: u64,
        key: u64,
        value: u64,
    ) -> u64 {
        debug_assert!(self.is_full());
        debug_assert!(right.is_empty());

        // 255 条原数据：
        // left 保留前 128 条，right 拿后 127 条。
        const SPLIT_INDEX: usize = (LEAF_CAPACITY + 1) / 2;

        let right_len = LEAF_CAPACITY - SPLIT_INDEX;

        unsafe {
            ptr::copy_nonoverlapping(
                self.entries.as_ptr().add(SPLIT_INDEX),
                right.entries.as_mut_ptr(),
                right_len,
            );
        }

        self.header.len = SPLIT_INDEX as u32;
        right.header.len = right_len as u32;

        // 把 right 插入叶子链表。
        right.header.next_page_id = self.header.next_page_id;
        self.header.next_page_id = right_page_id;

        // right 的第一个 key 就是两个节点之间的分界。
        let separator = right.entries()[0].key;

        if key < separator {
            self.insert(key, value);
        } else {
            right.insert(key, value);
        }

        // 如果新 key 插进 right，它不可能比 separator 小，
        // 所以 right 的第一个 key 不会发生变化。
        right.entries()[0].key
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct InternalEntry {
    pub key: u64,
    pub child_page_id: u64,
}

/// 每个 Internal Page 开头的元数据。
#[repr(C)]
pub struct InternalHeader {
    pub node_type: u8,

    // 对齐 len。
    pub _padding: [u8; 3],

    /// 当前有效的 separator key 数量。
    pub len: u32,

    /// 因为 n 个 separator key 会对应 n + 1 个 child，
    /// 所以必须额外保存第一个 child。
    pub first_child_page_id: u64,
}

const INTERNAL_CAPACITY: usize =
    (PAGE_SIZE - size_of::<InternalHeader>()) / size_of::<InternalEntry>();

// 一个完整的 B+ 树内部节点，占一个 4KB Page。
#[repr(C)]
pub struct InternalPage {
    pub header: InternalHeader,

    // 只有 entries[0..len] 有效。
    entries: [MaybeUninit<InternalEntry>; INTERNAL_CAPACITY],
}

impl InternalPage {
    // 创建 Internal 节点时至少已经有一个 child，
    // 所以需要传入 first_child_page_id。
    pub unsafe fn init(page: *mut PageData, first_child_page_id: u64) -> *mut Self {
        let internal = page.cast::<Self>();

        unsafe {
            ptr::addr_of_mut!((*internal).header).write(InternalHeader {
                node_type: NODE_INTERNAL,
                _padding: [0; 3],
                len: 0,
                first_child_page_id,
            });
        }

        internal
    }

    pub fn len(&self) -> usize {
        self.header.len as usize
    }

    pub fn is_full(&self) -> bool {
        self.len() == INTERNAL_CAPACITY
    }

    fn entries(&self) -> &[InternalEntry] {
        let len = self.len();
        assert!(len <= INTERNAL_CAPACITY);

        unsafe { std::slice::from_raw_parts(self.entries.as_ptr().cast::<InternalEntry>(), len) }
    }

    // 根据 key 找到下一层应该访问的 child PageId。
    pub fn child_for(&self, key: u64) -> u64 {
        let entries = self.entries();

        // 找到第一个 separator > key 的位置。
        let index = entries.partition_point(|entry| entry.key <= key);

        if index == 0 {
            self.header.first_child_page_id
        } else {
            entries[index - 1].child_page_id
        }
    }

    // 插入一个 separator 和它右侧的 child。
    fn insert(&mut self, key: u64, child_page_id: u64) -> bool {
        if self.is_full() {
            return false;
        }

        let len = self.len();

        let index = self.entries().partition_point(|entry| entry.key < key);

        unsafe {
            let entries = self.entries.as_mut_ptr();

            ptr::copy(entries.add(index), entries.add(index + 1), len - index);

            entries
                .add(index)
                .write(MaybeUninit::new(InternalEntry { key, child_page_id }));
        }

        self.header.len += 1;

        true
    }
}

pub struct BPlusTree {
    // 整棵树根节点所在的 Page。
    root_page_id: u64,
}

impl BPlusTree {
    pub fn new(root_page_id: u64) -> Self {
        Self { root_page_id }
    }

    // 从 Root 一直向下查到 Leaf。
    pub fn get(&self, buffer_pool: &BufferPool, key: u64) -> Result<Option<u64>> {
        let mut page_id = self.root_page_id;

        loop {
            // fetch_page 会 pin 当前 Page。
            let page = buffer_pool.fetch_page(page_id)?;

            let node_type = unsafe { (*page).data[0] };

            match node_type {
                NODE_LEAF => {
                    let leaf = unsafe { &*page.cast::<LeafPage>() };

                    let result = leaf.get(key);

                    buffer_pool.unpin_page(page_id, false)?;

                    return Ok(result);
                }

                NODE_INTERNAL => {
                    let internal = unsafe { &*page.cast::<InternalPage>() };

                    // 必须在 unpin 前先取出 child PageId，
                    // 因为 unpin 后这个 frame 理论上可以被淘汰。
                    let child_page_id = internal.child_for(key);

                    buffer_pool.unpin_page(page_id, false)?;

                    page_id = child_page_id;
                }

                _ => {
                    buffer_pool.unpin_page(page_id, false)?;

                    return Err(Error::new(ErrorKind::InvalidData, "B+ Tree page type 无效"));
                }
            }
        }
    }

    pub fn insert(&mut self, buffer_pool: &mut BufferPool, key: u64, value: u64) -> Result<()> {
        let old_root_page_id = self.root_page_id;
        let old_root_page = buffer_pool.fetch_page(old_root_page_id)?;

        let node_type = unsafe { (*old_root_page).data[0] };

        // 这一版暂时只处理 Root 本身就是 Leaf 的情况。
        if node_type != NODE_LEAF {
            buffer_pool.unpin_page(old_root_page_id, false)?;

            return Err(Error::new(
                ErrorKind::Unsupported,
                "multi-level B+ Tree insert is not implemented yet",
            ));
        }

        let leaf = unsafe { &mut *old_root_page.cast::<LeafPage>() };

        // 第一版使用唯一 key。
        if leaf.get(key).is_some() {
            buffer_pool.unpin_page(old_root_page_id, false)?;

            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "duplicate B+ Tree key",
            ));
        }

        // 绝大多数插入都走这里：当前 Leaf 还有空间。
        if !leaf.is_full() {
            leaf.insert(key, value);
            buffer_pool.unpin_page(old_root_page_id, true)?;

            return Ok(());
        }

        // Root Leaf 满了。
        // 先把需要的新 Page 全部分配好，再修改旧树结构。
        let right_page_id = buffer_pool.allocate_page_id()?;
        let new_root_page_id = buffer_pool.allocate_page_id()?;

        let right_page = buffer_pool.fetch_page(right_page_id)?;
        let new_root_page = buffer_pool.fetch_page(new_root_page_id)?;

        let right = unsafe { &mut *LeafPage::init(right_page) };

        // old root 原地变成左 Leaf。
        let separator = leaf.split_insert(right, right_page_id, key, value);

        // 创建新的 Internal Root。
        let new_root = unsafe { &mut *InternalPage::init(new_root_page, old_root_page_id) };

        new_root.insert(separator, right_page_id);

        // 最后才正式更换整棵树的 Root。
        self.root_page_id = new_root_page_id;

        buffer_pool.unpin_page(old_root_page_id, true)?;
        buffer_pool.unpin_page(right_page_id, true)?;
        buffer_pool.unpin_page(new_root_page_id, true)?;

        Ok(())
    }
}
