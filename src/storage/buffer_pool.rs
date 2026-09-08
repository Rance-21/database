use crate::{
    hash_map::ConcurrentHashMap,
    storage::{arena::PageArena, disk::DiskManager, page::PageData},
};

use std::{
    collections::HashSet,
    io::{Error, ErrorKind, Result},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
};

const INVALID_PAGE_ID: u64 = u64::MAX;

/*
 * 31                         22 21       20 19                 0
 * ┌────────────────────────────┬───────────┬────────────────────┐
 * │          unused            │ state     │     pin_count      │
 * └────────────────────────────┴───────────┴────────────────────┘
 * reader:
 *     READY, pin=0 -> READY, pin=1
 * evictor:
 *     READY, pin=0 -> EVICTING, pin=0
 * 这两个操作必须通过同一个 CAS 竞争，才能保证 frame 不会一边被 reader pin，一边又被淘汰
 */
const PIN_BITS: u32 = 20;
const PIN_MASK: u32 = (1 << PIN_BITS) - 1;

const STATE_SHIFT: u32 = PIN_BITS;
const STATE_MASK: u32 = 0b11 << STATE_SHIFT;

const FRAME_FREE: u32 = 0;
const FRAME_LOADING: u32 = 1;
const FRAME_READY: u32 = 2;
const FRAME_EVICTING: u32 = 3;

const FLAG_DIRTY: u8 = 1 << 0;
const FLAG_WRITEBACK: u8 = 1 << 1;

/* GCLOCK */
const MAX_USAGE: u8 = 3;

/*
 * 同一个 page 的并发 miss 必须合并，否则：
 * Thread A: miss page 100 -> frame 3
 * Thread B: miss page 100 -> frame 8
 * 最后同一个 page 会同时存在两个副本
 */
const PAGE_WAIT_SHARDS: usize = 64;

#[repr(C, align(64))]
struct FrameMeta {
    /*
     * page_id 单独一个 AtomicU64，frame 复用时才会改变它；
     * 普通 pin/unpin 不应该因为 page_id 被打包进状态字
     * 而产生无意义的 CAS 冲突。
     */
    page_id: AtomicU64,

    // 低 20 bit = pin_count，后 2 bit = frame state。
    state_and_pin: AtomicU32,

    usage_count: AtomicU8,
    flags: AtomicU8,
}

impl FrameMeta {
    fn empty() -> Self {
        Self {
            page_id: AtomicU64::new(INVALID_PAGE_ID),
            state_and_pin: AtomicU32::new(pack_state(FRAME_FREE, 0)),
            usage_count: AtomicU8::new(0),
            flags: AtomicU8::new(0),
        }
    }

    /*
     * 尝试 pin 一个 page。
     * page_table.get(page_id) 只能告诉我们：
     *     “某一个时刻 page_id 指向这个 frame。”
     * 在 HashMap lookup 与真正 pin 之间，
     * frame 仍然可能被 evict/reuse。
     * 所以最终是否拥有这个 frame 必须由这里的 CAS 决定。
     */
    fn try_pin(&self, expected_page_id: u64) -> Result<PinResult> {
        if self.page_id.load(Ordering::Acquire) != expected_page_id {
            return Ok(PinResult::Stale);
        }

        loop {
            let old = self.state_and_pin.load(Ordering::Acquire);

            match frame_state(old) {
                FRAME_READY => {}

                /*
                 * EVICTING：
                 * CLOCK 已经抢走这个 frame。
                 * LOADING：
                 * page 已经被 publish 到 page_table，
                 * 但状态转换还没有完全结束。
                 * 两种情况都应该等待状态变化，而不是忙等。
                 */
                FRAME_LOADING | FRAME_EVICTING => {
                    return Ok(PinResult::Busy);
                }

                FRAME_FREE => {
                    return Err(Error::new(ErrorKind::Other, "page table 指向 FREE frame"));
                }

                _ => unreachable!(),
            }

            let pins = pin_count(old);

            if pins == PIN_MASK {
                return Err(Error::new(ErrorKind::Other, "frame pin_count overflow"));
            }

            /*
             * state 位不变，只给低 20 bit 的 pin_count + 1。
             * 如果此时 evictor：READY,0 -> EVICTING,0
             * 抢先成功，那么这个 CAS 会失败。
             * 如果这里：READY,0 -> READY,1
             * 抢先成功，那么 evictor 的 CAS 会失败。
             */
            if self
                .state_and_pin
                .compare_exchange_weak(old, old + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            /*
             * page_id 与 state 分成了两个 atomic，所以成功 pin 后必须再确认一次 page_id。
             * 极端情况下：
             * 1. 第一次看到 page_id = 100
             * 2. frame 被完整 evict + reuse 成 page 200
             * 3. 我们正好 pin 到新的 READY frame
             * 如果发生这种情况，把刚才误加的 pin 撤掉并重试。
             */
            if self.page_id.load(Ordering::Acquire) != expected_page_id {
                self.state_and_pin.fetch_sub(1, Ordering::Release);
                return Ok(PinResult::Stale);
            }

            self.record_access();

            return Ok(PinResult::Pinned);
        }
    }

    fn unpin(&self, expected_page_id: u64, is_dirty: bool) -> Result<()> {
        if self.page_id.load(Ordering::Acquire) != expected_page_id {
            return Err(Error::new(
                ErrorKind::NotFound,
                "unpin 时 page 已经不在这个 frame 中",
            ));
        }

        loop {
            let old = self.state_and_pin.load(Ordering::Acquire);

            if frame_state(old) != FRAME_READY {
                return Err(Error::new(ErrorKind::Other, "只能 unpin READY frame"));
            }

            if pin_count(old) == 0 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "frame pin_count 已经为 0",
                ));
            }

            /*
             * dirty 一定要在 pin_count-- 之前发布。
             * 假设现在 pin_count = 1：
             * 如果先减成 0：
             *     Thread A: pin = 0
             *     Thread B: CLOCK 抢走 frame，看到 clean
             *     Thread A: dirty = true
             * B 就可能直接覆盖一个实际上修改过的页面。
             * 所以顺序必须是：
             *     dirty = true
             *     ↓
             *     pin_count--
             */
            if is_dirty {
                self.flags.fetch_or(FLAG_DIRTY, Ordering::Release);
            }

            if self
                .state_and_pin
                .compare_exchange_weak(old, old - 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /*
     * GCLOCK 的访问记录。
     * usage_count 不承担 correctness，
     * 所以允许不同线程之间发生 CAS retry，也不需要 Acquire/Release。
     */
    fn record_access(&self) {
        let _ = self
            .usage_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |usage| {
                if usage < MAX_USAGE {
                    Some(usage + 1)
                } else {
                    None
                }
            });
    }
}

enum PinResult {
    Pinned,
    Busy,
    Stale,
}

/*
 * GCLOCK replacer 自己几乎没有数据。
 * 所有 replacement 信息都在每个 FrameMeta 里：
 *     pin_count
 *     state
 *     usage_count
 * 所以 cache hit 完全不需要访问这个结构。
 */
struct ClockReplacer {
    next_victim: AtomicUsize,
}

impl ClockReplacer {
    fn new() -> Self {
        Self {
            next_victim: AtomicUsize::new(0),
        }
    }

    fn next(&self, frame_count: usize) -> usize {
        self.next_victim.fetch_add(1, Ordering::Relaxed) % frame_count
    }
}

/*
 * 用于合并同一个 page 的并发 miss，
 * 同时也用于等待 EVICTING / LOADING frame 状态变化。
 * HashSet 只存在于 miss 慢路径，不影响 cache hit。
 */
struct PageWaitShard {
    loading_pages: Mutex<HashSet<u64>>,
    changed: Condvar,
}

impl PageWaitShard {
    fn new() -> Self {
        Self {
            loading_pages: Mutex::new(HashSet::new()),
            changed: Condvar::new(),
        }
    }
}

pub struct BufferPool {
    disk: DiskManager,

    // 整块 mmap 得到的 4KB page 区域。
    arena: PageArena,

    /*
     * 每个 FrameMeta 独占一条 64B cache line。
     * page A 与 page B 的 pin/usage 修改不会发生 false sharing。
     */
    metadata: Box<[FrameMeta]>,

    /*
     * free list 只在 page miss 时访问。
     * 锁内只做 Vec::pop / push，
     * 绝不会拿着这个 Mutex 做磁盘 IO。
     */
    free_frames: Mutex<Vec<usize>>,

    /*
     * page_id -> frame_id
     * get 是我们自己实现的 lockless read path。
     */
    page_table: ConcurrentHashMap,

    // GCLOCK 全局只剩一个 atomic clock hand。
    replacer: ClockReplacer,

    /*
     * 同页 miss / frame 状态等待。
     * 分片后不存在“所有 page miss 抢一个 Mutex”的问题。
     */
    page_waiters: Box<[PageWaitShard]>,
}

/*
 * PageArena 当前内部保存 NonNull<u8>，因此 Rust 不会自动认为
 * BufferPool 可以 Send / Sync。
 * 这里暂时明确告诉 Rust：
 * 1. mmap 地址在 PageArena 生命周期内不会移动；
 * 2. 哪个 frame 可以被 DiskManager 改写，由 FrameMeta 生命周期状态控制；
 * 3. BufferPool 自己不会在一个仍被 pin 的 frame 上做 eviction IO。
 * 注意：
 * fetch_page 返回的仍然是裸指针。
 * PageData 内容本身的 reader/writer 同步以后会由 PageGuard + RwLock 负责。
 * 下一步改 arena.rs 时，可以把这两个 impl 移到 PageArena 上。
 */
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    pub fn new(disk: DiskManager, frame_count: usize) -> Result<Self> {
        if frame_count == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "BufferPool 至少需要一个 frame",
            ));
        }

        let arena = PageArena::new(frame_count)?;

        let metadata = (0..frame_count)
            .map(|_| FrameMeta::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        /*
         * 反过来存，这样 pop() 时依次得到：
         * 0, 1, 2, 3...
         * 对应 mmap 区域从低地址向高地址使用。
         */
        let free_frames = (0..frame_count).rev().collect();

        let page_waiters = (0..PAGE_WAIT_SHARDS)
            .map(|_| PageWaitShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        debug_assert_eq!(std::mem::size_of::<FrameMeta>(), 64);

        Ok(Self {
            disk,
            arena,
            metadata,
            free_frames: Mutex::new(free_frames),
            page_table: ConcurrentHashMap::new(frame_count),
            replacer: ClockReplacer::new(),
            page_waiters,
        })
    }

    /*
     * fetch 的快路径：
     * page_table.get
     *      ↓
     * per-frame CAS pin
     *      ↓
     * usage_count++
     *      ↓
     * return
     * 不经过：
     *     free list Mutex
     *     page wait Mutex
     *     replacer lock
     *     disk IO lock
     */
    pub fn fetch_page(&self, page_id: u64) -> Result<*mut PageData> {
        loop {
            if let Some(frame_id) = self.page_table.get(page_id) {
                if frame_id >= self.metadata.len() {
                    return Err(Error::new(ErrorKind::Other, "page table 包含非法 frame_id"));
                }

                match self.metadata[frame_id].try_pin(page_id)? {
                    PinResult::Pinned => {
                        return Ok(self.arena.frame_ptr(frame_id));
                    }

                    /*
                     * frame 正在 loading / evicting。
                     * 磁盘 IO 可能很慢，所以绝不能 spin。
                     * 用 Condvar 睡眠等待状态变化。
                     */
                    PinResult::Busy => {
                        self.wait_for_frame_change(page_id, frame_id)?;
                        continue;
                    }

                    /*
                     * HashMap lookup 后 frame 已经发生复用。
                     * 重新从 page_table 开始即可。
                     */
                    PinResult::Stale => {
                        continue;
                    }
                }
            }

            /*
             * page 不在 BufferPool 中。
             * claim_page_load() 保证同一个 page 只有一个线程
             * 真正负责磁盘 IO。
             */
            if self.claim_page_load(page_id) {
                return self.load_page(page_id);
            }

            /*
             * 没拿到 owner 身份：
             * 要么另一个线程刚加载完；要么等待期间 page 已经出现。
             * 回顶部重新走快路径。
             */
        }
    }

    pub fn unpin_page(&self, page_id: u64, is_dirty: bool) -> Result<()> {
        let frame_id = self
            .page_table
            .get(page_id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "unpin 的 page 不在 BufferPool 中"))?;

        if frame_id >= self.metadata.len() {
            return Err(Error::new(ErrorKind::Other, "page table 包含非法 frame_id"));
        }

        self.metadata[frame_id].unpin(page_id, is_dirty)
    }

    /*
     * 成为某个 page 的 loader。
     * 返回 true：当前线程负责把 page 从磁盘加载进来。
     * 返回 false：page 已经在 BufferPool 中，重新走 fetch 快路径。
     */
    fn claim_page_load(&self, page_id: u64) -> bool {
        let shard = self.page_wait_shard(page_id);
        let mut loading_pages = shard.loading_pages.lock().unwrap();

        loop {
            /*
             * 必须在 shard 锁内 double check。
             * 在最开始 page_table.get miss 后到这里之间，另一个线程可能已经完成加载。
             */
            if self.page_table.get(page_id).is_some() {
                return false;
            }

            if loading_pages.insert(page_id) {
                /*
                 * 这里只登记：
                 *     “这个 page 已经有人负责加载。”
                 * Mutex 马上释放。后面的 pread/pwrite 完全不持有它。
                 */
                return true;
            }

            /*
             * 同一个 page 已经有 loader。
             * 磁盘 IO 是长等待，所以睡眠而不是 spin_loop。
             */
            loading_pages = shard.changed.wait(loading_pages).unwrap();
        }
    }

    fn load_page(&self, page_id: u64) -> Result<*mut PageData> {
        let frame_id = match self.reserve_frame() {
            Ok(frame_id) => frame_id,

            Err(error) => {
                self.finish_page_load_failure(page_id);
                return Err(error);
            }
        };

        let meta = &self.metadata[frame_id];

        /*
         * frame 现在是：
         *     LOADING   pin_count = 1
         * 但还没有 publish 到 page_table，
         * 所以其他线程不可能访问它。
         */
        meta.page_id.store(page_id, Ordering::Relaxed);
        meta.usage_count.store(0, Ordering::Relaxed);
        meta.flags.store(0, Ordering::Relaxed);

        let ptr = self.arena.frame_ptr(frame_id);

        /*
         * 真正的磁盘读取。
         * 此时：
         * - 没有 free-list 锁
         * - 没有 page-load shard 锁
         * - 没有 replacer 锁
         * 所以：
         * read page 100
         * read page 200
         * write page 300
         * 可以在不同线程中并行进行。
         */
        if let Err(error) = unsafe { self.disk.read_page(page_id, ptr) } {
            self.release_loading_frame(frame_id);
            self.finish_page_load_failure(page_id);

            return Err(error);
        }

        if let Err(error) = self.publish_loaded_page(page_id, frame_id) {
            self.release_loading_frame(frame_id);
            return Err(error);
        }

        Ok(ptr)
    }

    /*
     * 找一个 frame 给新的 page
     * 优先： free list
     * free list 为空才 GCLOCK eviction
     */
    fn reserve_frame(&self) -> Result<usize> {
        if let Some(frame_id) = self.free_frames.lock().unwrap().pop() {
            let meta = &self.metadata[frame_id];

            /*
             * free list 中的 frame 理论上一定是 FREE。
             * CAS 是为了让这个约束被代码真正检查，而不是只靠相信。
             */
            meta.state_and_pin
                .compare_exchange(
                    pack_state(FRAME_FREE, 0),
                    pack_state(FRAME_LOADING, 1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map_err(|_| Error::new(ErrorKind::Other, "free list 中出现非 FREE frame"))?;

            return Ok(frame_id);
        }

        let victim = self.find_victim()?;

        /*
         * find_victim() 成功后 victim 已经是：
         *     EVICTING, pin=0
         * 所以再也不会有新的 reader 成功 pin 它。
         */
        self.prepare_victim(victim)?;

        Ok(victim)
    }

    /*
     * GCLOCK：
     * 1. 非 READY      -> 跳过
     * 2. pin != 0      -> 跳过
     * 3. usage > 0     -> usage--，给一次机会
     * 4. usage == 0    -> CAS 抢占
     */
    fn find_victim(&self) -> Result<usize> {
        let frame_count = self.metadata.len();

        /*
         * usage 最大只有 MAX_USAGE。
         * 理论上没有新访问时，MAX_USAGE + 1 圈已经足够把 usage 降到 0。
         * 再多给一圈余量，应对并发访问。
         */
        let max_scan = frame_count.saturating_mul(MAX_USAGE as usize + 2);

        for _ in 0..max_scan {
            let frame_id = self.replacer.next(frame_count);
            let meta = &self.metadata[frame_id];

            let old = meta.state_and_pin.load(Ordering::Acquire);

            if frame_state(old) != FRAME_READY {
                continue;
            }

            if pin_count(old) != 0 {
                continue;
            }

            let usage = meta.usage_count.load(Ordering::Relaxed);

            if usage != 0 {
                /*
                 * usage 只是 hint。
                 * CAS 失败通常意味着：
                 * - reader 刚增加了 usage
                 * - 另一个 CLOCK scanner 刚减少了 usage
                 * 无论哪种情况，这一轮都放过它即可。
                 */
                let _ = meta.usage_count.compare_exchange_weak(
                    usage,
                    usage - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );

                continue;
            }

            /*
             * 最关键的 CAS：
             * evictor:
             *     READY,0 -> EVICTING,0
             * reader:
             *     READY,0 -> READY,1
             * 只有一个能成功。
             */
            if meta
                .state_and_pin
                .compare_exchange(
                    old,
                    pack_state(FRAME_EVICTING, 0),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(frame_id);
            }
        }

        Err(Error::new(ErrorKind::WouldBlock, "当前没有可淘汰的 frame"))
    }

    /*
     * 把一个 EVICTING victim 清理干净，最后转换成 LOADING frame 交给新的 page。
     */
    fn prepare_victim(&self, frame_id: usize) -> Result<()> {
        let meta = &self.metadata[frame_id];

        let old_page_id = meta.page_id.load(Ordering::Acquire);

        if old_page_id == INVALID_PAGE_ID {
            return Err(Error::new(ErrorKind::Other, "EVICTING frame 没有 page_id"));
        }

        let flags = meta.flags.load(Ordering::Acquire);

        if flags & FLAG_DIRTY != 0 {
            /*
             * WRITEBACK 是 per-frame flag。
             * 它不是“整个 DiskManager 正在写”
             * 所以 frame A 在 pwrite 时，
             * frame B/C/D 仍然可以 pread / pwrite。
             */
            meta.flags.fetch_or(FLAG_WRITEBACK, Ordering::AcqRel);

            let ptr = self.arena.frame_ptr(frame_id);

            if let Err(error) = unsafe { self.disk.write_page(old_page_id, ptr) } {
                /*
                 * 写失败：
                 * 旧 page 的 page_table mapping 一直没有删除，所以只要恢复 READY 就可以继续使用。
                 * DIRTY 不能清除，因为磁盘并没有成功更新。
                 */
                let shard = self.page_wait_shard(old_page_id);
                let _guard = shard.loading_pages.lock().unwrap();

                meta.flags.fetch_and(!FLAG_WRITEBACK, Ordering::Release);

                meta.state_and_pin
                    .store(pack_state(FRAME_READY, 0), Ordering::Release);

                shard.changed.notify_all();

                return Err(error);
            }
        }

        /*
         * 到这里：
         * - clean page 本来就和磁盘一致
         * - dirty page 已经成功写回
         * 因此现在才真正删除旧 page 的 mapping。
         * 这样 writeback 失败时不需要复杂地重新插入旧 page。
         */
        let shard = self.page_wait_shard(old_page_id);
        let _guard = shard.loading_pages.lock().unwrap();

        let removed = self.page_table.remove(old_page_id);

        if removed != Some(frame_id) {
            /*
             * 这是内部 invariant 被破坏，而不是普通运行时情况。
             * 为了避免 frame 永久卡在 EVICTING，至少把它恢复成一个一致的状态。
             */
            match removed {
                None => {
                    let _ = self.page_table.insert(old_page_id, frame_id);

                    meta.state_and_pin
                        .store(pack_state(FRAME_READY, 0), Ordering::Release);
                }

                Some(other_frame_id) => {
                    /*
                     * page_table 原本竟然指向另一个 frame。
                     * 把刚删除的 mapping 恢复，当前 victim 则退回 FREE。
                     */
                    let _ = self.page_table.insert(old_page_id, other_frame_id);

                    meta.page_id.store(INVALID_PAGE_ID, Ordering::Relaxed);

                    meta.usage_count.store(0, Ordering::Relaxed);
                    meta.flags.store(0, Ordering::Relaxed);

                    meta.state_and_pin
                        .store(pack_state(FRAME_FREE, 0), Ordering::Release);

                    self.free_frames.lock().unwrap().push(frame_id);
                }
            }

            shard.changed.notify_all();

            return Err(Error::new(
                ErrorKind::Other,
                "page table 与 FrameMeta 状态不一致",
            ));
        }

        /*
         * 旧 page 已经彻底与这个 frame 脱离。
         * 这个 frame 现在被当前 loader 独占，
         * 所以转成：
         *     LOADING, pin=1
         */
        meta.page_id.store(INVALID_PAGE_ID, Ordering::Relaxed);

        meta.usage_count.store(0, Ordering::Relaxed);
        meta.flags.store(0, Ordering::Relaxed);

        meta.state_and_pin
            .store(pack_state(FRAME_LOADING, 1), Ordering::Release);

        /*
         * 可能有人之前：
         *     fetch(old_page)
         *       ↓
         *     看见 EVICTING
         *       ↓
         *     睡眠
         * 现在旧 mapping 已经消失，可以叫醒他们重新 fetch。
         */
        shard.changed.notify_all();

        Ok(())
    }

    /*
     * 磁盘读取已经完成，正式把 page 发布给其他线程。
     */
    fn publish_loaded_page(&self, page_id: u64, frame_id: usize) -> Result<()> {
        let shard = self.page_wait_shard(page_id);
        let mut loading_pages = shard.loading_pages.lock().unwrap();

        if !loading_pages.contains(&page_id) {
            return Err(Error::new(ErrorKind::Other, "page load owner 状态丢失"));
        }

        /*
         * 在同一个 shard Mutex 下重新检查。
         * 理论上不会出现，因为同一个 page 只能有一个 loader。
         */
        if self.page_table.get(page_id).is_some() {
            loading_pages.remove(&page_id);
            shard.changed.notify_all();

            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "page 在加载过程中被重复 publish",
            ));
        }

        /*
         * 先把 mapping 放进去，再把 frame 切成 READY。
         * 这中间另一个线程可能短暂看到：
         *     page_table -> LOADING frame
         * 但 try_pin 会返回 Busy，随后它会在这个 shard Condvar 上睡眠。
         * 因为我们当前正持有同一个 shard Mutex，所以不会发生 lost wakeup。
         */
        match self.page_table.insert(page_id, frame_id) {
            Ok(None) => {}

            Ok(Some(old_frame_id)) => {
                /*
                 * insert 会替换旧值。
                 * 正常情况下这里绝不应该存在旧值，所以立刻恢复，避免破坏 page table。
                 */
                let _ = self.page_table.insert(page_id, old_frame_id);

                loading_pages.remove(&page_id);
                shard.changed.notify_all();

                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "page table 出现重复 page",
                ));
            }

            Err(()) => {
                loading_pages.remove(&page_id);
                shard.changed.notify_all();

                return Err(Error::new(ErrorKind::Other, "page table 已满"));
            }
        }

        let meta = &self.metadata[frame_id];

        /*
         * 新加载的 page 已经有一次真实访问，所以 GCLOCK usage 从 1 开始。
         */
        meta.usage_count.store(1, Ordering::Relaxed);

        /*
         * Release：在 READY 被其他线程看到之前，
         * 前面的 4KB page read 与 page_id 初始化必须已经完成。
         */
        meta.state_and_pin
            .store(pack_state(FRAME_READY, 1), Ordering::Release);

        loading_pages.remove(&page_id);

        shard.changed.notify_all();

        Ok(())
    }

    /*
     * loader 失败：
     * 清除“page 正在被加载”的登记，让另一个线程以后可以重新尝试。
     */
    fn finish_page_load_failure(&self, page_id: u64) {
        let shard = self.page_wait_shard(page_id);
        let mut loading_pages = shard.loading_pages.lock().unwrap();

        loading_pages.remove(&page_id);

        shard.changed.notify_all();
    }

    /*
     * read_page 失败，或者 publish 失败时，把 loader 独占的 LOADING frame 放回 free list。
     */
    fn release_loading_frame(&self, frame_id: usize) {
        let meta = &self.metadata[frame_id];

        meta.page_id.store(INVALID_PAGE_ID, Ordering::Relaxed);

        meta.usage_count.store(0, Ordering::Relaxed);
        meta.flags.store(0, Ordering::Relaxed);

        meta.state_and_pin
            .store(pack_state(FRAME_FREE, 0), Ordering::Release);

        self.free_frames.lock().unwrap().push(frame_id);
    }

    /*
     * page_table 已经能找到 frame，
     * 但 frame 当前正在 LOADING / EVICTING。
     * 磁盘 IO 可能持续很久，因此用 Condvar，不在 CPU 上 busy-spin。
     */
    fn wait_for_frame_change(&self, page_id: u64, frame_id: usize) -> Result<()> {
        let shard = self.page_wait_shard(page_id);
        let mut loading_pages = shard.loading_pages.lock().unwrap();

        loop {
            /*
             * mapping 已经消失或指向别的 frame：
             * 状态已经变化，回 fetch_page 重试。
             */
            if self.page_table.get(page_id) != Some(frame_id) {
                return Ok(());
            }

            let state = frame_state(
                self.metadata[frame_id]
                    .state_and_pin
                    .load(Ordering::Acquire),
            );

            match state {
                FRAME_READY => {
                    return Ok(());
                }

                FRAME_LOADING | FRAME_EVICTING => {
                    loading_pages = shard.changed.wait(loading_pages).unwrap();
                }

                FRAME_FREE => {
                    return Err(Error::new(ErrorKind::Other, "page table 指向 FREE frame"));
                }

                _ => unreachable!(),
            }
        }
    }

    fn page_wait_shard(&self, page_id: u64) -> &PageWaitShard {
        /*
         * page_id 通常连续增长。
         * 先做一次简单的乘法 hash，避免某些特殊 page_id 模式集中到同一个 shard。
         */
        let hash = page_id.wrapping_mul(0x9e37_79b9_7f4a_7c15);

        &self.page_waiters[hash as usize & (PAGE_WAIT_SHARDS - 1)]
    }
}

#[inline]
fn pack_state(state: u32, pin_count: u32) -> u32 {
    debug_assert!(state <= 0b11);
    debug_assert!(pin_count <= PIN_MASK);

    (state << STATE_SHIFT) | pin_count
}

#[inline]
fn frame_state(state_and_pin: u32) -> u32 {
    (state_and_pin & STATE_MASK) >> STATE_SHIFT
}

#[inline]
fn pin_count(state_and_pin: u32) -> u32 {
    state_and_pin & PIN_MASK
}
