use crate::storage::PAGE_SIZE;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

pub const INVALID_PAGE_ID: u64 = u64::MAX;

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

pub const FRAME_FREE: u32 = 0;
pub const FRAME_LOADING: u32 = 1;
pub const FRAME_READY: u32 = 2;
pub const FRAME_EVICTING: u32 = 3;

const STATE_SHIFT: u32 = PIN_BITS;
const STATE_MASK: u32 = 0b11 << STATE_SHIFT;

pub const FLAG_DIRTY: u8 = 1 << 0;
pub const FLAG_WRITEBACK: u8 = 1 << 1;

/* GCLOCK */
pub const MAX_USAGE: u8 = 3;

pub enum PinResult {
    Pinned,
    Busy,
    Stale,
}

#[repr(C, align(4096))]
// 对齐一页，防止Prefetcher很可能不跨页导致的不命中
pub struct PageData {
    pub data: [u8; PAGE_SIZE],
}

#[repr(C, align(64))]
pub struct FrameMeta {
    /*
     * page_id 单独一个 AtomicU64，frame 复用时才会改变它；
     * 普通 pin/unpin 不应该因为 page_id 被打包进状态字
     * 而产生无意义的 CAS 冲突。
     */
    pub page_id: AtomicU64,

    // 低 20 bit = pin_count，后 2 bit = frame state。
    pub state_and_pin: AtomicU32,

    pub usage_count: AtomicU8,
    pub flags: AtomicU8,
}

impl FrameMeta {
    pub fn empty() -> Self {
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
    pub fn try_pin(&self, expected_page_id: u64) -> Result<PinResult> {
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

    pub fn unpin(&self, expected_page_id: u64, is_dirty: bool) -> Result<()> {
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
    pub fn record_access(&self) {
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

#[inline]
pub fn pack_state(state: u32, pin_count: u32) -> u32 {
    debug_assert!(state <= 0b11);
    debug_assert!(pin_count <= PIN_MASK);

    (state << STATE_SHIFT) | pin_count
}

#[inline]
pub fn frame_state(state_and_pin: u32) -> u32 {
    (state_and_pin & STATE_MASK) >> STATE_SHIFT
}

#[inline]
pub fn pin_count(state_and_pin: u32) -> u32 {
    state_and_pin & PIN_MASK
}
