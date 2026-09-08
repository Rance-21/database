use crate::sync::spin_lock::SpinLock;
use std::arch::x86_64::*;
use std::hint::spin_loop;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const GROUP_SIZE: usize = 16;

const EMPTY: u8 = 0x80; // 1000_0000
const DELETED: u8 = 0xfe; // 1111_1110

const EMPTY_WORD: u64 = u64::from_ne_bytes([EMPTY; 8]);

struct Group {
    // 偶数：稳定
    // 奇数：writer 正在修改
    version: AtomicU64,

    control_low: AtomicU64,
    control_high: AtomicU64,

    keys: [AtomicU64; GROUP_SIZE],
    values: [AtomicUsize; GROUP_SIZE],
}

struct InsertScan {
    matches: u16,
    empty: u16,
    deleted: u16,
}

pub struct ConcurrentHashMap {
    groups: Box<[Group]>,

    group_mask: usize,
    group_shift: u32,

    writer: SpinLock,
}

impl Group {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),

            control_low: AtomicU64::new(EMPTY_WORD),
            control_high: AtomicU64::new(EMPTY_WORD),

            keys: std::array::from_fn(|_| AtomicU64::new(0)),
            values: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn begin_write(&self) {
        // even -> odd
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    fn end_write(&self) {
        // odd -> even，同时发布刚才的修改
        self.version.fetch_add(1, Ordering::Release);
    }

    #[inline]
    fn control(&self) -> (u64, u64) {
        (
            self.control_low.load(Ordering::Relaxed),
            self.control_high.load(Ordering::Relaxed),
        )
    }
}

impl ConcurrentHashMap {
    pub fn new(max_entries: usize) -> Self {
        // 最大约 50% load factor
        let group_count = ((max_entries * 2 + GROUP_SIZE - 1) / GROUP_SIZE)
            .max(1)
            .next_power_of_two();

        let groups = (0..group_count)
            .map(|_| Group::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let group_bits = group_count.trailing_zeros();

        Self {
            groups,
            group_mask: group_count - 1,

            // group_count == 1 时单独处理
            group_shift: 64 - group_bits,

            writer: SpinLock::new(),
        }
    }

    #[inline]
    fn hash(key: u64) -> u64 {
        key.wrapping_mul(0x9e3779b97f4a7c15)
    }

    #[inline]
    fn fingerprint(hash: u64) -> u8 {
        // group index 用高位，fingerprint 用低 7 位
        (hash & 0x7f) as u8
    }

    #[inline]
    fn group_id(&self, hash: u64) -> usize {
        if self.group_mask == 0 {
            0
        } else {
            (hash >> self.group_shift) as usize & self.group_mask
        }
    }

    pub fn get(&self, key: u64) -> Option<usize> {
        let hash = Self::hash(key);
        let fingerprint = Self::fingerprint(hash);

        let mut group_id = self.group_id(hash);

        for _ in 0..self.groups.len() {
            let group = &self.groups[group_id];

            loop {
                let version_before = group.version.load(Ordering::Acquire);

                // writer 正在动这个 group
                if version_before & 1 != 0 {
                    spin_loop();
                    continue;
                }

                let (lo, hi) = group.control();

                let (mut matches, empty) = unsafe { scan_lookup_group(lo, hi, fingerprint) };

                let mut result = None;

                while matches != 0 {
                    let slot = matches.trailing_zeros() as usize;
                    matches &= matches - 1;

                    if group.keys[slot].load(Ordering::Relaxed) == key {
                        result = Some(group.values[slot].load(Ordering::Relaxed));
                        break;
                    }
                }

                fence(Ordering::Acquire);

                let version_after = group.version.load(Ordering::Relaxed);

                // snapshot 完整，没有 writer 插进来
                if version_before == version_after {
                    if result.is_some() {
                        return result;
                    }

                    if empty != 0 {
                        return None;
                    }

                    break;
                }

                // group 在读取过程中变过，重新读这一组
            }

            group_id = (group_id + 1) & self.group_mask;
        }

        None
    }

    pub fn insert(&self, key: u64, value: usize) -> Result<Option<usize>, ()> {
        let _guard = self.writer.lock();

        let hash = Self::hash(key);
        let fingerprint = Self::fingerprint(hash);

        let mut group_id = self.group_id(hash);
        let mut first_deleted = None;

        for _ in 0..self.groups.len() {
            let group = &self.groups[group_id];
            let (lo, hi) = group.control();

            let scan = unsafe { scan_insert_group(lo, hi, fingerprint) };

            let mut matches = scan.matches;

            while matches != 0 {
                let slot = matches.trailing_zeros() as usize;
                matches &= matches - 1;

                if group.keys[slot].load(Ordering::Relaxed) == key {
                    group.begin_write();

                    let old = group.values[slot].swap(value, Ordering::Relaxed);

                    group.end_write();

                    return Ok(Some(old));
                }
            }

            if first_deleted.is_none() && scan.deleted != 0 {
                first_deleted = Some((group_id, scan.deleted.trailing_zeros() as usize));
            }

            if scan.empty != 0 {
                let (target_group, target_slot) =
                    first_deleted.unwrap_or((group_id, scan.empty.trailing_zeros() as usize));

                self.publish_slot(target_group, target_slot, key, value, fingerprint);

                return Ok(None);
            }

            group_id = (group_id + 1) & self.group_mask;
        }

        if let Some((group_id, slot)) = first_deleted {
            self.publish_slot(group_id, slot, key, value, fingerprint);

            return Ok(None);
        }

        Err(())
    }

    #[inline]
    fn publish_slot(&self, group_id: usize, slot: usize, key: u64, value: usize, fingerprint: u8) {
        let group = &self.groups[group_id];

        group.begin_write();

        group.keys[slot].store(key, Ordering::Relaxed);
        group.values[slot].store(value, Ordering::Relaxed);

        Self::set_control(group, slot, fingerprint);

        group.end_write();
    }

    pub fn remove(&self, key: u64) -> Option<usize> {
        let _guard = self.writer.lock();

        let hash = Self::hash(key);
        let fingerprint = Self::fingerprint(hash);

        let mut group_id = self.group_id(hash);

        for _ in 0..self.groups.len() {
            let group = &self.groups[group_id];
            let (lo, hi) = group.control();

            let scan = unsafe { scan_insert_group(lo, hi, fingerprint) };

            let mut matches = scan.matches;

            while matches != 0 {
                let slot = matches.trailing_zeros() as usize;
                matches &= matches - 1;

                if group.keys[slot].load(Ordering::Relaxed) == key {
                    let value = group.values[slot].load(Ordering::Relaxed);

                    group.begin_write();

                    // key/value 不清除，只改变 control
                    Self::set_control(group, slot, DELETED);

                    group.end_write();

                    return Some(value);
                }
            }

            if scan.empty != 0 {
                return None;
            }

            group_id = (group_id + 1) & self.group_mask;
        }

        None
    }

    #[inline]
    fn set_control(group: &Group, slot: usize, control: u8) {
        let (word, offset) = if slot < 8 {
            (&group.control_low, slot)
        } else {
            (&group.control_high, slot - 8)
        };

        let shift = offset * 8;
        let clear_mask = !(0xffu64 << shift);

        let old = word.load(Ordering::Relaxed);

        let new = (old & clear_mask) | ((control as u64) << shift);

        // writer 已经全局互斥，所以不用 CAS
        word.store(new, Ordering::Relaxed);
    }
}

#[inline]
unsafe fn scan_lookup_group(lo: u64, hi: u64, fingerprint: u8) -> (u16, u16) {
    let controls = unsafe { _mm_set_epi64x(hi as i64, lo as i64) };

    let fingerprint_vec = unsafe { _mm_set1_epi8(fingerprint as i8) };

    let empty_vec = unsafe { _mm_set1_epi8(EMPTY as i8) };

    let matches = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(controls, fingerprint_vec)) as u16 };

    let empty = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(controls, empty_vec)) as u16 };

    (matches, empty)
}

#[inline]
unsafe fn scan_insert_group(lo: u64, hi: u64, fingerprint: u8) -> InsertScan {
    let controls = unsafe { _mm_set_epi64x(hi as i64, lo as i64) };

    let fingerprint_vec = unsafe { _mm_set1_epi8(fingerprint as i8) };

    let empty_vec = unsafe { _mm_set1_epi8(EMPTY as i8) };

    let matches = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(controls, fingerprint_vec)) as u16 };

    let empty = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(controls, empty_vec)) as u16 };

    // EMPTY / DELETED 最高位都是 1
    let special = unsafe { _mm_movemask_epi8(controls) as u16 };

    let deleted = special & !empty;

    InsertScan {
        matches,
        empty,
        deleted,
    }
}
