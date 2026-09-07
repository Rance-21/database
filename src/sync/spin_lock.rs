use std::hint::spin_loop;
use std::sync::atomic::{AtomicBool, Ordering};

// 写的时候注意false sharing
pub struct SpinLock {
    locked: AtomicBool,
}

// 写的时候注意false sharing
pub struct SpinLockGuard<'a> {
    lock: &'a SpinLock,
}

impl SpinLock {
    pub const fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    // 等待解锁
    pub fn lock(&self) -> SpinLockGuard<'_> {
        loop {
            while self.locked.load(Ordering::Relaxed) {
                spin_loop();
            }

            if self
                .locked
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return SpinLockGuard { lock: self };
            }
        }
    }

    // 尝试解锁
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_>> {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| SpinLockGuard { lock: self })
    }
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}
