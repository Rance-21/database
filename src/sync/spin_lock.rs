use std::hint::spin_loop;
use std::sync::atomic::{AtomicU32, Ordering};

// futex 必须使用 4 字节对齐的 32-bit word，不能使用 AtomicBool。
const UNLOCKED: u32 = 0;
const LOCKED: u32 = 1;
const CONTENDED: u32 = 2;
const SPIN_LIMIT: usize = 50;

// 写的时候注意false sharing
pub struct SpinLock {
    state: AtomicU32,
}

// 写的时候注意false sharing
pub struct SpinLockGuard<'a> {
    lock: &'a SpinLock,
}

impl SpinLock {
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(UNLOCKED),
        }
    }

    // 最多自旋尝试 50 次；仍未获取锁时进入 futex 睡眠。
    pub fn lock(&self) -> SpinLockGuard<'_> {
        for _ in 0..SPIN_LIMIT {
            if self.state.load(Ordering::Relaxed) == UNLOCKED
                && self
                    .state
                    .compare_exchange_weak(
                        UNLOCKED,
                        LOCKED,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                return SpinLockGuard { lock: self };
            }

            spin_loop();
        }

        /*
         * 0 = 空闲；1 = 持锁且尚未标记竞争；2 = 持锁且存在竞争者。
         *
         * 慢路径取得锁时保留状态 2，避免还有其他 futex waiter
         * 的情况下直接恢复为 1，导致后续 unlock 不再唤醒它们。
         *
         * futex WAIT 只有在当前值仍是 2 时才真正睡眠：
         * unlock 如果已经把状态设为 0，内核会返回 EAGAIN，
         * 因此不会发生「检查状态后、睡眠前」的 lost wakeup。
         */
        loop {
            if self.state.swap(CONTENDED, Ordering::Acquire) == UNLOCKED {
                return SpinLockGuard { lock: self };
            }

            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.state.as_ptr(),
                    // libc 分别导出操作码和私有标记，需要按位组合。
                    libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                    CONTENDED,
                    std::ptr::null::<libc::timespec>(),
                );
            }
            // EINTR / EAGAIN / 正常唤醒，都重新尝试获取锁。
        }
    }

    // 非阻塞尝试；不进行自旋，也不进入 futex wait。
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_>> {
        self.state
            .compare_exchange(
                UNLOCKED,
                LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .ok()
            .map(|_| SpinLockGuard { lock: self })
    }
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        // Release 发布临界区写入；只有竞争态才需要 futex wake 系统调用。
        if self.lock.state.swap(UNLOCKED, Ordering::Release) == CONTENDED {
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.lock.state.as_ptr(),
                    libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                    1,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize},
            mpsc,
        },
        thread,
        time::Duration,
    };

    #[test]
    fn try_lock_is_nonblocking() {
        let lock = SpinLock::new();

        let guard = lock.lock();
        assert!(lock.try_lock().is_none());

        drop(guard);
        assert!(lock.try_lock().is_some());
    }

    #[test]
    fn blocked_thread_wakes_after_unlock() {
        let lock = Arc::new(SpinLock::new());
        let guard = lock.lock();

        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiting_lock = Arc::clone(&lock);

        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _guard = waiting_lock.lock();
            done_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        // 锁仍被主线程持有，另一个线程不能越过 lock()。
        assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());

        drop(guard);
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multiple_waiters_make_progress_and_remain_exclusive() {
        const THREADS: usize = 8;
        const LOOPS: usize = 500;

        let lock = Arc::new(SpinLock::new());
        let inside = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicUsize::new(0));
        let initial_guard = lock.lock();

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let lock = Arc::clone(&lock);
                let inside = Arc::clone(&inside);
                let completed = Arc::clone(&completed);

                thread::spawn(move || {
                    for _ in 0..LOOPS {
                        let _guard = lock.lock();
                        assert!(!inside.swap(true, Ordering::SeqCst));
                        completed.fetch_add(1, Ordering::Relaxed);
                        inside.store(false, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        // 让多个线程有机会走到 futex wait 的慢路径。
        thread::sleep(Duration::from_millis(30));
        drop(initial_guard);

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(completed.load(Ordering::Relaxed), THREADS * LOOPS);
        assert!(!inside.load(Ordering::Relaxed));
    }
}
