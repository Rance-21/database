// 基于区域的内存管理技术
use crate::storage::{PAGE_SIZE, page::PageData};
use std::io::{Error, Result};
use std::ptr::NonNull;

pub struct PageArena {
    // 承诺不为空以减少标志位空间
    ptr: NonNull<u8>,
    frame_count: usize,
}

impl PageArena {
    // 申请缓冲池空头支票
    pub fn new(frame_count: usize) -> Result<Self> {
        let len = frame_count.checked_mul(PAGE_SIZE).expect("缓冲池过大");

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        Ok(Self {
            // C语言void指针强制转换mut u8
            ptr: NonNull::new(ptr.cast()).unwrap(),
            frame_count,
        })
    }

    // 根据帧id返回内存上的页
    pub fn frame_ptr(&self, frame_id: usize) -> *mut PageData {
        unsafe { self.ptr.as_ptr().add(frame_id * PAGE_SIZE).cast() }
    }
}

impl Drop for PageArena {
    fn drop(&mut self) {
        let len = self.frame_count * PAGE_SIZE;

        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), len);
        }
    }
}
