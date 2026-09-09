use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::os::fd::AsRawFd;
use std::path::Path;

use super::{PAGE_SIZE, page::PageData};

pub struct DiskManager {
    file: File,
    next_page_id: u64,
}

impl DiskManager {
    // 打开SSD上的文件
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_size = file.metadata()?.len();
        let next_page_id = file_size / PAGE_SIZE as u64;

        Ok(Self { file, next_page_id })
    }

    //SSD文件多一页
    pub fn allocate_page(&mut self) -> Result<u64> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        self.file.set_len(self.next_page_id * PAGE_SIZE as u64)?;

        Ok(page_id)
    }

    pub unsafe fn read_page(&self, page_id: u64, dst: *mut PageData) -> Result<()> {
        let mut read = 0;

        while read < PAGE_SIZE {
            let ptr = unsafe { dst.cast::<u8>().add(read) };

            let n = unsafe {
                libc::pread(
                    self.file.as_raw_fd(),
                    ptr.cast(),
                    PAGE_SIZE - read,
                    (page_id * PAGE_SIZE as u64 + read as u64) as libc::off_t,
                )
            };

            if n < 0 {
                let error = Error::last_os_error();

                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }

                return Err(error);
            }

            if n == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }

            read += n as usize;
        }

        Ok(())
    }

    pub unsafe fn write_page(&self, page_id: u64, src: *const PageData) -> Result<()> {
        let mut written = 0;

        while written < PAGE_SIZE {
            let ptr = unsafe { src.cast::<u8>().add(written) };

            let n = unsafe {
                libc::pwrite(
                    self.file.as_raw_fd(),
                    ptr.cast(),
                    PAGE_SIZE - written,
                    (page_id * PAGE_SIZE as u64 + written as u64) as libc::off_t,
                )
            };

            if n < 0 {
                let error = Error::last_os_error();

                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }

                return Err(error);
            }

            written += n as usize;
        }

        Ok(())
    }
}
