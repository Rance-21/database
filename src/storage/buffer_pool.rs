use crate::storage::{
    arena::PageArena,
    disk::DiskManager,
    page::{FrameMeta, PageData},
};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};

pub struct BufferPool {
    disk: DiskManager,
    arena: PageArena,
    metadata: Box<[FrameMeta]>,
}
