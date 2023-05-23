use dashmap::DashMap;
use page::Page;
use page_dir::PageDir;
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

mod ident;
pub mod page;
mod page_dir;
pub mod tuple;

pub use ident::Oid;
pub use ident::PageId;
pub use ident::TupleId;

const BUFFER_POOL_SIZE: usize = 1024;

/// BufferPool contains a set of pages.
/// The replacement algorithm is based on circle.
#[derive(Debug)]
pub struct HeapTable {
    // Configuration.
    data_file: PathBuf,
    // Inner state.
    inner: DashMap<PageId, Page>,
    page_dir: PageDir,
}

#[derive(Debug, Clone)]
pub struct HeapTableRef(Arc<HeapTable>);

impl HeapTableRef {
    /// Create a new buffer pool owned reference.
    pub fn new(heap_table: HeapTable) -> HeapTableRef {
        HeapTableRef(Arc::new(heap_table))
    }
}

impl Deref for HeapTableRef {
    type Target = HeapTable;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl HeapTable {
    /// Creates a new buffer pool.
    pub async fn new<P: AsRef<Path>>(data_file: P, page_dir_file: P) -> anyhow::Result<HeapTable> {
        let page_dir = PageDir::new(page_dir_file).await?;
        let inner = DashMap::with_capacity(BUFFER_POOL_SIZE);
        Ok(HeapTable {
            data_file: data_file.as_ref().into(),
            inner,
            page_dir,
        })
    }
}
