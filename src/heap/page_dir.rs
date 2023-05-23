use super::PageId;
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const PAGE_LOCATION_SIZE: usize = 4 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageLocation(pub usize);

#[derive(Debug)]
pub struct PageDir {
    // Page directory file path.
    file: PathBuf,
    // In-memory page directory.
    inner: DashMap<PageId, PageLocation>,
}

impl PageDir {
    pub async fn new(file: impl AsRef<Path>) -> anyhow::Result<PageDir> {
        let inner = Self::inner_from_disk(file.as_ref()).await?;
        Ok(Self {
            file: file.as_ref().into(),
            inner,
        })
    }

    async fn inner_from_disk(
        file: impl AsRef<Path>,
    ) -> anyhow::Result<DashMap<PageId, PageLocation>> {
        let mut page_dir_file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file.as_ref())
            .await?;

        let inner = DashMap::new();
        let mut buf = BytesMut::with_capacity(1024);
        loop {
            if buf.remaining() > PAGE_LOCATION_SIZE {
                let page_id = buf.get_u32();
                let offset = buf.get_u64();
                inner.insert(PageId::from(page_id), PageLocation(offset as usize));
                continue;
            }
            match page_dir_file.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        // FIXME: the write here is not atomic.
        // Should write the compacted temporary file and replace the original temporary file.

        // The insertion of the map essentially do the compaction.
        // Flush it again into file.
        page_dir_file.seek(std::io::SeekFrom::Start(0)).await?;

        for ref_multi in inner.iter() {
            page_dir_file.write_u32((*ref_multi.key()).into()).await?;
            page_dir_file.write_u64(ref_multi.value().0 as u64).await?;
        }

        page_dir_file
            .set_len((inner.len() * PAGE_LOCATION_SIZE) as u64)
            .await?;

        Ok(inner)
    }

    pub async fn insert(&self, page_id: PageId, page_location: PageLocation) -> anyhow::Result<()> {
        // FIXME: not atomic operation.

        let mut page_dir_file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file)
            .await?;
        page_dir_file.write_u32(page_id.into()).await?;
        page_dir_file.write_u64(page_location.0 as u64).await?;
        self.inner.insert(page_id, page_location);

        Ok(())
    }
}
