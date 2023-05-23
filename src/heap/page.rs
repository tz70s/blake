use super::{PageId, TupleId};
use anyhow::ensure;
use bytes::{Buf, BytesMut};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

const PAGE_SIZE: usize = 8 * 1024;

pub struct PageGuard {
    inner: Arc<Mutex<Page>>,
}

impl From<Page> for PageGuard {
    fn from(value: Page) -> Self {
        Self {
            inner: Arc::new(Mutex::new(value)),
        }
    }
}

/// Page contains the content of a page.
#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    slots: HashMap<TupleId, Slot>,
    inner: BytesMut,
}

impl Page {
    /// Create a Page from raw bytes.
    pub fn try_from(data: BytesMut) -> anyhow::Result<Self> {
        let mut cursor = Cursor::new(data.as_ref());
        let header = PageHeader::try_from(&mut cursor)?;
        let mut slots = HashMap::with_capacity(header.nslots);
        for _ in 0..header.nslots {
            let slot = Slot::try_from(&mut cursor)?;
            slots.insert(slot.tuple_id, slot);
        }
        Ok(Page {
            header,
            slots,
            inner: data,
        })
    }

    /// Returns the remaining size of the page.
    pub fn remaining(&self) -> usize {
        PAGE_SIZE - self.header.size
    }
}

/// PageHeader contains the metadata to fastly index the data within page.
///
/// The real content of the page header as follows:
///
/// --------------------------------------------------------------------
/// | page_id (u64) | page_size (u32) | nslots (u32) | slots (slot) ... |
/// --------------------------------------------------------------------
#[derive(Debug, Default)]
struct PageHeader {
    page_id: PageId,
    size: usize,
    nslots: usize,
}

const PAGE_HEADER_SIZE: usize = 4 + 4 + 4;

impl PageHeader {
    fn try_from(data: &mut impl Buf) -> anyhow::Result<Self> {
        ensure!(
            data.remaining() >= PAGE_HEADER_SIZE,
            "data corruption: page header"
        );
        let page_id = PageId::from(data.get_u32());
        let size = data.get_u32() as usize;
        let nslots = data.get_u32() as usize;

        Ok(Self {
            page_id,
            size,
            nslots,
        })
    }
}

#[derive(Debug)]
struct Slot {
    tuple_id: TupleId,
    offset: usize,
}

impl Slot {
    fn try_from(data: &mut impl Buf) -> anyhow::Result<Self> {
        ensure!(data.remaining() >= 8, "data corruption: tuple slot");
        let tuple_id = TupleId::from(data.get_u32());
        let offset = data.get_u64() as usize;
        Ok(Slot { tuple_id, offset })
    }
}
