#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Oid(u64);

impl Oid {
    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<(PageId, TupleId)> for Oid {
    fn from((page_id, tuple_id): (PageId, TupleId)) -> Self {
        Self((page_id.0 as u64) << 32 + tuple_id.0 as u64)
    }
}

impl From<Oid> for (PageId, TupleId) {
    fn from(value: Oid) -> (PageId, TupleId) {
        let page_id = PageId((value.0 >> 32) as u32);
        let tuple_id = TupleId(value.0 as u32);
        (page_id, tuple_id)
    }
}

impl From<u64> for Oid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Oid> for u64 {
    fn from(value: Oid) -> u64 {
        value.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct PageId(u32);

impl PageId {
    pub fn get(&self) -> u32 {
        self.0
    }
}

impl From<u32> for PageId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<PageId> for u32 {
    fn from(value: PageId) -> Self {
        value.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TupleId(u32);

impl TupleId {
    pub fn get(&self) -> u32 {
        self.0
    }
}

impl From<u32> for TupleId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<TupleId> for u32 {
    fn from(value: TupleId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::{Oid, PageId, TupleId};

    #[test]
    fn oid_from_tup() {
        let page_id = PageId(1);
        let tuple_id = TupleId(2);
        let expected = Oid(0x0000000100000002);
        assert_eq!(expected, (page_id, tuple_id).into())
    }

    #[test]
    fn oid_into_tup() {
        let oid = Oid(0x0000000100000002);
        let (page_id, tuple_id) = oid.into();
        assert_eq!(PageId::from(0x00000001), page_id);
        assert_eq!(TupleId::from(0x00000001), tuple_id);
    }
}
