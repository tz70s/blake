use anyhow::ensure;
use bytes::Buf;
use std::io::Cursor;

pub struct Tuple<'a> {
    header: TupleHeader,
    data: &'a [u8],
}

impl Tuple<'_> {
    /// try_from reads the tuple.
    pub fn try_from(data: &[u8]) -> anyhow::Result<Tuple> {
        let mut cursor = Cursor::new(data);
        let header = TupleHeader::try_from(&mut cursor)?;
        ensure!(
            cursor.remaining() >= header.size,
            "data corruption: tuple body"
        );
        let data = &data[TUPLE_HEADER_SIZE..TUPLE_HEADER_SIZE + header.size];
        Ok(Tuple { header, data })
    }
}

// ------------------------------------
// | oid | next_oid | null_map | size |
// ------------------------------------
pub struct TupleHeader {
    oid: u64,
    next_oid: u64,
    version: u32,
    // null_map configures the field nullability.
    null_map: u32,
    size: usize,
}

const TUPLE_HEADER_SIZE: usize = 8 + 8 + 4 + 4 + 4;

impl TupleHeader {
    fn try_from(data: &mut impl Buf) -> anyhow::Result<TupleHeader> {
        ensure!(
            data.remaining() >= TUPLE_HEADER_SIZE,
            "data corruption: tuple header"
        );
        let oid = data.get_u64();
        let next_oid = data.get_u64();
        let version = data.get_u32();
        let null_map = data.get_u32();
        let size = data.get_u32() as usize;

        Ok(TupleHeader {
            oid,
            next_oid,
            version,
            null_map,
            size,
        })
    }
}
