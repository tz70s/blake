use crate::schema::Schema;

/// DB contains a heap-based storage (as the main tuple store).
/// A heap storage contains multiple key-value pairs.
/// Optionally, series of B-tree based index storage, of which from a (composite) key to heap tuple id.
#[derive(Debug)]
pub struct DB {
    schema: Schema,
}

impl DB {
    /// new creates a DB instance.
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }

    /// scan scans all data out with a set of filtering.
    pub async fn scan(&self, exprs: Vec<Expr>) -> anyhow::Result<RecordBatch> {
        unimplemented!();
    }

    pub async fn insert(&self, record_batches: RecordBatch) -> anyhow::Result<()> {
        unimplemented!();
    }
}

pub struct Tx {}

impl Tx {
    /// scan scans all data out with a set of filtering.
    pub async fn scan(&self, exprs: Vec<Expr>) -> anyhow::Result<RecordBatch> {
        unimplemented!();
    }

    pub async fn insert(&self, record_batches: RecordBatch) -> anyhow::Result<()> {
        unimplemented!();
    }

    /// commit commits the transaction.
    pub async fn commit(&self) -> anyhow::Result<()> {
        unimplemented!();
    }

    /// rollback rollbacks the transaction.
    pub async fn rollback(&self) -> anyhow::Result<()> {
        unimplemented!();
    }
}

pub enum Expr {
    Eq(String, Vec<u8>),
}

#[derive(Debug)]
pub struct RecordBatch {
    keys: Vec<String>,
    values: Vec<Vec<Vec<u8>>>,
}
