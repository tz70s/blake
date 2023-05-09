use std::{collections::HashMap, fmt::Display};

use anyhow::bail;

/// Schema defines the schema of the dataset.
#[derive(Debug)]
pub struct Schema {
    fds: HashMap<String, Field>,
    indices: HashMap<String, Vec<String>>,
}

impl Schema {
    /// from creates a schema from a vector of fields.
    pub fn from(fds: Vec<Field>) -> anyhow::Result<Self> {
        let mut unique_fds = HashMap::new();
        for fd in fds {
            if unique_fds.contains_key(&fd.name) {
                bail!(
                    "Schema should contain unique fd names only, {} is duplicated",
                    fd.name
                );
            }
            unique_fds.insert(fd.name.clone(), fd);
        }
        anyhow::Ok(Self {
            fds: unique_fds,
            indices: HashMap::new(),
        })
    }

    /// with_index creates an index.
    pub fn with_index(mut self, name: String, fds: Vec<String>) -> anyhow::Result<Self> {
        // Validate if all fds in unique_fds.
        for fd in &fds {
            if !self.fds.contains_key(fd) {
                bail!("Unknown field name {} to create index", fd);
            }
        }
        self.indices.insert(name, fds);
        anyhow::Ok(self)
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, (_, fd)) in self.fds.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{fd}")?;
        }
        for (i, (name, fds)) in self.indices.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name}{fds:?}")?;
        }
        write!(f, "]")
    }
}

/// Field is the field of the schema.
/// As of now, no any constraints will be implemented.
#[derive(Debug)]
pub struct Field {
    name: String,
}

impl Field {
    /// new creates a new field.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Field({})", self.name)
    }
}
