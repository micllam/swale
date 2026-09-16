//! The partition key of an asset, in the charset of a run id.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Maximum length of a partition key in bytes.
pub const MAX_PARTITION_LEN: usize = 16;

/// The key of an unpartitioned asset.
pub const UNPARTITIONED: &str = "none";

/// A partition key: `[A-Za-z0-9_]` of one to [`MAX_PARTITION_LEN`] bytes. A
/// date partition is `20260915`, an hourly one `20260915T02` and an
/// unpartitioned asset uses [`UNPARTITIONED`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Partition(String);

/// The text is not a partition key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a partition key: `[A-Za-z0-9_]` of one to {MAX_PARTITION_LEN} bytes")]
pub struct InvalidPartition(pub String);

impl Partition {
    /// Validates `text` as a partition key.
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidPartition> {
        let text = text.into();
        let valid = !text.is_empty()
            && text.len() <= MAX_PARTITION_LEN
            && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if valid {
            Ok(Partition(text))
        } else {
            Err(InvalidPartition(text))
        }
    }

    /// The key of an unpartitioned asset.
    pub fn none() -> Self {
        Partition(UNPARTITIONED.to_string())
    }

    /// The key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Partition {
    type Error = InvalidPartition;

    fn try_from(text: String) -> Result<Self, InvalidPartition> {
        Partition::new(text)
    }
}

impl From<Partition> for String {
    fn from(partition: Partition) -> String {
        partition.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_documented_forms_and_rejects_the_rest() {
        for text in ["20260915", "20260915T02", "none", "a", "0123456789ABCDEF"] {
            assert_eq!(Partition::new(text).unwrap().as_str(), text);
        }
        for text in ["", "2026-09-15", "0123456789ABCDEFG", "a b", "é"] {
            assert_eq!(
                Partition::new(text),
                Err(InvalidPartition(text.to_string()))
            );
        }
        assert_eq!(Partition::none().as_str(), UNPARTITIONED);
    }

    #[test]
    fn serde_form_is_the_key_text_and_is_validated() {
        let partition: Partition = serde_json::from_str("\"20260915\"").unwrap();
        assert_eq!(serde_json::to_string(&partition).unwrap(), "\"20260915\"");
        assert!(serde_json::from_str::<Partition>("\"2026-09-15\"").is_err());
    }
}
