use std::fmt;

use crate::graph::Problem;

/// The failure of a definition load.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file cannot be read.
    #[error("cannot read the definition: {0}")]
    Io(#[from] std::io::Error),
    /// The text is not a definition file.
    #[error("the definition does not parse: {0}")]
    Parse(#[from] toml::de::Error),
    /// The definition parses and has at least one fault.
    #[error("{}", ProblemList(.0))]
    Invalid(Vec<Problem>),
}

struct ProblemList<'a>(&'a [Problem]);

impl fmt::Display for ProblemList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the definition is invalid:")?;
        for problem in self.0 {
            write!(f, "\n  {problem}")?;
        }
        Ok(())
    }
}
