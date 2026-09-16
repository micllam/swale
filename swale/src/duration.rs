//! Durations in the definition file: an integer and a unit, such as `30s`,
//! `5m`, `6h` or `7d`.

use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// The text is not an integer followed by one of the units `s`, `m`, `h` or
/// `d`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a duration such as `30s`, `5m`, `6h` or `7d`")]
pub struct InvalidDuration(pub String);

/// Parses a duration such as `7d`.
pub fn parse(text: &str) -> Result<Duration, InvalidDuration> {
    let invalid = || InvalidDuration(text.to_string());
    let digits = text.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = &text[digits.len()..];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let count: u64 = digits.parse().map_err(|_| invalid())?;
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(invalid()),
    };
    count
        .checked_mul(seconds)
        .map(Duration::from_secs)
        .ok_or_else(invalid)
}

/// Deserializes a duration from its text form, for a serde field.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    parse(&text).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_unit() {
        assert_eq!(parse("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(parse("6h"), Ok(Duration::from_secs(21_600)));
        assert_eq!(parse("7d"), Ok(Duration::from_secs(604_800)));
    }

    #[test]
    fn rejects_text_without_an_integer_and_a_known_unit() {
        for text in ["", "7", "d", "7w", "7 d", "-7d", "1.5h", "7dd"] {
            assert_eq!(
                parse(text),
                Err(InvalidDuration(text.to_string())),
                "{text}"
            );
        }
    }

    #[test]
    fn rejects_an_overflowing_count() {
        assert!(parse("99999999999999999999d").is_err());
    }
}
