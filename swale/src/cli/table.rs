//! The tables of the status commands.

/// The first twelve characters of a definition hash.
pub(crate) fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

/// A time in milliseconds from the Unix epoch as UTC, to the second.
pub(crate) fn format_time(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            || ms.to_string(),
            |time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )
}

/// The header and the rows as columns padded to the widest cell.
fn format_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) -> String {
    let mut widths = header.map(str::len);
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let mut text = String::new();
    let mut push = |cells: [&str; N]| {
        let line: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        text.push_str(line.join("  ").trim_end());
        text.push('\n');
    };
    push(header);
    for row in rows {
        push(row.each_ref().map(String::as_str));
    }
    text
}

pub(crate) fn print_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) {
    print!("{}", format_table(header, rows));
}
