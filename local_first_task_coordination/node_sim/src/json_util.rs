//! Minimal hand-rolled JSON rendering helpers.
//!
//! The project intentionally has zero third-party dependencies (see the
//! top-level README), so scenario/metrics output is serialized by hand
//! instead of via `serde_json`. These helpers centralize the escaping and
//! array-formatting logic so `main.rs` and `scenarios.rs` don't each
//! reimplement it slightly differently.

/// Escapes a string for embedding in a JSON string literal.
///
/// Handles the two characters that are structurally required (`\` and `"`)
/// plus the ASCII control characters (0x00-0x1F) that would otherwise
/// produce invalid JSON if they ever appeared in log text — a literal
/// newline or tab inside a JSON string is not legal per the JSON grammar.
pub fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Renders a string as a quoted, escaped JSON string literal.
pub fn json_string(s: &str) -> String {
    format!("\"{}\"", escape_json_string(s))
}

/// Renders a slice of `usize` as a JSON array, e.g. `[1, 2, 3]`.
pub fn usize_array(values: &[usize]) -> String {
    let items: Vec<String> = values.iter().map(|n| n.to_string()).collect();
    format!("[{}]", items.join(", "))
}

/// Renders `Some(n)` as the number `n` and `None` as JSON `null`.
pub fn opt_usize(value: Option<usize>) -> String {
    match value {
        Some(n) => n.to_string(),
        None => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_backslash_and_quote() {
        assert_eq!(escape_json_string(r#"a\b"c"#), r#"a\\b\"c"#);
    }

    #[test]
    fn escapes_newline_tab_and_carriage_return() {
        assert_eq!(escape_json_string("a\nb\tc\rd"), "a\\nb\\tc\\rd");
    }

    #[test]
    fn escapes_other_control_characters() {
        assert_eq!(escape_json_string("a\u{1}b"), "a\\u0001b");
    }

    #[test]
    fn leaves_ordinary_text_unchanged() {
        assert_eq!(escape_json_string("hello world"), "hello world");
    }

    #[test]
    fn json_string_wraps_and_escapes() {
        assert_eq!(json_string("a\"b"), "\"a\\\"b\"");
    }

    #[test]
    fn usize_array_renders_comma_separated() {
        assert_eq!(usize_array(&[1, 2, 3]), "[1, 2, 3]");
        assert_eq!(usize_array(&[]), "[]");
    }

    #[test]
    fn opt_usize_renders_null_for_none() {
        assert_eq!(opt_usize(Some(5)), "5");
        assert_eq!(opt_usize(None), "null");
    }
}
