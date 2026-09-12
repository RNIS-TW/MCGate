//! Port of the duration/byte-size parsing helpers in `me.hippodev.config.Config`.

use anyhow::{bail, Result};

/// Parses durations like "3m", "60s", "500ms", "-1s", "30d" into milliseconds.
pub fn parse_duration_millis(value: &str) -> Result<i64> {
    let value = value.trim();
    let unit_start = value
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .ok_or_else(|| anyhow::anyhow!("Invalid duration: {value}"))?;
    let (amount_str, unit) = value.split_at(unit_start);
    let amount: i64 = amount_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid duration: {value}"))?;
    let millis = match unit {
        "ms" => amount,
        "s" => amount * 1000,
        "m" => amount * 60_000,
        "h" => amount * 3_600_000,
        "d" => amount * 86_400_000,
        _ => bail!("Invalid duration unit: {value}"),
    };
    Ok(millis)
}

/// Parses byte sizes for `metrics.*.limit`: a plain integer is bytes, or a `k`/`m`/`g`/`t`/`p`
/// suffix multiplies by the matching power of 1024 (an optional `i`/`b`/`ib`/`b` is accepted and
/// ignored, so `100g`, `100G`, `100GiB`, `100gb` are all `100 * 1024^3`). Any negative value
/// means "unlimited" and normalizes to -1.
pub fn parse_byte_size(value: &str) -> Result<i64> {
    let trimmed = value.trim();
    let digits_end = trimmed
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(trimmed.len());
    if digits_end == 0 || (digits_end == 1 && trimmed.starts_with('-')) {
        bail!("invalid byte size: {value} (try e.g. 100g, 512m, 5242880, or -1)");
    }
    let amount: i64 = trimmed[..digits_end]
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid byte size: {value} (try e.g. 100g, 512m, 5242880, or -1)"))?;
    if amount < 0 {
        return Ok(-1);
    }
    let unit = trimmed[digits_end..].trim().to_lowercase();
    let unit = unit.strip_suffix("ib").or_else(|| unit.strip_suffix('b')).unwrap_or(&unit);
    let factor: i64 = match unit {
        "" => 1,
        "k" => 1024,
        "m" => 1024 * 1024,
        "g" => 1024 * 1024 * 1024,
        "t" => 1024i64 * 1024 * 1024 * 1024,
        "p" => 1024i64 * 1024 * 1024 * 1024 * 1024,
        _ => bail!("invalid byte size unit: {value}"),
    };
    amount
        .checked_mul(factor)
        .ok_or_else(|| anyhow::anyhow!("byte size out of range: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_millis("3m").unwrap(), 180_000);
        assert_eq!(parse_duration_millis("60s").unwrap(), 60_000);
        assert_eq!(parse_duration_millis("500ms").unwrap(), 500);
        assert_eq!(parse_duration_millis("30d").unwrap(), 30 * 86_400_000);
        assert_eq!(parse_duration_millis("-1s").unwrap(), -1000);
        assert!(parse_duration_millis("bogus").is_err());
    }

    #[test]
    fn byte_sizes() {
        assert_eq!(parse_byte_size("100g").unwrap(), 100i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("100G").unwrap(), 100i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("100GiB").unwrap(), 100i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("100gb").unwrap(), 100i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("-1").unwrap(), -1);
        assert_eq!(parse_byte_size("5242880").unwrap(), 5242880);
        assert!(parse_byte_size("nope").is_err());
    }
}
