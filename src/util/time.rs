//! Time and date parsing utilities.

use crate::error::{BeadsError, Result};
use chrono::{DateTime, Duration, Local, NaiveDate, NaiveTime, TimeZone, Utc};

/// Parse a flexible time specification into a `DateTime<Utc>`.
///
/// Supports:
/// - RFC3339: `2025-01-15T12:00:00Z`, `2025-01-15T12:00:00+00:00`
/// - Simple date: `2025-01-15` (defaults to 9:00 AM local time)
/// - Relative duration: `+1h`, `+2d`, `+1w`, `+30m`
/// - Keywords: `tomorrow`, `next-week`
///
/// # Errors
///
/// Returns an error if:
/// - The time format is invalid or unrecognized
/// - A relative duration has an invalid unit (only m, h, d, w supported)
/// - The local time is ambiguous (e.g., during DST transitions)
///
/// # Panics
///
/// This function does not panic. The internal `unwrap()` calls on `from_hms_opt(9, 0, 0)`
/// are safe because 9:00:00 is always a valid time.
pub fn parse_flexible_timestamp(s: &str, field_name: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();

    // Try RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try simple date (YYYY-MM-DD) - default to 9:00 AM local time
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        let naive_dt = date.and_time(time);
        return local_to_utc(&naive_dt, field_name);
    }

    // Try relative duration (+1h, +2d, +1w, +30m, -7d)
    if let Some(rest) = s.strip_prefix(['+', '-'].as_ref()) {
        let is_negative = s.starts_with('-');
        if let Some(unit_char) = rest.chars().last() {
            let amount_str = &rest[..rest.len() - unit_char.len_utf8()];
            if let Ok(amount) = amount_str.parse::<i64>() {
                let signed_amount = if is_negative { -amount } else { amount };
                // Clamp amount to a safe range to avoid panic in Duration methods.
                // 1000 years is plenty for any issue tracker.
                let max_safe_amount = 365 * 1000;
                let clamped_amount = signed_amount.clamp(-max_safe_amount, max_safe_amount);
                let duration = match unit_char {
                    'm' => Duration::minutes(clamped_amount),
                    'h' => Duration::hours(clamped_amount),
                    'd' => Duration::days(clamped_amount),
                    'w' => Duration::weeks(clamped_amount.clamp(-52000, 52000)), // ~1000 years in weeks
                    _ => {
                        return Err(BeadsError::validation(
                            field_name,
                            "invalid unit (use m, h, d, w)",
                        ));
                    }
                };
                return Ok(Utc::now() + duration);
            }
        }
    }

    // Try keywords
    let now = Local::now();
    match s.to_lowercase().as_str() {
        "today" => {
            let time = NaiveTime::from_hms_opt(17, 0, 0).unwrap();
            let naive_dt = now.date_naive().and_time(time);
            Ok(local_to_utc(&naive_dt, field_name)?)
        }
        "yesterday" => {
            let yesterday = now.date_naive() - Duration::days(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
            let naive_dt = yesterday.and_time(time);
            Ok(local_to_utc(&naive_dt, field_name)?)
        }
        "tomorrow" => {
            let tomorrow = now.date_naive() + Duration::days(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
            let naive_dt = tomorrow.and_time(time);
            Ok(local_to_utc(&naive_dt, field_name)?)
        }
        "next-week" | "nextweek" => {
            let next_week = now.date_naive() + Duration::weeks(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
            let naive_dt = next_week.and_time(time);
            Ok(local_to_utc(&naive_dt, field_name)?)
        }
        _ => Err(BeadsError::validation(
            field_name,
            "invalid time format (try: +1h, -7d, tomorrow, next-week, or 2025-01-15)",
        )),
    }
}

/// Parse a relative time expression into a `DateTime<Utc>`.
///
/// Supports:
/// - Relative duration: `+1h`, `+2d`, `+1w`, `+30m`, `-7d`
/// - Keywords: `today`, `yesterday`, `tomorrow`, `next-week`
///
/// Returns `None` if the input cannot be parsed as a relative time.
#[must_use]
pub fn parse_relative_time(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();

    // Try relative duration (+1h, -2d, +1w, -30m)
    if let Some(rest) = s.strip_prefix(['+', '-'].as_ref()) {
        let is_negative = s.starts_with('-');
        if let Some(unit_char) = rest.chars().last() {
            let amount_str = &rest[..rest.len() - unit_char.len_utf8()];
            if let Ok(amount) = amount_str.parse::<i64>() {
                let signed_amount = if is_negative { -amount } else { amount };
                // Clamp amount to a safe range to avoid panic in Duration methods.
                let max_safe_amount = 365 * 1000;
                let clamped_amount = signed_amount.clamp(-max_safe_amount, max_safe_amount);
                let duration = match unit_char {
                    'm' => Duration::minutes(clamped_amount),
                    'h' => Duration::hours(clamped_amount),
                    'd' => Duration::days(clamped_amount),
                    'w' => Duration::weeks(clamped_amount.clamp(-52000, 52000)),
                    _ => return None,
                };
                return Some(Utc::now() + duration);
            }
        }
    }

    // Try keywords
    let now = Local::now();
    match s.to_lowercase().as_str() {
        "today" => {
            let time = NaiveTime::from_hms_opt(17, 0, 0)?;
            let naive_dt = now.date_naive().and_time(time);
            local_to_utc_opt(&naive_dt)
        }
        "yesterday" => {
            let yesterday = now.date_naive() - Duration::days(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0)?;
            let naive_dt = yesterday.and_time(time);
            local_to_utc_opt(&naive_dt)
        }
        "tomorrow" => {
            let tomorrow = now.date_naive() + Duration::days(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0)?;
            let naive_dt = tomorrow.and_time(time);
            local_to_utc_opt(&naive_dt)
        }
        "next-week" | "nextweek" => {
            let next_week = now.date_naive() + Duration::weeks(1);
            let time = NaiveTime::from_hms_opt(9, 0, 0)?;
            let naive_dt = next_week.and_time(time);
            local_to_utc_opt(&naive_dt)
        }
        _ => None,
    }
}

/// Format a duration as a human-readable relative time string (e.g., "2 days ago").
#[must_use]
pub fn format_relative_time(dt: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let duration = if dt > now {
        dt.signed_duration_since(now)
    } else {
        now.signed_duration_since(dt)
    };

    let suffix = if dt > now { "from now" } else { "ago" };

    let seconds = duration.num_seconds();
    if seconds < 60 {
        return "just now".to_string();
    }

    let minutes = duration.num_minutes();
    if minutes < 60 {
        return format!(
            "{} minute{} {}",
            minutes,
            if minutes == 1 { "" } else { "s" },
            suffix
        );
    }

    let hours = duration.num_hours();
    if hours < 24 {
        return format!(
            "{} hour{} {}",
            hours,
            if hours == 1 { "" } else { "s" },
            suffix
        );
    }

    let days = duration.num_days();
    if days < 30 {
        return format!(
            "{} day{} {}",
            days,
            if days == 1 { "" } else { "s" },
            suffix
        );
    }

    if days < 365 {
        #[allow(clippy::cast_possible_truncation)]
        let months = (days as f64 / 30.44).round() as i64;
        let months = months.max(1);
        return format!(
            "{} month{} {}",
            months,
            if months == 1 { "" } else { "s" },
            suffix
        );
    }

    let years = days / 365;
    let years = years.max(1);
    format!(
        "{} year{} {}",
        years,
        if years == 1 { "" } else { "s" },
        suffix
    )
}

fn local_to_utc(naive_dt: &chrono::NaiveDateTime, field_name: &str) -> Result<DateTime<Utc>> {
    use chrono::LocalResult;
    match Local.from_local_datetime(naive_dt) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => Ok(dt.with_timezone(&Utc)),
        LocalResult::None => {
            // Time doesn't exist (DST gap), push forward by 1 hour
            let shifted = *naive_dt + Duration::hours(1);
            match Local.from_local_datetime(&shifted) {
                LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => {
                    Ok(dt.with_timezone(&Utc))
                }
                LocalResult::None => Err(BeadsError::validation(
                    field_name,
                    "invalid local time around DST transition",
                )),
            }
        }
    }
}

fn local_to_utc_opt(naive_dt: &chrono::NaiveDateTime) -> Option<DateTime<Utc>> {
    use chrono::LocalResult;
    match Local.from_local_datetime(naive_dt) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => Some(dt.with_timezone(&Utc)),
        LocalResult::None => {
            let shifted = *naive_dt + Duration::hours(1);
            match Local.from_local_datetime(&shifted) {
                LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => {
                    Some(dt.with_timezone(&Utc))
                }
                LocalResult::None => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_flexible_rfc3339() {
        let result = parse_flexible_timestamp("2025-01-15T12:00:00Z", "test").unwrap();
        assert_eq!(result.year(), 2025);
    }

    #[test]
    fn test_parse_flexible_simple_date() {
        let result = parse_flexible_timestamp("2025-06-20", "test").unwrap();
        assert_eq!(result.year(), 2025);
        assert_eq!(result.month(), 6);
        assert_eq!(result.day(), 20);
    }

    #[test]
    fn test_parse_flexible_relative() {
        let result = parse_flexible_timestamp("+1h", "test").unwrap();
        assert!(result > Utc::now());
    }

    #[test]
    fn test_parse_flexible_relative_negative() {
        let result = parse_flexible_timestamp("-1d", "test").unwrap();
        assert!(result < Utc::now());
    }

    #[test]
    fn test_parse_flexible_keywords() {
        let result = parse_flexible_timestamp("tomorrow", "test").unwrap();
        assert!(result > Utc::now());
    }

    #[test]
    fn test_parse_relative_time_positive() {
        let result = parse_relative_time("+1h").unwrap();
        assert!(result > Utc::now());
    }

    #[test]
    fn test_parse_relative_time_negative() {
        let result = parse_relative_time("-7d").unwrap();
        assert!(result < Utc::now());
    }

    #[test]
    fn test_parse_relative_time_invalid() {
        assert!(parse_relative_time("invalid").is_none());
        assert!(parse_relative_time("2025-01-15").is_none());
    }
}
