//! Timestamp parsing for log formats.

use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::parser::SourceTz;

#[derive(Debug, Clone)]
enum Kind {
    Rfc3339,
    Epoch,
    EpochMillis,
    /// chrono strftime; `zoned` when the format carries an offset (`%z`, `%:z`, `%#z`).
    Strftime {
        fmt: String,
        zoned: bool,
    },
}

#[derive(Debug, Clone)]
pub struct TimestampParser {
    kind: Kind,
    assume_current_year: bool,
}

impl TimestampParser {
    pub fn new(format: &str, assume_current_year: bool) -> Self {
        let kind = match format.trim().to_ascii_lowercase().as_str() {
            "rfc3339" | "iso8601" => Kind::Rfc3339,
            "epoch" | "unix" => Kind::Epoch,
            "epoch_ms" | "unix_ms" => Kind::EpochMillis,
            _ => Kind::Strftime {
                fmt: format.to_string(),
                zoned: ["%z", "%:z", "%#z", "%::z", "%:::z"]
                    .iter()
                    .any(|z| format.contains(z)),
            },
        };
        TimestampParser {
            kind,
            assume_current_year,
        }
    }

    /// Parse `value`. `zone` (from `zone_field`) wins over `from_tz` for naive
    /// formats; zoned formats and epochs ignore both.
    pub fn parse(
        &self,
        value: &str,
        zone: Option<&str>,
        from_tz: &SourceTz,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let value = value.trim();
        match &self.kind {
            Kind::Rfc3339 => DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|d| d.with_timezone(&Utc)),
            Kind::Epoch => {
                let secs: f64 = value.parse().ok()?;
                let millis = (secs * 1000.0).round() as i64;
                DateTime::from_timestamp_millis(millis)
            }
            Kind::EpochMillis => {
                let millis: i64 = value.parse().ok()?;
                DateTime::from_timestamp_millis(millis)
            }
            Kind::Strftime { fmt, zoned: true } => {
                DateTime::<FixedOffset>::parse_from_str(value, fmt)
                    .ok()
                    .map(|d| d.with_timezone(&Utc))
            }
            Kind::Strftime { fmt, zoned: false } => {
                let naive = if self.assume_current_year {
                    let year_fmt = format!("%Y {fmt}");
                    let this_year = NaiveDateTime::parse_from_str(
                        &format!("{} {value}", now.year()),
                        &year_fmt,
                    )
                    .ok()?;
                    // A log line cannot be in the future: around New Year, "Dec 31" read on Jan 1 is last year.
                    let candidate = naive_to_utc(this_year, zone, from_tz);
                    if candidate > now + Duration::days(1) {
                        NaiveDateTime::parse_from_str(
                            &format!("{} {value}", now.year() - 1),
                            &year_fmt,
                        )
                        .ok()?
                    } else {
                        this_year
                    }
                } else {
                    NaiveDateTime::parse_from_str(value, fmt).ok()?
                };
                Some(naive_to_utc(naive, zone, from_tz))
            }
        }
    }
}

/// Interpret a naive time in `zone` (IANA name or ±HH:MM offset) if given and
/// valid, otherwise in `from_tz`.
pub fn naive_to_utc(naive: NaiveDateTime, zone: Option<&str>, from_tz: &SourceTz) -> DateTime<Utc> {
    if let Some(z) = zone.map(str::trim).filter(|z| !z.is_empty()) {
        if let Ok(tz) = z.parse::<Tz>() {
            return SourceTz::Named(tz).to_utc(naive);
        }
        if let Some(off) = parse_offset(z)
            && let Some(dt) = off.from_local_datetime(&naive).earliest()
        {
            return dt.with_timezone(&Utc);
        }
        tracing::debug!(
            zone = z,
            "unrecognised zone in log line, using --from-timezone"
        );
    }
    from_tz.to_utc(naive)
}

fn parse_offset(s: &str) -> Option<FixedOffset> {
    if s.eq_ignore_ascii_case("z") || s.eq_ignore_ascii_case("utc") || s.eq_ignore_ascii_case("gmt")
    {
        return FixedOffset::east_opt(0);
    }
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
    let (h, m) = match digits.len() {
        2 => (digits.parse::<i32>().ok()?, 0),
        4 => (
            digits[..2].parse::<i32>().ok()?,
            digits[2..].parse::<i32>().ok()?,
        ),
        _ => return None,
    };
    FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc() -> SourceTz {
        SourceTz::Named(chrono_tz::UTC)
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 27, 12, 0, 0).unwrap()
    }

    #[test]
    fn strftime_naive_uses_from_tz() {
        let p = TimestampParser::new("%Y-%m-%d %H:%M:%S", false);
        let madrid = SourceTz::Named(chrono_tz::Europe::Madrid);
        let d = p
            .parse("2024-07-01 12:00:00", None, &madrid, now())
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2024-07-01T10:00:00+00:00");
    }

    #[test]
    fn zone_field_overrides_from_tz() {
        let p = TimestampParser::new("%d-%b-%Y %H:%M:%S", false);
        let d = p
            .parse("27-Oct-2026 10:00:00", Some("Europe/Madrid"), &utc(), now())
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2026-10-27T09:00:00+00:00");
        let d = p
            .parse("27-Oct-2026 10:00:00", Some("+05:30"), &utc(), now())
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2026-10-27T04:30:00+00:00");
        let d = p
            .parse("27-Oct-2026 10:00:00", Some("Nowhere/Land"), &utc(), now())
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2026-10-27T10:00:00+00:00");
    }

    #[test]
    fn zoned_strftime_ignores_from_tz() {
        let p = TimestampParser::new("%d/%b/%Y:%H:%M:%S %z", false);
        let d = p
            .parse(
                "10/Oct/2000:13:55:36 -0700",
                None,
                &SourceTz::Named(chrono_tz::Asia::Tokyo),
                now(),
            )
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2000-10-10T20:55:36+00:00");
    }

    #[test]
    fn apache_error_format_with_microseconds() {
        let p = TimestampParser::new("%a %b %d %H:%M:%S%.f %Y", false);
        let d = p
            .parse("Mon Jan 01 12:00:00.123456 2024", None, &utc(), now())
            .unwrap();
        assert_eq!(
            d.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2024-01-01T12:00:00.123456Z"
        );
    }

    #[test]
    fn syslog_assumes_current_year_and_rolls_back() {
        let p = TimestampParser::new("%b %d %H:%M:%S", true);
        let d = p.parse("Oct 27 10:00:00", None, &utc(), now()).unwrap();
        assert_eq!(d.to_rfc3339(), "2026-10-27T10:00:00+00:00");
        // Single-digit day as syslog writes it (two spaces).
        let d = p.parse("Oct  7 10:00:00", None, &utc(), now()).unwrap();
        assert_eq!(d.to_rfc3339(), "2026-10-07T10:00:00+00:00");
        // Dec 31 read on Jan 1 belongs to the previous year.
        let jan1 = Utc.with_ymd_and_hms(2027, 1, 1, 0, 30, 0).unwrap();
        let d = p.parse("Dec 31 23:59:00", None, &utc(), jan1).unwrap();
        assert_eq!(d.to_rfc3339(), "2026-12-31T23:59:00+00:00");
    }

    #[test]
    fn rfc3339_and_epochs() {
        let p = TimestampParser::new("rfc3339", false);
        assert_eq!(
            p.parse("2024-01-01T12:00:00+02:00", None, &utc(), now())
                .unwrap()
                .to_rfc3339(),
            "2024-01-01T10:00:00+00:00"
        );
        let p = TimestampParser::new("epoch", false);
        assert_eq!(
            p.parse("1704110400", None, &utc(), now())
                .unwrap()
                .to_rfc3339(),
            "2024-01-01T12:00:00+00:00"
        );
        assert_eq!(
            p.parse("1704110400.5", None, &utc(), now())
                .unwrap()
                .timestamp_millis(),
            1704110400500
        );
        let p = TimestampParser::new("epoch_ms", false);
        assert_eq!(
            p.parse("1704110400500", None, &utc(), now())
                .unwrap()
                .timestamp_millis(),
            1704110400500
        );
    }

    #[test]
    fn garbage_is_none() {
        let p = TimestampParser::new("%Y-%m-%d %H:%M:%S", false);
        assert!(
            p.parse("2016-13-99 17:25:45", None, &utc(), now())
                .is_none()
        );
        assert!(p.parse("", None, &utc(), now()).is_none());
    }
}
