use std::fmt;
use std::str::FromStr;

use opentelemetry::logs::Severity;
use serde::{Deserialize, Serialize};

/// Laravel / PSR-3 log levels, highest severity first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Level {
    Emergency,
    Alert,
    Critical,
    Error,
    Warning,
    Notice,
    Info,
    Debug,
}

impl Level {
    pub const ALL: [Level; 8] = [
        Level::Emergency,
        Level::Alert,
        Level::Critical,
        Level::Error,
        Level::Warning,
        Level::Notice,
        Level::Info,
        Level::Debug,
    ];

    /// Upper-case name as Laravel writes it in the log line.
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Emergency => "EMERGENCY",
            Level::Alert => "ALERT",
            Level::Critical => "CRITICAL",
            Level::Error => "ERROR",
            Level::Warning => "WARNING",
            Level::Notice => "NOTICE",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }

    /// OpenTelemetry severity number per the logs data model.
    pub fn severity(self) -> Severity {
        match self {
            Level::Emergency => Severity::Fatal4,
            Level::Alert => Severity::Fatal3,
            Level::Critical => Severity::Fatal,
            Level::Error => Severity::Error,
            Level::Warning => Severity::Warn,
            Level::Notice => Severity::Info2,
            Level::Info => Severity::Info,
            Level::Debug => Severity::Debug,
        }
    }

    /// Datadog `status` value.
    pub fn datadog_status(self) -> &'static str {
        match self {
            Level::Emergency => "emergency",
            Level::Alert => "alert",
            Level::Critical => "critical",
            Level::Error => "error",
            Level::Warning => "warning",
            Level::Notice => "notice",
            Level::Info => "info",
            Level::Debug => "debug",
        }
    }
}

impl FromStr for Level {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        Level::ALL
            .iter()
            .copied()
            .find(|l| l.as_str().eq_ignore_ascii_case(s))
            .ok_or_else(|| format!("unknown log level '{s}'"))
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A level as found in the log: either a known PSR-3 level or an arbitrary
/// string Laravel channels may emit (custom monolog levels).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LogLevel {
    Known(Level),
    Unknown(String),
}

impl LogLevel {
    pub fn parse(s: &str) -> Self {
        match Level::from_str(s) {
            Ok(l) => LogLevel::Known(l),
            Err(_) => LogLevel::Unknown(s.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            LogLevel::Known(l) => l.as_str(),
            LogLevel::Unknown(s) => s.as_str(),
        }
    }

    pub fn severity(&self) -> Severity {
        match self {
            LogLevel::Known(l) => l.severity(),
            LogLevel::Unknown(_) => Severity::Info,
        }
    }

    pub fn datadog_status(&self) -> String {
        match self {
            LogLevel::Known(l) => l.datadog_status().to_string(),
            LogLevel::Unknown(s) => s.to_ascii_lowercase(),
        }
    }

    /// Case-insensitive comparison against a user-supplied level name.
    pub fn matches(&self, name: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(name.trim())
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_any_case() {
        assert_eq!("error".parse::<Level>().unwrap(), Level::Error);
        assert_eq!("Warning".parse::<Level>().unwrap(), Level::Warning);
        assert_eq!(" EMERGENCY ".parse::<Level>().unwrap(), Level::Emergency);
        assert!("verbose".parse::<Level>().is_err());
    }

    #[test]
    fn severity_table() {
        let expected = [
            (Level::Emergency, Severity::Fatal4),
            (Level::Alert, Severity::Fatal3),
            (Level::Critical, Severity::Fatal),
            (Level::Error, Severity::Error),
            (Level::Warning, Severity::Warn),
            (Level::Notice, Severity::Info2),
            (Level::Info, Severity::Info),
            (Level::Debug, Severity::Debug),
        ];
        for (level, sev) in expected {
            assert_eq!(level.severity(), sev, "{level}");
        }
    }

    #[test]
    fn unknown_levels_are_preserved() {
        let l = LogLevel::parse("VERBOSE");
        assert_eq!(l, LogLevel::Unknown("VERBOSE".into()));
        assert_eq!(l.severity(), Severity::Info);
        assert!(l.matches("verbose"));
        assert_eq!(l.datadog_status(), "verbose");
    }
}
