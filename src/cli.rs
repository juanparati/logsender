use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// Tail Laravel logs and ship them to OpenTelemetry (OTLP), Datadog or Elasticsearch.
///
/// Successor of laralog. Every option can also be set through an
/// environment variable named LOGSENDER_<OPTION> (for example LOGSENDER_INDEX).
#[derive(Parser, Debug, Clone)]
#[command(name = "logsender", version, about, long_about = None, before_help = BANNER, after_help = AFTER_HELP)]
pub struct Cli {
    /// Log format: path to a YAML format file, or a built-in name
    #[arg(value_name = "LOG_FORMAT", env = "LOGSENDER_LOG_FORMAT")]
    pub log_format: String,

    /// Destination endpoint, or a file with one endpoint per line. Required by every sender except stdout.
    ///
    /// otlp: http://localhost:4317 (grpc) or http://localhost:4318 (http).
    /// datadog: https://http-intake.logs.datadoghq.eu (the API key goes in --api-key).
    /// elastic_search: https://user:pass@es.example.com:9200
    #[arg(
        long,
        value_name = "HOST",
        env = "LOGSENDER_TO",
        hide_env_values = true
    )]
    pub to: Option<String>,

    /// Log file to tail. Reads stdin when omitted.
    #[arg(short, long, value_name = "PATH", env = "LOGSENDER_INPUT")]
    pub input: Option<PathBuf>,

    /// Index (Elasticsearch) or service name (Datadog, OTLP).
    #[arg(long, default_value = "laravel", env = "LOGSENDER_INDEX")]
    pub index: String,

    /// Comma-separated log levels to drop, e.g. DEBUG,INFO.
    #[arg(
        long,
        value_delimiter = ',',
        value_name = "LEVELS",
        env = "LOGSENDER_IGNORE"
    )]
    pub ignore: Vec<String>,

    /// Entries per request [default: 500 for datadog, 10 otherwise]. Clamped to the back-end limit (Datadog: 1000).
    #[arg(long, value_name = "N", env = "LOGSENDER_BATCH_SIZE")]
    pub batch_size: Option<usize>,

    /// Skip TLS certificate verification (elastic_search and otlp over http only).
    #[arg(long, env = "LOGSENDER_NO_CHECK_CERT")]
    pub no_check_cert: bool,

    /// Retries per batch before it is dropped.
    #[arg(long, default_value_t = 2, value_name = "N", env = "LOGSENDER_RETRIES")]
    pub retries: u32,

    /// File poll interval in milliseconds.
    #[arg(
        long,
        default_value_t = 500,
        value_name = "MS",
        env = "LOGSENDER_READ_FREQ"
    )]
    pub read_freq: u64,

    /// Timezone the log file was written in (IANA name). Defaults to TZ, then the system zone.
    #[arg(long, value_name = "ZONE", env = "LOGSENDER_FROM_TIMEZONE")]
    pub from_timezone: Option<String>,

    /// Timezone for the formatted timestamp (elastic_search only; others use UTC).
    #[arg(long, value_name = "ZONE", env = "LOGSENDER_TO_TIMEZONE")]
    pub to_timezone: Option<String>,

    /// Delivery back-end.
    #[arg(long, value_enum, default_value_t = SenderKind::Stdout, env = "LOGSENDER_SENDER")]
    pub sender: SenderKind,

    /// Hostname attached to every entry. Defaults to the machine's hostname.
    #[arg(long, env = "LOGSENDER_HOSTNAME")]
    pub hostname: Option<String>,

    /// Parse JSON context into a `params` field (or OTLP attributes). Formats with `json_context: true` only.
    #[arg(long, env = "LOGSENDER_SMART")]
    pub smart: bool,

    /// Environment name for formats that do not capture one (e.g. nginx, syslog).
    #[arg(long, value_name = "NAME", env = "LOGSENDER_ENVIRONMENT")]
    pub environment: Option<String>,

    /// Print each entry as JSON to stdout. Repeat (-vv, -vvv) for more internal diagnostics.
    #[arg(short, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// API key: Datadog (also DD_API_KEY) or Elasticsearch `ApiKey` authorization.
    #[arg(
        long,
        value_name = "KEY",
        env = "LOGSENDER_API_KEY",
        hide_env_values = true
    )]
    pub api_key: Option<String>,

    /// Extra request header for the post sender, as "Name: value". Repeatable; in the
    /// environment variable separate several headers with ";".
    #[arg(
        short = 'H',
        long = "header",
        value_name = "NAME: VALUE",
        env = "LOGSENDER_HEADER",
        hide_env_values = true,
        value_delimiter = ';'
    )]
    pub headers: Vec<String>,

    /// OTLP transport.
    #[arg(long, value_enum, default_value_t = OtlpProtocol::Grpc, env = "LOGSENDER_OTLP_PROTOCOL")]
    pub otlp_protocol: OtlpProtocol,

    /// Read the input file from the beginning instead of the end.
    #[arg(long, env = "LOGSENDER_FROM_START")]
    pub from_start: bool,

    /// Entries held in memory between the reader and the sender. See --on-full.
    #[arg(
        long,
        default_value_t = 10_000,
        value_name = "N",
        env = "LOGSENDER_MAX_QUEUE"
    )]
    pub max_queue: usize,

    /// What to do when the queue is full.
    #[arg(long, value_enum, default_value_t = OnFull::Block, value_name = "POLICY", env = "LOGSENDER_ON_FULL")]
    pub on_full: OnFull,

    /// Longest time a partial batch waits before being sent, in milliseconds.
    #[arg(
        long,
        default_value_t = 1000,
        value_name = "MS",
        env = "LOGSENDER_FLUSH_INTERVAL"
    )]
    pub flush_interval: u64,

    /// Seconds to wait for queued entries to drain on shutdown.
    #[arg(
        long,
        default_value_t = 5,
        value_name = "S",
        env = "LOGSENDER_SHUTDOWN_TIMEOUT"
    )]
    pub shutdown_timeout: u64,

    /// Request timeout per batch, in seconds.
    #[arg(
        long,
        default_value_t = 10,
        value_name = "S",
        env = "LOGSENDER_REQUEST_TIMEOUT"
    )]
    pub request_timeout: u64,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderKind {
    /// OpenTelemetry OTLP exporter (Collector or Datadog Agent).
    Otlp,
    /// Datadog Logs API v2.
    Datadog,
    /// Elasticsearch 8+ Bulk API.
    #[value(name = "elastic_search", alias = "elasticsearch", alias = "es")]
    ElasticSearch,
    /// Print to stdout.
    Stdout,
    /// Generic HTTP POST of a JSON array per batch (webhooks, custom endpoints).
    Post,
}

impl SenderKind {
    /// Batch size used when --batch-size is not given.
    pub fn default_batch_size(self) -> usize {
        match self {
            SenderKind::Datadog => 500,
            _ => 10,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnFull {
    /// Stop reading until the sender catches up. Nothing is lost while reading
    /// a file; a slow back-end slows down whatever writes into stdin.
    Block,
    /// Drop the oldest queued entry. Never stalls the input; loses entries.
    Drop,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    /// gRPC (default port 4317).
    Grpc,
    /// HTTP with protobuf payload (default port 4318).
    Http,
}

const BANNER: &str = r"    __                _____                __
   / /   ____  ____ _/ ___/___  ____  ____/ /__  _____
  / /   / __ \/ __ `/\__ \/ _ \/ __ \/ __  / _ \/ ___/
 / /___/ /_/ / /_/ /___/ /  __/ / / / /_/ /  __/ /
/_____/\____/\__, //____/\___/_/ /_/\__,_/\___/_/
            /____/";

const AFTER_HELP: &str = "\
Examples:
  logsender laravel --to=http://localhost:4317 --sender=otlp --input=storage/logs/laravel.log --smart
  logsender laravel --to=https://http-intake.logs.datadoghq.eu --sender=datadog --api-key=$DD_API_KEY --index=myapp
  logsender laravel --to=https://es.lan:9200 --sender=es --input=laravel.log --index=laravel --to-timezone=Europe/Madrid
  logsender laravel -v < laravel.log
  logsender laravel --to=https://hooks.example.com/logs --sender=post -H 'X-Token: abc' --input=laravel.log
  logsender nginx_error --to=http://localhost:4317 --sender=otlp --input=/var/log/nginx/error.log
  logsender ./log_formats/my_app.yaml --to=http://localhost:4317 --sender=otlp --input=app.log";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = Cli::try_parse_from(["logsender", "laravel"]).unwrap();
        assert_eq!(c.index, "laravel");
        assert_eq!(c.batch_size, None);
        assert_eq!(c.on_full, OnFull::Block);
        assert_eq!(c.retries, 2);
        assert_eq!(c.read_freq, 500);
        assert_eq!(c.sender, SenderKind::Stdout);
        assert!(c.input.is_none());
        assert_eq!(c.log_format, "laravel");
        assert!(c.to.is_none());
    }

    #[test]
    fn headers_are_repeatable_and_env_split_on_semicolon() {
        let c = Cli::try_parse_from(["logsender", "laravel", "-H", "A: 1", "--header", "B: 2"])
            .unwrap();
        assert_eq!(c.headers, vec!["A: 1", "B: 2"]);
        let c = Cli::try_parse_from(["logsender", "laravel", "-H", "A: 1;B: 2"]).unwrap();
        assert_eq!(c.headers, vec!["A: 1", "B: 2"]);
    }

    #[test]
    fn log_format_is_required() {
        assert!(Cli::try_parse_from(["logsender"]).is_err());
    }

    #[test]
    fn sender_aliases() {
        for name in ["elastic_search", "elasticsearch", "es"] {
            let c = Cli::try_parse_from(["logsender", "laravel", "--sender", name]).unwrap();
            assert_eq!(c.sender, SenderKind::ElasticSearch);
        }
    }

    #[test]
    fn ignore_is_comma_separated() {
        let c = Cli::try_parse_from(["logsender", "laravel", "--ignore=DEBUG,INFO"]).unwrap();
        assert_eq!(c.ignore, vec!["DEBUG", "INFO"]);
    }
}
