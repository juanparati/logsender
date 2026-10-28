use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser as _, ValueEnum as _};
use tracing_subscriber::EnvFilter;

use logsender::cli::{Cli, SenderKind};
use logsender::folder::Folder;
use logsender::format::LogFormat;
use logsender::parser::{Parser, SourceTz};
use logsender::pipeline::{Outcome, Pipeline, PipelineConfig};
use logsender::reader::{FileTail, LineSource, StdinReader, file::DEFAULT_MAX_LINE};
use logsender::sender::{
    Sender,
    datadog::DatadogSender,
    elastic::ElasticSender,
    otlp::{OtlpConfig, OtlpSender},
    parse_header,
    post::PostSender,
    resolve_hosts,
    stdout::StdoutSender,
};

const EXIT_CONFIG: u8 = 2;

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    if std::env::var_os("NO_COLOR").is_some() {
        colored::control::set_override(false);
    }
    logsender::sender::ensure_crypto_provider();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: cannot start runtime: {e}");
            return ExitCode::from(1);
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(code) => code,
        Err(e) => {
            let is_config = e
                .downcast_ref::<logsender::Error>()
                .is_some_and(|le| matches!(le, logsender::Error::Config(_)));
            eprintln!("{} {e:#}", colored::Colorize::red("error:"));
            ExitCode::from(if is_config { EXIT_CONFIG } else { 1 })
        }
    }
}

fn init_tracing(verbose: u8) {
    let default = match verbose {
        0 | 1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("logsender={default},warn")));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_ansi(std::env::var_os("NO_COLOR").is_none())
        .init();
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let hosts = match (cli.sender, cli.to.as_deref()) {
        (SenderKind::Stdout, to) => {
            if to.is_some() {
                tracing::warn!("--to is ignored for the stdout sender");
            }
            Vec::new()
        }
        (sender, None) => {
            let name = sender.to_possible_value().expect("no skipped variants");
            return Err(logsender::Error::Config(format!(
                "--to is required for the {} sender",
                name.get_name()
            ))
            .into());
        }
        (_, Some(to)) => {
            let hosts = resolve_hosts(to).with_context(|| format!("reading hosts file {to}"))?;
            if hosts.is_empty() {
                return Err(logsender::Error::Config(format!("hosts file {to} is empty")).into());
            }
            hosts
        }
    };

    let hostname = match cli.hostname.clone() {
        Some(h) => h,
        None => hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "localhost".into()),
    };
    let from_tz =
        SourceTz::resolve(cli.from_timezone.as_deref()).map_err(logsender::Error::Config)?;
    let to_tz = match cli
        .to_timezone
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(name) => Some(
            name.parse::<chrono_tz::Tz>()
                .map_err(|_| logsender::Error::Config(format!("unknown --to-timezone '{name}'")))?,
        ),
        None => None,
    };
    let timeout = Duration::from_secs(cli.request_timeout.max(1));
    let format = std::sync::Arc::new(LogFormat::resolve(&cli.log_format)?);
    if cli.smart && !format.json_context {
        tracing::warn!(format = %format.name, "--smart has no effect: this format has no json_context");
    }

    let headers = cli
        .headers
        .iter()
        .map(|h| h.trim())
        .filter(|h| !h.is_empty())
        .map(parse_header)
        .collect::<Result<Vec<_>, _>>()?;
    if !headers.is_empty() && cli.sender != SenderKind::Post {
        tracing::warn!("--header is only used by the post sender");
    }

    let sender: Box<dyn Sender> = match cli.sender {
        SenderKind::Stdout => Box::new(StdoutSender::new(&cli.index)),
        SenderKind::Post => {
            if hosts.len() > 1 {
                tracing::warn!("only the first post URL is used");
            }
            Box::new(PostSender::new(
                &hosts[0],
                &headers,
                cli.api_key.clone(),
                !cli.no_check_cert,
                timeout,
            )?)
        }
        SenderKind::Datadog => {
            let key = cli
                .api_key
                .clone()
                .or_else(|| {
                    std::env::var("DD_API_KEY")
                        .ok()
                        .filter(|k| !k.trim().is_empty())
                })
                .ok_or_else(|| {
                    logsender::Error::Config(
                        "the Datadog sender needs --api-key or DD_API_KEY".into(),
                    )
                })?;
            if cli.no_check_cert {
                tracing::warn!("--no-check-cert is ignored for the Datadog sender");
            }
            if hosts.len() > 1 {
                tracing::warn!("only the first Datadog host is used");
            }
            Box::new(DatadogSender::new(&hosts[0], key, &cli.index, timeout)?)
        }
        SenderKind::ElasticSearch => Box::new(ElasticSender::new(
            &hosts,
            &cli.index,
            cli.api_key.clone(),
            to_tz,
            !cli.no_check_cert,
            timeout,
        )?),
        SenderKind::Otlp => {
            if hosts.len() > 1 {
                tracing::warn!("only the first OTLP endpoint is used");
            }
            Box::new(OtlpSender::new(OtlpConfig {
                endpoint: &hosts[0],
                protocol: cli.otlp_protocol,
                service_name: &cli.index,
                hostname: &hostname,
                check_cert: !cli.no_check_cert,
                timeout,
                smart: cli.smart,
            })?)
        }
    };
    if cli.to_timezone.is_some() && cli.sender != SenderKind::ElasticSearch {
        tracing::warn!(
            "--to-timezone only applies to the elastic_search sender; timestamps are sent in UTC"
        );
    }

    let source: Box<dyn LineSource> = match &cli.input {
        Some(path) => {
            tracing::info!(path = %path.display(), from_start = cli.from_start, "tailing file");
            Box::new(FileTail::new(path, !cli.from_start, DEFAULT_MAX_LINE))
        }
        None => Box::new(StdinReader::new(DEFAULT_MAX_LINE)),
    };

    let folder = Folder::new(
        Parser::new(format.clone(), from_tz, cli.smart, hostname.clone())
            .with_environment(cli.environment.clone()),
    );
    let cfg = PipelineConfig {
        read_freq: Duration::from_millis(cli.read_freq.max(1)),
        batch_size: cli.batch_size.max(1),
        flush_interval: Duration::from_millis(cli.flush_interval.max(1)),
        retries: cli.retries,
        shutdown_timeout: Duration::from_secs(cli.shutdown_timeout),
        echo_entries: cli.verbose >= 1,
        ignore_levels: cli
            .ignore
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        stats_interval: (cli.verbose >= 1).then_some(Duration::from_secs(60)),
    };
    tracing::info!(
        sender = ?cli.sender,
        host = hosts.first().map(|h| logsender::sender::redact_url(h)),
        index = %cli.index,
        log_format = %format.name,
        format_source = %format.source,
        from_timezone = %from_tz.name(),
        hostname = %hostname,
        "starting logsender {}",
        env!("CARGO_PKG_VERSION")
    );

    let pipeline = Pipeline::new(cfg, cli.max_queue);
    let signal_kind = spawn_signal_handler(&pipeline);

    let (outcome, stats) = pipeline.run(source, folder, sender).await;

    if stats.dropped() > 0 {
        eprintln!(
            "{} {} entries were dropped (queue full: {}, delivery failed: {})",
            colored::Colorize::yellow("warning:"),
            stats.dropped(),
            stats.dropped_queue,
            stats.dropped_send
        );
    }
    let interrupted = signal_kind.load(std::sync::atomic::Ordering::SeqCst) == SIG_INT;
    Ok(ExitCode::from(exit_code(
        outcome,
        stats.dropped(),
        interrupted,
    )))
}

/// 130 when stopped by SIGINT; otherwise 1 if the reader failed or any entry
/// was lost (queue overflow, delivery failure, rejection, shutdown timeout); else 0.
fn exit_code(outcome: Outcome, dropped: u64, interrupted: bool) -> u8 {
    if interrupted {
        return 130;
    }
    match outcome {
        Outcome::ReaderFailed | Outcome::StoppedWithLoss(_) => 1,
        Outcome::Eof | Outcome::Stopped if dropped > 0 => 1,
        Outcome::Eof | Outcome::Stopped => 0,
    }
}

const SIG_NONE: u8 = 0;
const SIG_INT: u8 = 1;
const SIG_TERM: u8 = 2;

fn spawn_signal_handler(pipeline: &Pipeline) -> std::sync::Arc<std::sync::atomic::AtomicU8> {
    use tokio::signal::unix::{SignalKind, signal};
    let kind = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(SIG_NONE));
    let stop = pipeline.stop.clone();
    let kind2 = kind.clone();
    tokio::spawn(async move {
        let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut hup = signal(SignalKind::hangup()).expect("SIGHUP handler");
        let which = tokio::select! {
            _ = int.recv() => SIG_INT,
            _ = term.recv() => SIG_TERM,
            _ = hup.recv() => SIG_TERM,
        };
        kind2.store(which, std::sync::atomic::Ordering::SeqCst);
        eprintln!("Exiting...");
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        // A second signal exits immediately.
        tokio::select! {
            _ = int.recv() => {}
            _ = term.recv() => {}
            _ = hup.recv() => {}
        }
        std::process::exit(130);
    });
    kind
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes() {
        assert_eq!(exit_code(Outcome::Eof, 0, false), 0);
        assert_eq!(exit_code(Outcome::Stopped, 0, false), 0);
        assert_eq!(exit_code(Outcome::Eof, 3, false), 1);
        assert_eq!(exit_code(Outcome::Stopped, 1, false), 1);
        assert_eq!(exit_code(Outcome::StoppedWithLoss(2), 2, false), 1);
        assert_eq!(exit_code(Outcome::ReaderFailed, 0, false), 1);
        assert_eq!(exit_code(Outcome::Eof, 5, true), 130);
    }
}
