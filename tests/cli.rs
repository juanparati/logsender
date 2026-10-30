use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;

fn bin() -> Command {
    let mut c = Command::cargo_bin("logsender").unwrap();
    c.env_remove("DD_API_KEY").env_remove("TZ");
    c
}

const FIXTURE: &str = include_str!("fixtures/formats/laravel.log");

#[test]
fn help_lists_laralog_flags() {
    bin()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"))
        .stdout(predicate::str::contains("<LOG_FORMAT>"))
        .stdout(predicate::str::contains("<HOST>"))
        .stdout(predicate::str::contains("--input"))
        .stdout(predicate::str::contains("--sender"))
        .stdout(predicate::str::contains("--from-timezone"))
        .stdout(predicate::str::contains("--smart"))
        .stdout(predicate::str::contains("--api-key"));
}

#[test]
fn version_flag() {
    bin()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("logsender "));
}

#[test]
fn missing_log_format_is_usage_error() {
    bin().assert().failure().code(2);
}

#[test]
fn to_is_required_except_for_stdout() {
    bin()
        .args(["laravel", "--sender=otlp"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "--to is required for the otlp sender",
        ));
    bin()
        .args(["laravel", "--sender=es"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "--to is required for the elastic_search sender",
        ));
    bin()
        .args(["laravel", "--to=http://x", "--from-timezone=UTC"])
        .write_stdin("[2019-11-24 06:30:56] production.INFO: hi\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("hi"));
}

#[test]
fn stdin_to_stdout_emits_every_entry() {
    let out = bin()
        .args([
            "laravel",
            "--sender=stdout",
            "--from-timezone=UTC",
            "--hostname=test",
        ])
        .write_stdin(FIXTURE)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let out = String::from_utf8(out).unwrap();
    let headers = FIXTURE.lines().filter(|l| l.starts_with('[')).count();
    let docs: Vec<serde_json::Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
        .collect();
    assert_eq!(docs.len(), headers);
    assert!(docs.iter().all(|d| d["index"] == "logsender"), "{out}");
    assert!(!out.lines().any(|l| l.starts_with("=====")), "{out}");
    assert!(out.contains(r#""message":"Hello logstash""#));
    assert!(out.contains(r#""stacktrace":"Stack trace:\n#0"#));
    assert!(out.contains(r#""hostname":"test""#));
}

#[test]
fn ignore_levels_filters_entries() {
    let out = bin()
        .args([
            "laravel",
            "--sender=stdout",
            "--ignore=info,error",
            "--from-timezone=UTC",
        ])
        .write_stdin(FIXTURE)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let out = String::from_utf8(out).unwrap();
    assert!(!out.contains(r#""level":"INFO""#));
    assert!(!out.contains(r#""level":"ERROR""#));
    assert!(out.contains(r#""level":"DEBUG""#));
    let kept = out.lines().count();
    let expected = FIXTURE
        .lines()
        .filter(|l| l.starts_with('[') && !l.contains(".INFO:") && !l.contains(".ERROR:"))
        .count();
    assert_eq!(kept, expected);
}

#[test]
fn smart_adds_params() {
    bin()
        .args([
            "laravel",
            "--sender=stdout",
            "--smart",
            "--from-timezone=UTC",
        ])
        .write_stdin(
            "[2019-11-24 06:30:56] production.INFO: Job finished {\"queue\":\"default\"}\n",
        )
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""params":{"queue":"default"}"#));
}

#[test]
fn verbose_echoes_entries_as_json_lines() {
    bin()
        .args(["laravel", "--sender=stdout", "-v", "--from-timezone=UTC"])
        .write_stdin("[2019-11-24 06:30:56] production.INFO: hi\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            r#"{"timestamp":"2019-11-24T06:30:56Z""#,
        ));
}

#[test]
fn datadog_v1_url_is_rejected_with_exit_2() {
    bin()
        .args([
            "laravel",
            "--to=https://http-intake.logs.datadoghq.eu/v1/input/abc123",
            "--sender=datadog",
            "--api-key=k",
        ])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--api-key"));
}

#[test]
fn datadog_without_key_is_exit_2() {
    bin()
        .args([
            "laravel",
            "--to=https://http-intake.logs.datadoghq.eu",
            "--sender=datadog",
        ])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("DD_API_KEY"));
}

#[test]
fn bad_timezone_is_exit_2() {
    bin()
        .args(["laravel", "--sender=stdout", "--from-timezone=Mars/Olympus"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("timezone"));
}

#[test]
fn env_vars_configure_options() {
    bin()
        .env("LOGSENDER_SENDER", "stdout")
        .env("LOGSENDER_INDEX", "from-env")
        .env("LOGSENDER_FROM_TIMEZONE", "UTC")
        .arg("laravel")
        .write_stdin("[2019-11-24 06:30:56] production.INFO: hi\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""index":"from-env""#));
}

#[test]
fn tails_file_from_start_and_stops_on_sigterm() {
    use std::process::{Command as StdCommand, Stdio};

    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("laravel.log");
    std::fs::write(&log, "[2019-11-24 06:30:56] production.INFO: first\n").unwrap();

    let exe = assert_cmd::cargo::cargo_bin("logsender");
    let child = StdCommand::new(exe)
        .args([
            "laravel",
            "--sender=stdout",
            "--from-start",
            "--read-freq=50",
            "--flush-interval=50",
            "--from-timezone=UTC",
            "--input",
        ])
        .arg(&log)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(400));
    let mut f = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
    writeln!(f, "[2019-11-24 06:30:57] production.ERROR: second").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(400));

    unsafe {
        libc_kill(child.id() as i32, 15);
    }
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("\"message\":\"first\""), "{stdout}");
    assert!(stdout.contains("\"message\":\"second\""), "{stdout}");
    assert!(out.status.success(), "{:?}", out.status);
}

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

#[test]
fn builtin_format_by_name_emits_fields() {
    bin()
        .args(["nginx_error", "--sender=stdout", "--from-timezone=UTC"])
        .write_stdin("2026/10/27 10:15:03 [notice] 1#1: signal process started\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""level":"NOTICE""#))
        .stdout(predicate::str::contains(
            r#""fields":{"process.pid":"1","process.thread.id":"1"}"#,
        ))
        .stdout(predicate::str::contains(r#""timestamp":1793096103000"#));
}

#[test]
fn web_server_fixtures_round_trip() {
    // (format, entries after folding continuations, expected output snippets)
    let cases: [(&str, usize, &[&str]); 4] = [
        (
            "nginx_access",
            13,
            &[r#""client.address":"2a01:7e00::f03c:91ff:fe70:a4cc""#],
        ),
        (
            "apache_access",
            12,
            &[r#""client.address":"crawler-7.example.net""#],
        ),
        (
            "nginx_error",
            11,
            &[r#""stacktrace":"Stack trace:\n#0 {main}\" while reading"#],
        ),
        (
            "apache_error",
            12,
            &[
                r#""stacktrace":"  PHP Stack trace:\n  PHP   1. {main}() /var/www/html/app.php:0""#,
                r#""message":"PHP Fatal error:  Uncaught Error: Call to undefined function foo()"#,
            ],
        ),
    ];
    for (name, entries, snippets) in cases {
        let path = format!(
            "{}/tests/fixtures/formats/{name}.log",
            env!("CARGO_MANIFEST_DIR")
        );
        let out = bin()
            .args([name, "--sender=stdout", "--from-timezone=UTC"])
            .write_stdin(std::fs::read_to_string(path).unwrap())
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let out = String::from_utf8(out).unwrap();
        assert_eq!(out.lines().count(), entries, "{name}");
        for snippet in snippets {
            assert!(out.contains(snippet), "{name}: missing {snippet}\n{out}");
        }
    }
}

#[test]
fn format_file_by_path() {
    let dir = tempfile::tempdir().unwrap();
    let custom = dir.path().join("kv.yaml");
    std::fs::write(
        &custom,
        "match: ['^%{WORD:level} %{GREEDYDATA:message}$']\ntimestamp: {field: null}\nmultiline: {mode: none}\n",
    )
    .unwrap();
    bin()
        .arg(&custom)
        .args(["--sender=stdout"])
        .write_stdin("error something broke\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            r#""level":"ERROR","message":"something broke""#,
        ));
}

#[test]
fn unknown_format_is_exit_2() {
    bin()
        .args(["nope", "--sender=stdout"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("log format 'nope' not found"))
        .stderr(predicate::str::contains("laravel"));
}

#[test]
fn invalid_format_file_is_exit_2() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, "match: ['%{NOPE:message}']\n").unwrap();
    bin()
        .arg(&bad)
        .args(["--sender=stdout"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("match[0]"));
}

#[test]
fn environment_flag_fills_formats_without_one() {
    bin()
        .args(["syslog", "--sender=stdout", "--environment=staging"])
        .write_stdin("Oct 27 10:00:01 web01 cron[1]: job done\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""environment":"staging""#));
}

#[tokio::test]
async fn post_sender_sends_json_array_with_headers() {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hooks/logs"))
        .and(header("X-Token", "abc"))
        .and(header("Authorization", "Bearer k1"))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;

    let url = format!("{}/hooks/logs", server.uri());
    let exe = assert_cmd::cargo::cargo_bin("logsender");
    // Run the binary off the async runtime so the mock server keeps serving.
    let output = tokio::task::spawn_blocking(move || {
        let mut c = std::process::Command::new(exe);
        c.env_remove("DD_API_KEY")
            .args(["laravel", "--to", &url, "--sender=post", "-H", "X-Token: abc", "--api-key=k1", "--from-timezone=UTC", "--batch-size=10"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = c.spawn().unwrap();
        {
            use std::io::Write;
            let stdin = child.stdin.as_mut().unwrap();
            stdin
                .write_all(b"[2026-10-27 10:00:00] prod.ERROR: boom\n[2026-10-27 10:00:01] prod.INFO: fine\n")
                .unwrap();
        }
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["message"], "boom");
    assert_eq!(arr[0]["level"], "ERROR");
    assert_eq!(arr[1]["message"], "fine");
}

#[test]
fn bad_header_is_exit_2() {
    bin()
        .args([
            "laravel",
            "--to=http://127.0.0.1:9",
            "--sender=post",
            "-H",
            "no-colon-here",
        ])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid --header"));
}

#[test]
fn help_does_not_print_secret_env_values() {
    bin()
        .env("LOGSENDER_TO", "https://elastic:pw@es:9200")
        .env("LOGSENDER_HEADER", "X-Token: s3cret")
        .env("LOGSENDER_API_KEY", "k3y")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("elastic:pw").not())
        .stdout(predicate::str::contains("s3cret").not())
        .stdout(predicate::str::contains("k3y").not());
}

#[test]
fn startup_log_redacts_url_credentials() {
    bin()
        .env("NO_COLOR", "1")
        .args([
            "laravel",
            "--to=https://elastic:pw@127.0.0.1:9/?token=abc",
            "--sender=es",
            "-vv",
            "--retries=0",
        ])
        .write_stdin("")
        .assert()
        .stderr(predicate::str::contains("elastic:***@127.0.0.1:9"))
        .stderr(predicate::str::contains("token=***"))
        .stderr(predicate::str::contains(":pw@").not())
        .stderr(predicate::str::contains("token=abc").not());
}

#[test]
fn lost_entries_exit_1() {
    bin()
        .args([
            "laravel",
            "--to=http://127.0.0.1:9",
            "--sender=post",
            "--retries=0",
            "--request-timeout=2",
        ])
        .write_stdin("[2026-10-27 10:00:00] p.ERROR: x\n")
        .assert()
        .code(1)
        .stderr(predicate::str::contains("1 entries were dropped"));
}
