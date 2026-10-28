# Format fixtures

One sample file per built-in format in `log_formats/`, used by the unit tests in
`src/format/mod.rs` and the end-to-end tests in `tests/cli.rs`. Tests address
lines by index, so append new lines rather than inserting them, and keep every
line unique (the pipeline drops consecutive duplicate entries).

## Provenance

- `laravel.log` comes from laralog's test suite.
- `nginx_access.log` lines 3–10 are taken verbatim from
  [`nginx_logs/nginx_logs`](https://github.com/elastic/examples/tree/master/Common%20Data%20Formats/nginx_logs)
  in the Elastic examples repository (Apache License 2.0).
- `apache_access.log` lines 3–10 are taken verbatim from
  [`apache_logs/apache_logs`](https://github.com/elastic/examples/tree/master/Common%20Data%20Formats/apache_logs)
  in the same repository.
- Everything else is hand-written to cover cases the Elastic samples lack:
  - empty or binary request lines
  - non-UTC offsets
  - escaped quotes
  - hostname clients
  - error-log levels
  - continuation lines

The Elastic repository has no error logs, so `nginx_error.log` and `apache_error.log` are entirely hand-written.
