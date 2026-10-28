```
     __                _____                __
    / /   ____  ____ _/ ___/___  ____  ____/ /__  _____
   / /   / __ \/ __ `/\__ \/ _ \/ __ \/ __  / _ \/ ___/
  / /___/ /_/ / /_/ /___/ /  __/ / / / /_/ /  __/ /
 /_____/\____/\__, //____/\___/_/ /_/\__,_/\___/_/
             /____/
```

# LogSender 

A log parser and forwarder, based on [Laralog](https://github.com/juanparati/laralog).


## Motivation

Many log forwarder agents are slow and chunky; they may require the installation of a runtime (Java, PHP, Ruby...) or having a lot of dependencies.

LogSender is a small, fast single static binary that makes it ideal for small container deployments and simple server setups.


## How it works

LogSender tails a log file (or reads stdin), parses each entry, and ships it to a destination.


### Supported destinations

- OpenTelemetry (Sender: **otlp**): OpenTelemetry compatible sender.
- Datadog (Sender: **datadog**): Datadog Logs API v2 direct sender (Without OTLP).
- Elasticsearch (Sender: **es**): Elasticsearch 8+ Bulk API sender.
- POST request (Sender: **post**): Any line-oriented log format sender.
- Stdout (Sender: **stdout**): It's the default sender. Prints each entry as one JSON line (JSON Lines) including the `--index` value as the `index` field, so the output can be piped into `jq` or saved as-is (Useful for data transformation and debugging).

### Supported log formats

It may be possible to define your log format into a YAML file. See the current [log format definitions](log_formats).

See [Log formats](#custom-log-formats) if you want to implement your own custom log format.

The following log formats are built-in into LogSender:

- **Laravel** (Format: laravel): Laravel log format.
- **Apache access** (Format: apache_access): Apache access log format.
- **Apache error** (Format: apache_error): Apache error log format.
- **Nginx access** (Format: nginx_access): Nginx access log format.
- **Nginx error** (Format: nginx_error): Nginx error log format.
- **PHP** (Format: php): PHP FPM logs.
- **Syslog** (Format: syslog): Syslog format.

## Usage

See `logsender --help`

```sh
# Use laravel built-in format
# Send to http://localhost:4317 (--to)
# Using OTLP (OpenTelemetry Protocol) (--sender)
# Read log file from storage/logs/laravel.log file (--input)
# Reporting logs for service "myapp" (--index)
# Parse JSON context into a "params" field (--smart)
logsender laravel --to=http://localhost:4317 --sender=otlp --input=storage/logs/laravel.log --index=myapp --smart

# Use syslog built-in format
# Send to http://localhost:4318
# Using OTLP (OpenTelemetry Protocol) (--sender) but using OTLP over HTTP (--otlp-protocol)
# Read log file from syslog.log file (--input)
logsender syslog --to=http://localhost:4318 --sender=otlp --otlp-protocol=http --input=syslog.log

# Use laravel built-in format
# Send to https://http-intake.logs.datadoghq.eu
# Using direct Datadog sender (--sender)
# Defined the API key from the environment variable $DD_API_KEY (--api-key)
# Reporting logs for service "myapp" (--index)
# Defined the sender hostname is "web01" (--hostname)
# Indicating that logs datetimes are in the Europe/Madrid timezone (--from-timezone)
# Read log file from laravel.log file (--input)
# Parse JSON context into a "params" field (--smart)
logsender laravel --to=https://http-intake.logs.datadoghq.eu --sender=datadog --api-key=$DD_API_KEY \
  --index=myapp --hostname=web01 --from-timezone=Europe/Madrid --input=laravel.log --smart  

# Use custom format defined into my_format.yaml file
# Send to https://user:pass@es.lan:9200 (--to)
# Using the Elastic Search sender (--sender)
# Read log file my.log (--input)
# Reporting logs for service "myapp" (--index)
# Specified that source date times defined in the log are using UTC (--from-timezone)
# Specified that dates times are going to be converted into Europe/Madrid timezone (--to-timezone)
logsender my_format.yaml --to=https://user:pass@es.lan:9200 --sender=es --input=my.log --index=myapp --from-timezone=UTC --to-timezone=Europe/Madrid

# Backfill an old file to stdout as JSON lines and inspect with jq
logsender laravel --smart < storage/logs/laravel-2024-01-01.log | jq .

# Backfill an old file and send the results to using the post sender
logsender laravel --to=https://example.net --sender=post < storage/logs/laravel-2024-01-01.log

# Any webhook: one POST per batch with a JSON array body
logsender laravel --to=https://hooks.example.com/logs --sender=post -H 'X-Token: abc' --input=laravel.log --smart

# nginx error log with a built-in format, or your own format file
logsender nginx_error --to=http://localhost:4317 --sender=otlp --input=/var/log/nginx/error.log
logsender ./log_formats/my_app.yaml --to=http://localhost:4317 --sender=otlp --input=app.log
```

## Custom log formats

A custom format is defined into a YAML file that tells LogSender how to recognize a header line and which captures
become the timestamp, level, message and environment. Patterns use [Grok](https://github.com/logstash-plugins/logstash-patterns-core/blob/main/patterns/ecs-v1/grok-patterns) syntax on top of PCRE2, so the whole Logstash pattern library (`TIMESTAMP_ISO8601`, `LOGLEVEL`,
`IPORHOST`, `HTTPDATE`, ...) is available.

### Writing a format

```yaml
name: myapp
description: My application log

patterns:                      # optional extra grok patterns usable in `match`
  MYAPP_LEVEL: '(?:DBG|INF|WRN|ERR)'

match:                         # tried in order; the first expression that matches starts an entry
  - '^%{TIMESTAMP_ISO8601:timestamp} \[%{MYAPP_LEVEL:level}\] %{DATA:logger}: %{GREEDYDATA:message}$'

timestamp:
  field: timestamp             # capture name; `null` stamps entries with the read time
  format: '%Y-%m-%dT%H:%M:%S%.f'   # chrono strftime, or rfc3339 | epoch | epoch_ms
  assume_current_year: false   # true for formats without a year (syslog)
  zone_field: null             # capture holding an IANA zone or ±HH:MM that overrides --from-timezone

level:
  field: level                 # omit the capture or set `field: null` to always use `default`
  default: INFO
  map:                         # case-insensitive aliases → EMERGENCY, ALERT, CRITICAL, ERROR, WARNING, NOTICE, INFO, DEBUG
    dbg: DEBUG
    inf: INFO
    wrn: WARNING
    err: ERROR

message:
  field: message

environment:
  field: null                  # capture name, if the log carries one
  default: ''                  # otherwise this, or --environment

multiline:
  mode: continuation           # continuation | none | pattern
  pattern: '^\s+at '           # with mode: pattern, only matching lines are attached

json_context: false            # true: extract {...} chunks from the message like laralog
```

Every capture that is not one of the mapped fields becomes an extra field (`logger` above). Naive
timestamps are interpreted in `--from-timezone`; formats whose `format` carries `%z` keep the zone
from the line. A header line whose timestamp does not parse is dropped and reported once.

### Using a custom format


```sh
logsender ./myformats/myformat.yaml --to=http://localhost --sender=post --input=storage/logs/mylogs.log --index=myapp --smart
```

## Supervisor

LogSender works perfectly with [Supervisor](https://github.com/Supervisor/supervisor). See the following configuration file for supervisor:

```ini
[program:logsender]
command=/usr/local/bin/logsender laravel --to=http://localhost:4317 --sender=otlp --index=laravel --smart --from-timezone=Europe/Copenhagen --input=/var/www/storage/logs/laravel.log
autostart=true
autorestart=true
user=www-data
stopsignal=TERM
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/var/log/logsender.log
```

## Build

```sh
cargo build --release
cargo test
```


## License

MIT
