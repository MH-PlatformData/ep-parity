# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Project Overview

`ep-parity` is a Python 3.10+ CLI tool for parity testing the Eligibility Processor service. It compares data between primary (EP 1.5) and replicated (EP 2.0) PostgreSQL databases to validate processing produces identical results.

## Architecture

```
ep_parity/
├── cli/           # Click CLI subcommands (export, compare, monitor, report, validate, config, init)
├── core/
│   ├── config.py      # Unified config: .env, paths_config.ini, comparison_config.ini
│   ├── database.py    # SQLAlchemy engine management, dual credential modes
│   ├── exporter.py    # SQL query execution and PSV/CSV export
│   ├── comparison/    # Comparison engine with specialized comparators per file type
│   ├── monitoring/    # SQS queue + DB polling monitors for processing completion
│   └── reporting/     # Excel summary generation (openpyxl)
└── utils/
    ├── logging.py     # Centralized logging setup
    └── runner.py      # Generic multi-employer batch runner (sequential/parallel)
```

## Common Commands

```bash
# Install (editable mode with dev dependencies)
pip install -e ".[dev]"

# Run all tests
pytest tests/

# Run the CLI
ep-parity --help
ep-parity export --emp_ids 150 --db_target both
ep-parity compare --emp_ids 150
ep-parity monitor --emp_ids 150 --env qa
ep-parity report --emp_ids 150 289
ep-parity validate

# Docker
docker compose build
docker compose run ep-parity validate
```

## Setup Guide for New Users

The recommended first-time setup uses the interactive wizard:

```bash
pip install -e ".[dev]"
ep-parity init
ep-parity validate
```

`ep-parity init` prompts for database credentials and paths, builds the PostgreSQL URIs automatically (handling special character escaping), and writes both `.env` and `paths_config.ini`.

For manual setup, a user needs three config files:

### 1. `.env` (database credentials)
```bash
cp .env.example .env
# Edit .env — replace placeholders with real credentials
# Required: DB_PRIMARY_URI, DB_REPLICATED_URI
# Optional: DB_PRODUCTION_URI, DB_PARIVEDA_DEV_URI
```

### 2. `paths_config.ini` (output paths and SQL query location)
```bash
cp paths_config.ini.example paths_config.ini
# Edit paths_config.ini:
#   [paths]
#   base_path = /path/to/parity/testing/results
#   sql_directory = /path/to/sql/scripts
```

### 3. `comparison_config.ini` (already checked in — no action needed)

### Prerequisites
- Python 3.10+
- VPN connection to Marathon Health internal network (databases use *.internal.marathon-health.com)
- AWS SSO session (only for `ep-parity monitor` — run `aws sso login --profile DataEngineerQA`)
- SQL query files in the configured `sql_directory`

### Validate setup
```bash
ep-parity validate            # config checks only
ep-parity validate --check-db  # also tests database connectivity (requires VPN)
```

## Configuration Details

### Database targets (--db_target)
Both short codes and full names are accepted:
- `pri` / `primary` → DB_PRIMARY_URI
- `rep` / `replicated` → DB_REPLICATED_URI
- `both` → exports from both, auto-runs comparison
- `dev` / `pariveda-dev` → DB_PARIVEDA_DEV_URI
- `prod` / `production` → DB_PRODUCTION_URI

### Defaults section in paths_config.ini
Users can set defaults for common CLI arguments:
```ini
[defaults]
db_target = both
env = qa
aws_profile = DataEngineerQA
check_interval = 120
max_wait_time = 7200
```

### Credential modes
- **Default**: `.env` file with `DB_PRIMARY_URI=postgresql://...`
- **AWS Secrets Manager**: Set `use_aws_secrets = true` in `[defaults]` — fetches creds at runtime via AWS SSO

## Key Design Patterns

- **AppConfig** (`core/config.py`) is the single source of truth for all configuration. Never read config files directly in other modules.
- **DatabaseManager** (`core/database.py`) manages SQLAlchemy engine lifecycle. Use it instead of creating engines directly.
- **run_batch()** (`utils/runner.py`) handles multi-employer execution. Every CLI command that accepts `--emp_ids` delegates to this function.
- **Comparison dispatch** (`core/comparison/engine.py`) routes files to specialized comparators (eligibility, activities, issues) or falls back to generic comparison.

## Testing

```bash
pytest tests/                    # All tests
pytest tests/unit/               # Unit tests only
pytest tests/unit/test_config.py # Single module
pytest -v                        # Verbose output
```

Tests mock external dependencies (DB, boto3). No VPN or credentials needed.

## Files Never Committed

- `.env` — database credentials
- `paths_config.ini` — local filesystem paths
- Both are in `.gitignore`

## Containerization

```bash
docker compose build
docker compose run ep-parity export --emp_ids 150 --db_target both
```

Volumes mount config, output, SQL queries, and AWS credentials. `network_mode: host` required for VPN database access.

## VPN Requirements by Command

| Command | Needs VPN? | Needs AWS SSO? |
|---------|-----------|----------------|
| `ep-parity init` | No | No |
| `ep-parity validate` | No | No |
| `ep-parity validate --check-db` | Yes | No |
| `ep-parity config show` | No | No |
| `ep-parity export` | Yes | No |
| `ep-parity compare` | No | No |
| `ep-parity report` | No | No |
| `ep-parity monitor` | Yes | Yes (`aws sso login --profile DataEngineerQA`) |

## Troubleshooting

| Error / Symptom | Cause | Fix |
|-----------------|-------|-----|
| `could not translate host name` | VPN not connected | Connect to Marathon Health VPN |
| `password authentication failed` | Wrong credentials in `.env` | Run `ep-parity init` to reconfigure |
| `connection refused` | Wrong host/port or DB is down | Check `.env` URIs against `.env.example` |
| `connection timed out` | VPN connected but routing issue | Restart VPN; check host is reachable |
| `.env file not found` | Missing credential file | Run `ep-parity init` or copy `.env.example` to `.env` |
| `base_path not set` | Missing or misconfigured paths_config.ini | Run `ep-parity init` or copy `paths_config.ini.example` |
| `No SQL files found` | sql_directory empty or wrong path | Check `sql_directory` in `paths_config.ini`; run `ep-parity validate` |
| `No files found to compare` | Export didn't produce files or wrong directory | Re-run export; check `base_path` directory structure |
| `Token has expired` | AWS SSO session expired | Run `aws sso login --profile DataEngineerQA` |
| `No queues found` | Wrong `--env` or missing SQS access | Verify `--env qa` matches your AWS profile's region/account |
| DLQ errors detected | Lambda processing failures | Check CloudWatch logs; fix root cause; reprocess employer |

## Multi-Employer Debugging

When batch processing fails for some employers:

1. Check the summary printed at the end — it lists which employers failed
2. Re-run only the failed employers: `ep-parity export --emp_ids <failed_ids> --db_target both`
3. Use `--max_retries 1` for transient DB connection issues
4. For persistent failures, run a single employer with `-v` for debug output:
   ```bash
   ep-parity -v export --emp_ids <failed_id> --db_target both
   ```
5. If one employer consistently fails, check if it has unusual data (very large, special characters in names, etc.)
