# EP Parity

A CLI tool for comparing data between primary and replicated databases used by the Eligibility Processor. It exports query results, compares them file-by-file, monitors processing completion, and generates Excel summary reports.

## What is Parity Testing?

Marathon Health's Eligibility Processor (EP) is being migrated from version 1.5 to version 2.0. During the transition, both versions process the same eligibility files in parallel — EP 1.5 writes to the "primary" database and EP 2.0 writes to the "replicated" database. Parity testing confirms that both versions produce identical results, so we can trust EP 2.0 before decommissioning EP 1.5.

## Overview

EP Parity validates that the eligibility processor produces identical results in primary (EP 1.5) and replicated (EP 2.0) databases. The workflow is:

1. **Export** -- Run SQL queries against one or both databases, save results as PSV/CSV files
2. **Compare** -- Diff the exported files, applying configurable ignore rules and normalization
3. **Monitor** (optional) -- Wait for EP 1.5 and/or EP 2.0 processing to finish before exporting
4. **Report** -- Generate a color-coded Excel workbook summarizing comparison results

All commands support multiple employers via `--emp_ids` or `--emp_ids_file`, with optional parallel execution.

```
ep-parity export   --emp_ids 150 --db_target ep15-qa --db_target ep20-qa
ep-parity compare  --emp_ids 150
ep-parity monitor  --emp_ids 150 --env qa
ep-parity report   --emp_ids 150 289 300
ep-parity validate
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- VPN connection to Marathon Health's internal network
- Database credentials (read access to primary and replicated databases)
- AWS SSO credentials (only needed for `monitor` command)

### Installation

```bash
git clone <repo-url> && cd ep-parity

python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

pip install -e .
```

This installs the `ep-parity` CLI globally within the virtualenv.

### Configure (Recommended: Interactive Setup)

The easiest way to configure ep-parity is the interactive setup wizard:

```bash
ep-parity init
```

This prompts you for:
- Database credentials per environment (Dev, QA, Prod — passwords may differ; skip any you don't have)
- Where to save output files and where your SQL query files are located

It automatically builds the PostgreSQL connection URIs (handling special characters in passwords) and writes both `.env` and `paths_config.ini` for you.

### Configure (Manual)

If you prefer to set up manually:

```bash
cp .env.example .env
cp paths_config.ini.example paths_config.ini
```

Edit `.env` with your database credentials. The format is `KEY=VALUE` with no quotes or spaces around `=`:

```properties
# Dev
DB_EP15_DEV_URI=postgresql://jane.doe:your_password@primary-portal-db.dev.internal.marathon-health.com:5432/portal_dev
DB_EP20_DEV_URI=postgresql://jane.doe:your_password@primary-pariveda-db.dev.internal.marathon-health.com:5432/pariveda_dev

# QA (most common)
DB_EP15_QA_URI=postgresql://jane.doe:your_password@primary-portal-db.qa.internal.marathon-health.com:5432/portal_qa
DB_EP20_QA_URI=postgresql://jane.doe:your_password@primary-parevida-db.qa.internal.marathon-health.com:5432/portal_qa

# Production (optional, read-only)
# DB_PRODUCTION_URI=postgresql://jane.doe:your_password@primary-portal-db.prod.internal.marathon-health.com:5432/portal_production
```

If your password contains special characters (`@`, `#`, `%`, `:`), they must be URL-encoded. For example, `p@ss#word` becomes `p%40ss%23word`. The `ep-parity init` command handles this automatically.

Edit `paths_config.ini` with your local paths:

```ini
[paths]
base_path = /Users/yourname/Documents/Parity Testing
sql_directory = /Users/yourname/Documents/Parity Testing/scripts
```

### Getting Your Credentials

Database credentials are issued by the Platform Data team. You need read access to the QA (and optionally production) PostgreSQL databases. Your credentials are typically your Marathon Health Active Directory username and a database-specific password.

If you don't have credentials, ask your team lead or check with Platform Data engineering.

### Validate Setup

```bash
ep-parity validate              # Check config files
ep-parity validate --check-db   # Also test database connectivity (requires VPN)
```

This checks Python version, dependencies, `.env`, `paths_config.ini`, `comparison_config.ini`, SQL file availability, and optionally database connectivity.

### First Run

```bash
# Connect to VPN first!
ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa
```

This exports data from both databases and automatically runs a comparison. The report is saved to the output directory configured in `paths_config.ini`.

---

## Configuration

EP Parity uses three configuration files. The tool searches for them in this order: explicit `--config-dir` path, `PARITY_CONFIG_DIR` environment variable, current working directory, then the package root.

### `.env` -- Database Credentials

```properties
# Dev
DB_EP15_DEV_URI=postgresql://user:pass@primary-portal-db.dev.internal.marathon-health.com:5432/portal_dev
DB_EP20_DEV_URI=postgresql://user:pass@primary-pariveda-db.dev.internal.marathon-health.com:5432/pariveda_dev

# QA
DB_EP15_QA_URI=postgresql://user:pass@primary-portal-db.qa.internal.marathon-health.com:5432/portal_qa
DB_EP20_QA_URI=postgresql://user:pass@primary-parevida-db.qa.internal.marathon-health.com:5432/portal_qa

# Production
DB_PRODUCTION_URI=postgresql://user:pass@primary-portal-db.prod.internal.marathon-health.com:5432/portal_production
```

Never commit `.env` to version control.

### `paths_config.ini` -- Paths and Defaults

```ini
[paths]
base_path = /Users/yourname/Documents/Parity Testing
sql_directory = /Users/yourname/Documents/Parity Testing/scripts

[output]
directory_format = {emp_id} {date} {time}
date_format = %%m-%%d-%%y %%H%%M

[defaults]
# All optional. CLI arguments always override these.
# db_target = ep15-qa ep20-qa
# env = qa
# aws_profile = DataEngineerQA
# check_interval = 120
# max_wait_time = 7200
# parallel = false

# AWS Secrets Manager mode (see Credential Management section)
# use_aws_secrets = false
# aws_secret_path = your/secret/path
```

Use forward slashes for paths on all platforms, including Windows (`C:/Users/...`).

### `comparison_config.ini` -- Comparison Rules

Controls which columns to ignore, which files to skip, normalization rules, and display limits. This file ships with sensible defaults and is checked into version control.

Key sections:

| Section | Purpose |
|---------|---------|
| `[global_ignore_columns]` | Columns ignored in every file (e.g., `updated_at`, `created_at`, `id`) |
| `[file_specific_ignore_columns]` | Extra columns to ignore per file (e.g., `1-deposited_files.psv = ended_at,scan_started_at`) |
| `[exclude_files]` | Files skipped entirely (e.g., `2a-deposited_file_rows.psv = true`) |
| `[normalize_columns]` | Columns normalized before comparison (lowercase, whitespace) |
| `[normalize_none_string_columns]` | Columns where the string "None" equals empty/null |
| `[comparison_settings]` | `max_sample_differences`, `max_unique_rows_display`, `case_sensitive_comparison` |

---

## Commands

Every command accepts these global options:

```
--verbose / -v          Enable debug logging
--config-dir PATH       Directory containing config files
```

Every command that processes employers accepts:

```
--emp_ids ID [ID ...]   One or more employer IDs
--emp_ids_file FILE     Text file with one employer ID per line (# comments allowed)
--parallel              Process employers in parallel
--max_workers N         Max parallel workers (default: min(4, num_employers))
--max_retries N         Retry attempts per employer on failure (default: 0)
```

### `ep-parity export`

Export query results from one or both databases.

```bash
# Export from both QA databases (auto-runs comparison)
ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa

# Export from EP 1.5 QA only, no comparison
ep-parity export --emp_ids 150 --db_target ep15-qa --no_compare

# Multiple employers in parallel
ep-parity export --emp_ids 150 289 300 --db_target ep15-qa --db_target ep20-qa --parallel

# Employers from a file, single target
ep-parity export --emp_ids_file employers.txt --db_target ep20-qa

# Force comparison even for single-db export
ep-parity export --emp_ids 150 --db_target ep15-qa --compare
```

Options:

| Option | Description |
|--------|-------------|
| `--db_target` | One or more targets: `ep15-dev`, `ep15-qa`, `ep20-dev`, `ep20-qa`, `prod` (also accepts friendly aliases) |
| `--compare / --no_compare` | Run comparison after export (default: auto when exactly 2 targets given) |

Output directory structure:

```
{base_path}/
  YYYY-MM-DD/
    {emp_id} MM-DD-YY HHMM/
      ep15-qa/
        1-deposited_files.psv
        5a-activities-potential.psv
        ...
      ep20-qa/
        1-deposited_files.psv
        5a-activities-potential.psv
        ...
      {emp_id}_MM-DD-YY_HHMM_comparison_report.txt
```

### `ep-parity compare`

Compare previously exported parity results between primary and replicated databases.

```bash
# Compare most recent export for employer 289
ep-parity compare --emp_ids 289

# Compare a specific run
ep-parity compare --emp_ids 150 289 --run_timestamp "11-14-25 1530"

# Compare explicit directories
ep-parity compare --emp_ids 289 \
  --left_dir /path/to/ep15-qa \
  --right_dir /path/to/ep20-qa

# Parallel comparison with retries
ep-parity compare --emp_ids 150 289 300 --parallel --max_retries 2
```

Options:

| Option | Description |
|--------|-------------|
| `--run_timestamp` | Specific run folder timestamp (default: most recent) |
| `--left_dir` | Explicit path to left (e.g. ep15-qa) export folder |
| `--right_dir` | Explicit path to right (e.g. ep20-qa) export folder |

The comparison report includes:

- Summary of matches and differences
- File-by-file breakdown with row counts and column-level statistics
- Sample data for mismatched records
- Specialized logic for activities, eligibilities, and issues files

### `ep-parity monitor`

Monitor EP 1.5 and/or EP 2.0 processing completion, then optionally trigger export and comparison.

```bash
# Monitor both processors, auto-run parity when done
ep-parity monitor --emp_ids 150 --env qa

# Monitor EP 2.0 only (SQS queues)
ep-parity monitor --emp_ids 150 --env qa --mode ep20_only

# Monitor EP 1.5 only (database polling)
ep-parity monitor --emp_ids 150 --env qa --mode ep15_only

# Monitor without running parity
ep-parity monitor --emp_ids 150 289 --env dev --skip_parity

# Custom check interval and timeout
ep-parity monitor --emp_ids 150 --env qa --check_interval 30 --max_wait_time 7200
```

Options:

| Option | Default | Description |
|--------|---------|-------------|
| `--env` | (from config) | Environment: `qa` or `dev` |
| `--mode` | `both` | `both`, `ep15_only`, or `ep20_only` |
| `--aws_profile` | `DataEngineerQA` | AWS profile for SQS access |
| `--check_interval` | `120` | Seconds between status checks |
| `--max_wait_time` | `7200` | Maximum seconds to wait (2 hours) |
| `--skip_parity` | `false` | Skip parity testing after monitoring |

The monitor performs these steps:

1. **Pre-flight** -- Checks `deposited_files` table for the employer
2. **Poll** -- EP 1.5: checks `cleaned_datasets.status`; EP 2.0: checks 5 SQS queue depths
3. **DLQ check** -- Verifies no Dead Letter Queue messages (processing errors)
4. **Parity** -- Exports and compares (unless `--skip_parity` or DLQ errors found)

Requires `aws sso login --profile DataEngineerQA` for SQS monitoring.

### `ep-parity report`

Generate an Excel summary from existing text comparison reports.

```bash
# Generate Excel summary for multiple employers
ep-parity report --emp_ids 150 289 300

# Filter by date
ep-parity report --emp_ids 150 289 --date 11-14-25

# Custom output path
ep-parity report --emp_ids_file employers.txt --output summary.xlsx

# Custom base path
ep-parity report --emp_ids 150 --base_path /path/to/reports
```

The Excel workbook contains three sheets:

| Sheet | Purpose |
|-------|---------|
| **Executive Summary** | Per-employer success rate with color-coded status (green/yellow/red) |
| **Detailed Analysis** | Every file comparison with priority, issue type, and difference counts |
| **Issues Requiring Review** | Only files with differences, sorted by severity (HIGH/MEDIUM/LOW) |

### `ep-parity validate`

Check that the environment is correctly configured.

```bash
ep-parity validate              # Config file checks only
ep-parity validate --check-db   # Also test database connectivity (requires VPN)
```

Checks: Python version, virtual environment, installed dependencies, `.env` credentials, `paths_config.ini` paths, `comparison_config.ini`, SQL file availability, and write permissions. With `--check-db`, also tests live database connectivity and reports connection latency or actionable error messages.

### `ep-parity config show`

Display the current configuration with passwords masked.

```bash
ep-parity config show
```

Shows database URIs (with passwords replaced by `****`), configured paths, output format settings, defaults, and comparison rules. Useful for verifying what configuration is actually loaded.

### `ep-parity init`

Interactive setup wizard for first-time configuration.

```bash
ep-parity init
```

Prompts for database credentials per environment (Dev, QA, Prod — passwords may differ). Environments can be skipped if you don't have access. Builds all 5 PostgreSQL URIs with proper special character escaping. Writes `.env` and `paths_config.ini`, then runs validation.

For CI/automation, use `--non-interactive` with environment variables:

```bash
EP_INIT_QA_USER=jane EP_INIT_QA_PASS=secret \
  ep-parity init --non-interactive
```

Set `EP_INIT_DEV_USER`/`EP_INIT_DEV_PASS` and/or `EP_INIT_PROD_USER`/`EP_INIT_PROD_PASS` for additional environments.

---

## Multi-Employer Usage

All commands accept multiple employer IDs directly on the command line:

```bash
ep-parity export --emp_ids 150 289 300 --db_target ep15-qa --db_target ep20-qa
ep-parity compare --emp_ids 150 289 300
ep-parity report --emp_ids 150 289 300
```

Or load them from a file:

```bash
# employers.txt
# One ID per line. Comments and blank lines are ignored.
150
289
300
# 999  (commented out)
```

```bash
ep-parity export --emp_ids_file employers.txt --db_target ep15-qa --db_target ep20-qa
```

You can combine both:

```bash
ep-parity export --emp_ids 150 --emp_ids_file more_employers.txt --db_target ep15-qa --db_target ep20-qa
```

### Parallel Mode

Add `--parallel` to process employers concurrently:

```bash
ep-parity export --emp_ids 150 289 300 --db_target ep15-qa --db_target ep20-qa --parallel --max_workers 3
```

Sequential mode (the default) is safer and easier to debug. Use parallel mode when:

- You have sufficient system resources (CPU, memory, DB connections)
- You are running export or compare (not monitor, which is resource-intensive)
- You are confident the process works for your employers

Add `--max_retries 2` for transient failures:

```bash
ep-parity export --emp_ids 150 289 300 --db_target ep15-qa --db_target ep20-qa --parallel --max_retries 2
```

---

## Docker Usage

### Setup

Place your config files in the `config/` directory:

```bash
cp paths_config.ini.example config/paths_config.ini
cp comparison_config.ini config/comparison_config.ini
```

Create a `.env` file at the project root with your database credentials.

### Build and Run

```bash
docker compose build
docker compose run --rm ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa
docker compose run --rm ep-parity compare --emp_ids 150
```

### Volume Mounts

The `docker-compose.yml` maps three directories:

| Host Path | Container Path | Purpose |
|-----------|---------------|---------|
| `./config` | `/app/config` | `paths_config.ini`, `comparison_config.ini` |
| `./output` | `/app/output` | Exported query results |
| `./queries` | `/app/queries` | SQL query files |
| `~/.aws` | `/root/.aws` (read-only) | AWS credentials for SQS monitoring |

### VPN Access

The container uses `network_mode: host` so it can reach internal Marathon Health databases through your VPN connection.

On macOS with Docker Desktop, you may need to replace hostnames in `.env` with `host.docker.internal` if `network_mode: host` does not route correctly.

---

## Architecture

### Module Layout

```
ep-parity/
  pyproject.toml              # Package definition, ep-parity CLI entry point
  .env.example                # Database credential template
  paths_config.ini.example    # Path configuration template
  comparison_config.ini       # Comparison rules (checked in)
  Dockerfile
  docker-compose.yml

  ep_parity/
    __init__.py
    __main__.py               # python -m ep_parity entry point

    cli/                      # Click CLI layer
      main.py                 # @click.group with subcommands
      common.py               # Shared --emp_ids, --parallel decorators
      export_cmd.py
      compare_cmd.py
      monitor_cmd.py
      report_cmd.py
      validate_cmd.py

    core/                     # Business logic
      config.py               # AppConfig: unified config loader
      database.py             # DatabaseManager: connection pooling
      exporter.py             # SQL query execution and file export

      comparison/             # File comparison engine
        engine.py             # ParityComparison orchestrator
        base_comparator.py    # Generic row/column diff
        generic_comparator.py
        activities_comparator.py   # Specialized: 5a-activities-potential.psv
        eligibility_comparator.py  # Specialized: 8b-eligibilities.psv
        issues_comparator.py       # Specialized: 9-issues, 10-issues
        report_writer.py           # Text report generation

      monitoring/             # Processing completion monitors
        base_monitor.py       # Pre-flight checks, polling loop
        sqs_monitor.py        # EP 2.0 SQS queue + DLQ monitoring
        db_monitor.py         # EP 1.5 database status polling

      reporting/
        excel_summary.py      # Excel workbook generation

    utils/
      logging.py              # Structured logging setup
      runner.py               # Batch execution (sequential/parallel), retries, summary

  tests/
    unit/
      comparison/
      monitoring/
      reporting/
    integration/
```

### Data Flow

```
ep-parity export
  SQL files --> DatabaseManager --> PSV/CSV files on disk

ep-parity compare
  PSV/CSV files --> Comparators --> Text report on disk

ep-parity report
  Text reports --> ExcelSummary --> .xlsx workbook

ep-parity monitor
  SQS / DB polling --> Completion --> export --> compare (automatic)
```

---

## Migration Guide

If you are migrating from the original standalone scripts, here is how the old commands map to the new CLI:

| Old Command | New Command |
|-------------|-------------|
| `python parity_testing_args.py --emp_id 150 --db_target both` | `ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa` |
| `python parity_testing_args.py --emp_id 150 --db_target both --no_comparison` | `ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa --no_compare` |
| `python parity_testing_args.py --emp_id 150 --db_target pri` | `ep-parity export --emp_ids 150 --db_target ep15-qa` |
| `python compare_parity_results.py --emp_id 150` | `ep-parity compare --emp_ids 150` |
| `python compare_parity_results.py --emp_id 150 --run_timestamp "11-09-25 1405"` | `ep-parity compare --emp_ids 150 --run_timestamp "11-09-25 1405"` |
| `python compare_parity_results.py --emp_id 150 -v` | `ep-parity -v compare --emp_ids 150` |
| `python monitor_dual_processors.py --employer_id 150 --env qa` | `ep-parity monitor --emp_ids 150 --env qa` |
| `python monitor_dual_processors.py --employer_id 150 --env qa --ep15_only` | `ep-parity monitor --emp_ids 150 --env qa --mode ep15_only` |
| `python monitor_dual_processors.py --employer_id 150 --env qa --ep20_only` | `ep-parity monitor --emp_ids 150 --env qa --mode ep20_only` |
| `python monitor_dual_processors.py --employer_id 150 --env qa --skip_parity` | `ep-parity monitor --emp_ids 150 --env qa --skip_parity` |
| `python monitor_and_test_parity.py --employer_id 150 --env qa` | `ep-parity monitor --emp_ids 150 --env qa --mode ep20_only` |
| `python run_parity_multi_employer.py --emp_ids 150 289 --db_target both --parallel` | `ep-parity export --emp_ids 150 289 --db_target ep15-qa --db_target ep20-qa --parallel` |
| `python run_comparison_multi_employer.py --emp_ids 150 289 300` | `ep-parity compare --emp_ids 150 289 300` |
| `python run_monitor_multi_employer.py --employer_ids 150 289 --env qa` | `ep-parity monitor --emp_ids 150 289 --env qa` |
| `python generate_comparison_summary.py --emp_ids 150 289 300` | `ep-parity report --emp_ids 150 289 300` |
| `python generate_comparison_summary.py --emp_ids 150 --date 11-14-25` | `ep-parity report --emp_ids 150 --date 11-14-25` |
| `python validate_setup.py` | `ep-parity validate` |

Key differences:

- **Single CLI entry point**: `ep-parity <subcommand>` replaces 7+ separate Python scripts
- **`--emp_id` became `--emp_ids`**: Always plural, accepts multiple IDs directly
- **`--employer_id` / `--employer_ids` unified to `--emp_ids`**: Consistent across all commands
- **`--ep15_only` / `--ep20_only` became `--mode`**: Cleaner enum-style option
- **`--no_comparison` became `--no_compare`**: Follows Click boolean flag convention
- **`-v` is a global option**: Place it before the subcommand (`ep-parity -v export ...`)
- **Multi-employer is built in**: No separate wrapper scripts needed
- **`--emp_ids_file`**: New option on every command for loading IDs from a file

---

## Credential Management

EP Parity supports two modes for database credentials.

### Mode 1: `.env` File (Default)

Store PostgreSQL connection URIs in a `.env` file alongside your config:

```properties
DB_EP15_QA_URI=postgresql://user:pass@primary-portal-db.qa.internal.marathon-health.com:5432/portal_qa
DB_EP20_QA_URI=postgresql://user:pass@primary-parevida-db.qa.internal.marathon-health.com:5432/portal_qa
```

Up to 5 database URIs can be configured (`DB_EP15_DEV_URI`, `DB_EP15_QA_URI`, `DB_EP20_DEV_URI`, `DB_EP20_QA_URI`, `DB_PRODUCTION_URI`). This is the simplest approach for local development and is the default.

### Mode 2: AWS Secrets Manager

For environments where credentials should not be stored on disk, enable AWS Secrets Manager in `paths_config.ini`:

```ini
[defaults]
use_aws_secrets = true
aws_secret_path = your/secret/path
```

When enabled, the `DatabaseManager` resolves credentials from AWS Secrets Manager instead of reading `.env` variables. You still need valid AWS credentials (via `aws sso login` or IAM role).

---

## Troubleshooting

### Can't connect to the database?

1. **Are you on VPN?** — All databases use `*.internal.marathon-health.com` hostnames that are only reachable through Marathon Health VPN.
   - Error: `could not translate host name` → Connect to VPN
   - Error: `connection timed out` → Restart VPN connection

2. **Are your credentials correct?** — Check `.env` for typos, extra spaces, or unescaped special characters.
   - Error: `password authentication failed` → Run `ep-parity init` to reconfigure
   - Passwords with `@`, `#`, `%`, or `:` must be URL-encoded (the init wizard handles this)

3. **Is the host/port right?** — Compare your `.env` URIs against `.env.example`.
   - Error: `connection refused` → Wrong hostname or port, or DB is down

Run `ep-parity config show` to see what configuration is actually loaded.

### Other common issues

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError` | Activate virtualenv: `source .venv/bin/activate` |
| `No files found to compare` | Verify the run directory exists and both database folders have data |
| `No SQL files found` | Check `sql_directory` in `paths_config.ini`; run `ep-parity validate` |
| `Token has expired and refresh failed` | Run `aws sso login --profile DataEngineerQA` |
| `No queues found` | Check `--env` is correct and AWS profile has SQS access |
| DLQ errors detected | Check CloudWatch logs for Lambda errors; fix root cause and reprocess |
| Timeout during monitoring | Increase `--max_wait_time` or check CloudWatch for stuck Lambdas |
| `ep-parity: command not found` | Run `pip install -e .` in the ep-parity directory with your virtualenv active |

### Debug mode

For detailed diagnostics, add `-v` before the subcommand:

```bash
ep-parity -v export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa
```

### Which commands need VPN?

- **Need VPN**: `export`, `monitor`, `validate --check-db`
- **Offline**: `init`, `validate` (without --check-db), `config show`, `compare`, `report`

---

Internal use only -- Marathon Health
