"""Query execution and data export to pipe-delimited PSV/CSV files.

Migrated from parity_testing_args.py. Handles:
- Reading SQL files with {{employer_id}} and {{time_interval}} placeholders
- Querying deposited_files for time interval calculation
- Exporting results as pipe-delimited files
"""

import csv
import datetime
import os
from pathlib import Path

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.core.database import DatabaseManager
from ep_parity.utils.logging import get_logger

logger = get_logger("exporter")

# Active query file mappings: SQL filename -> output filename
# Commented-out entries from the original are omitted; add back as needed.
QUERY_MAP: dict[str, str] = {
    "1-dep_files.sql": "1-deposited_files.psv",
    "4a-clean_dataset_rows.sql": "4a-cleaned_dataset_rows.csv",
    "4c-clean_dataset_rows_process_notes.sql": "4c-cleaned_dataset_rows_process_notes.csv",
    "5a-activities-potentials.sql": "5a-activities-potential.psv",
    "8b-eligibilities.sql": "8b-eligibilities.psv",
    "9-issues-potentials.sql": "9-issues-potentials.psv",
    "10-issues-uep.sql": "10-issues-ueps.psv",
    "11-users.sql": "11-users.psv",
    "13-pg_search_docs-users.sql": "13-pg_search_docs-users.psv",
    "14-pg_search_docs-uep.sql": "14-pg_search_docs-ueps.psv",
    "15-potentials.sql": "15-potentials.psv",
    "18b-user_employer_profiles.sql": "18b-user_employer_profiles.psv",
    "19a-versions_create.sql": "19a-versions-potentials_create.psv",
    "20-offering_sets.sql": "20-offering_sets.psv",
}


def get_created_at_from_deposited_files(
    db: DatabaseManager, target: str, emp_id: str
) -> datetime.datetime | None:
    """Get the most recent created_at timestamp from deposited_files for an employer.

    Returns a timezone-aware UTC datetime, or None if no records found.
    """
    row = db.execute_scalar(
        target,
        "SELECT created_at FROM deposited_files "
        "WHERE employer_id = :emp_id ORDER BY created_at DESC LIMIT 1",
        {"emp_id": emp_id},
    )
    if row and "created_at" in row:
        return pd.to_datetime(row["created_at"]).replace(
            tzinfo=datetime.timezone.utc
        )
    return None


def read_and_format_sql_file(
    filepath: Path,
    emp_id: str,
    created_at: datetime.datetime | None = None,
    now: datetime.datetime | None = None,
) -> str:
    """Read a SQL file and replace {{employer_id}} and {{time_interval}} placeholders."""
    query = filepath.read_text()
    query = query.replace("{{employer_id}}", emp_id)

    if created_at and now:
        min_time_interval = int((now - created_at).total_seconds() / 60)
        time_interval = f"{min_time_interval} minutes"
        query = query.replace("{{time_interval}}", time_interval)

    return query


def export_queries(
    config: AppConfig,
    db: DatabaseManager,
    target: str,
    emp_id: str,
    output_directory: Path,
    query_map: dict[str, str] | None = None,
) -> Path:
    """Export all SQL query results for one employer against one database target.

    Args:
        config: Application configuration.
        db: Database manager instance.
        target: DB target short code ('pri', 'rep', 'dev', 'prod').
        emp_id: Employer ID.
        output_directory: Base output directory for this run.
        query_map: Optional override for the query file -> output file mapping.

    Returns:
        Path to the database-specific output subdirectory.
    """
    queries = query_map or QUERY_MAP
    sql_dir = config.sql_directory
    folder_name = config.get_folder_name(target)
    db_output_dir = output_directory / folder_name
    db_output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc)

    # Get time interval from deposited_files
    created_at = None
    try:
        created_at = get_created_at_from_deposited_files(db, target, emp_id)
        if created_at:
            minutes = int((now - created_at).total_seconds() / 60)
            logger.info(
                f"Retrieved created_at from {folder_name}: {created_at} "
                f"(lookback: {minutes} min)"
            )
    except Exception as e:
        logger.warning(f"Could not retrieve created_at for {folder_name}: {e}")

    for sql_filename, output_filename in queries.items():
        sql_path = sql_dir / sql_filename
        if not sql_path.exists():
            logger.warning(
                f"SQL file not found, skipping: {sql_path}\n"
                f"  Check that sql_directory is correct in paths_config.ini.\n"
                f"  Current sql_directory: {sql_dir}"
            )
            continue

        query = read_and_format_sql_file(sql_path, emp_id, created_at, now)

        try:
            df = db.execute_query(target, query)
            output_path = db_output_dir / output_filename
            df.to_csv(
                output_path,
                index=False,
                sep="|",
                quoting=csv.QUOTE_NONE,
                escapechar=" ",
            )
            logger.info(f"Exported {output_filename} to {folder_name} for employer {emp_id}")
        except Exception as e:
            logger.error(f"Error exporting {output_filename}: {e}")

    return db_output_dir


def build_output_directory(config: AppConfig, emp_id: str) -> Path:
    """Create and return the timestamped output directory for a parity run.

    Structure: {base_path}/{MM-DD-YYYY}/{emp_id MM-DD-YY HHMM}/
    """
    now = datetime.datetime.now()
    date_parts = config.date_format.split()

    daily_foldername = config.directory_format.format(
        emp_id=emp_id,
        date=now.strftime(date_parts[0]) if date_parts else now.strftime("%m-%d-%y"),
        time=now.strftime(date_parts[1]) if len(date_parts) > 1 else "",
    ).strip()

    output_path = config.base_path / now.strftime("%m-%d-%Y")
    output_directory = output_path / daily_foldername
    output_directory.mkdir(parents=True, exist_ok=True)

    return output_directory


def run_export(
    config: AppConfig,
    db: DatabaseManager,
    emp_id: str,
    db_targets: list[str],
) -> tuple[Path, list[str]]:
    """Run the full export workflow for one employer.

    Args:
        config: Application configuration.
        db: Database manager instance.
        emp_id: Employer ID.
        db_targets: List of resolved DB target short codes (e.g. ['ep15-qa', 'ep20-qa']).

    Returns:
        Tuple of (output_directory, list of target short codes that were exported).
    """
    targets = db_targets
    output_directory = build_output_directory(config, emp_id)

    logger.info(f"Exporting employer {emp_id} to {output_directory}")

    for target in targets:
        export_queries(config, db, target, emp_id, output_directory)

    logger.info(f"Export completed for employer {emp_id}")
    logger.info(f"Output directory: {output_directory}")

    return output_directory, targets
