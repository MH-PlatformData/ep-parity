"""Orchestrator for parity comparison runs.

``ParityComparison`` ties together directory discovery, file dispatch to
specialized comparators, and report generation.  It receives an ``AppConfig``
instance at construction time -- no internal config loading.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

from .activities_comparator import compare_activities
from .base_comparator import load_and_prepare_dataframe
from .eligibility_comparator import compare_eligibilities
from .generic_comparator import compare_dataframes
from .issues_comparator import compare_issues
from .report_writer import generate_report

logger = get_logger("comparison.engine")


class ParityComparison:
    """High-level orchestrator for a single parity comparison run.

    Parameters
    ----------
    config:
        Fully-initialised ``AppConfig`` (paths, DB settings, comparison settings).
    employer_id:
        Employer ID for the run.
    run_timestamp:
        Specific run-timestamp folder name.  When ``None`` the most recent
        matching directory is used.
    """

    def __init__(
        self,
        config: AppConfig,
        employer_id: str | int,
        run_timestamp: str | None = None,
    ) -> None:
        self.config = config
        self.employer_id = str(employer_id)
        self.run_timestamp = run_timestamp
        self.comparison_results: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Directory discovery
    # ------------------------------------------------------------------

    def find_run_directory(self) -> Path:
        """Find the appropriate run directory based on employer_id and timestamp."""
        base_path = self.config.base_path

        if self.run_timestamp:
            pattern = f"*{self.employer_id}*{self.run_timestamp}*"
        else:
            pattern = f"*{self.employer_id}*"

        matching_dirs: list[Path] = []
        for date_dir in base_path.glob("*"):
            if date_dir.is_dir():
                for run_dir in date_dir.glob(pattern):
                    if run_dir.is_dir():
                        matching_dirs.append(run_dir)

        if not matching_dirs:
            raise FileNotFoundError(
                f"No run directories found for employer {self.employer_id}"
            )

        most_recent = max(matching_dirs, key=lambda x: x.stat().st_mtime)
        logger.info(f"Using run directory: {most_recent}")
        return most_recent

    def get_database_directories(
        self,
        run_dir: Path,
        primary_dir: Path | None = None,
        replicated_dir: Path | None = None,
    ) -> tuple[Path, Path]:
        """Get the primary and replicated database directories.

        When *primary_dir* / *replicated_dir* are provided explicitly they are
        used directly (the "compare pre-existing folders" feature).  Otherwise
        the standard sub-directory names under *run_dir* are used.
        """
        if primary_dir is None:
            primary_dir = run_dir / "primary-portal-db.qa"
        if replicated_dir is None:
            replicated_dir = run_dir / "replicated-pariveda-db.qa"

        if not primary_dir.exists():
            raise FileNotFoundError(
                f"Primary database directory not found: {primary_dir}"
            )
        if not replicated_dir.exists():
            raise FileNotFoundError(
                f"Replicated database directory not found: {replicated_dir}"
            )

        return primary_dir, replicated_dir

    def get_files_to_compare(
        self,
        primary_dir: Path,
        replicated_dir: Path,
    ) -> list[str]:
        """Get list of files that exist in both directories and should be compared."""
        primary_files = {f.name for f in primary_dir.glob("*") if f.is_file()}
        replicated_files = {f.name for f in replicated_dir.glob("*") if f.is_file()}

        common_files = primary_files & replicated_files

        exclude_lower = {ef.lower() for ef in self.config.exclude_files}
        files_to_compare = [
            f for f in common_files if f.lower() not in exclude_lower
        ]
        files_to_compare.sort()

        logger.info(f"Found {len(files_to_compare)} files to compare")
        logger.info(f"Files: {files_to_compare}")

        if primary_files - replicated_files:
            logger.warning(
                f"Files only in primary: {primary_files - replicated_files}"
            )
        if replicated_files - primary_files:
            logger.warning(
                f"Files only in replicated: {replicated_files - primary_files}"
            )

        return files_to_compare

    # ------------------------------------------------------------------
    # Ignore-column resolution (mirrors original helper)
    # ------------------------------------------------------------------

    def _get_ignore_columns_for_file(self, filename: str) -> list[str]:
        """Get the complete list of columns to ignore for a specific file."""
        ignore_cols = list(self.config.global_ignore_columns)
        ignore_cols.extend(self.config.get_file_specific_ignore_columns(filename))
        return list(set(ignore_cols))

    # ------------------------------------------------------------------
    # File comparison dispatch
    # ------------------------------------------------------------------

    def compare_files(
        self,
        primary_dir: Path,
        replicated_dir: Path,
        files_to_compare: list[str],
    ) -> list[dict[str, Any]]:
        """Compare all files between primary and replicated directories."""
        results: list[dict[str, Any]] = []

        for filename in files_to_compare:
            logger.info(f"Comparing {filename}...")

            primary_file = primary_dir / filename
            replicated_file = replicated_dir / filename

            try:
                ignore_columns = self._get_ignore_columns_for_file(filename)

                # Preserve key columns needed by specialized comparators
                if filename.lower().startswith("8") and "eligib" in filename.lower():
                    ignore_columns_for_loading = [
                        col
                        for col in ignore_columns
                        if col not in ("id", "row_number")
                    ]
                elif (
                    "5a-activities" in filename.lower()
                    or "activities-potential" in filename.lower()
                ):
                    ignore_columns_for_loading = [
                        col
                        for col in ignore_columns
                        if col not in ("row_number", "object_changes")
                    ]
                else:
                    ignore_columns_for_loading = [
                        col for col in ignore_columns if col != "row_number"
                    ]

                df_primary = load_and_prepare_dataframe(
                    primary_file, ignore_columns_for_loading, self.config
                )
                df_replicated = load_and_prepare_dataframe(
                    replicated_file, ignore_columns_for_loading, self.config
                )

                # Dispatch to specialized comparator based on filename pattern
                if (
                    filename.lower().startswith("8")
                    and "eligib" in filename.lower()
                ):
                    logger.info(
                        f"Using specialized eligibility comparison for {filename}"
                    )
                    comparison_result = compare_eligibilities(
                        df_primary, df_replicated, filename, self.config
                    )
                elif (
                    "5a-activities" in filename.lower()
                    or "activities-potential" in filename.lower()
                ):
                    logger.info(
                        f"Using specialized activities comparison for {filename}"
                    )
                    comparison_result = compare_activities(
                        df_primary, df_replicated, filename, self.config
                    )
                elif "issues" in filename.lower() and (
                    "potentials" in filename.lower()
                    or "uep" in filename.lower()
                ):
                    logger.info(
                        f"Using specialized issues comparison for {filename}"
                    )
                    comparison_result = compare_issues(
                        df_primary, df_replicated, filename, self.config
                    )
                else:
                    comparison_result = compare_dataframes(
                        df_primary, df_replicated, filename, self.config
                    )

                results.append(comparison_result)

                if comparison_result["match"]:
                    logger.info(f"MATCH: {filename}")
                else:
                    logger.warning(
                        f"DIFF: {filename} - {comparison_result['summary']}"
                    )

            except Exception as e:
                logger.error(f"ERROR: {filename} - {str(e)}")
                results.append(
                    {
                        "filename": filename,
                        "match": False,
                        "error": str(e),
                        "summary": f"Error: {str(e)}",
                    }
                )

        return results

    # ------------------------------------------------------------------
    # Full run
    # ------------------------------------------------------------------

    def run_comparison(
        self,
        output_report: str | Path | None = None,
        primary_dir: Path | None = None,
        replicated_dir: Path | None = None,
    ) -> list[dict[str, Any]]:
        """Run the complete parity comparison process.

        Parameters
        ----------
        output_report:
            Optional path to save the comparison report.  When ``None`` a
            default path inside the run directory is used.
        primary_dir:
            Explicit primary directory (skips standard sub-directory lookup).
        replicated_dir:
            Explicit replicated directory (skips standard sub-directory lookup).

        Returns
        -------
        List of comparison result dicts.
        """
        try:
            run_dir = self.find_run_directory()

            pri_dir, rep_dir = self.get_database_directories(
                run_dir,
                primary_dir=primary_dir,
                replicated_dir=replicated_dir,
            )

            files_to_compare = self.get_files_to_compare(pri_dir, rep_dir)

            if not files_to_compare:
                logger.warning("No files found to compare!")
                return []

            results = self.compare_files(pri_dir, rep_dir, files_to_compare)
            self.comparison_results = results

            run_dir_name = run_dir.name
            safe_run_dir_name = run_dir_name.replace(" ", "_")

            if output_report is None:
                output_report = (
                    run_dir / f"{safe_run_dir_name}_comparison_report.txt"
                )

            generate_report(
                results,
                employer_id=self.employer_id,
                output_file=output_report,
                run_dir_name=run_dir_name,
            )

            return results

        except Exception as e:
            logger.error(f"Comparison failed: {e}")
            raise
