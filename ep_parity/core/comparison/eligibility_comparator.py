"""Specialized comparator for eligibility files (8b-eligibilities.psv).

Provides detailed analysis of missing/extra records by eligibility ID,
including time-filter issue detection for old data in the replicated set.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

from .base_comparator import compare_matching_records, normalize_text

logger = get_logger("comparison.eligibility")


def compare_eligibilities(
    df_primary: pd.DataFrame,
    df_replicated: pd.DataFrame,
    filename: str,
    config: AppConfig,
) -> dict[str, Any]:
    """Compare eligibility DataFrames keyed on ``id``.

    Returns a result dict compatible with the report writer, with the extra
    flag ``is_eligibility_file = True``.
    """
    result: dict[str, Any] = {
        "filename": filename,
        "primary_rows": len(df_primary),
        "replicated_rows": len(df_replicated),
        "match": False,
        "differences": {},
        "summary": "",
        "is_eligibility_file": True,
    }

    # Convert ID columns to numeric for comparison
    if "id" in df_primary.columns:
        df_primary["id"] = pd.to_numeric(df_primary["id"], errors="coerce")
    if "id" in df_replicated.columns:
        df_replicated["id"] = pd.to_numeric(df_replicated["id"], errors="coerce")

    # Check for perfect match first
    if df_primary.equals(df_replicated):
        result["match"] = True
        result["summary"] = "Perfect match"
        return result

    differences: list[str] = []

    # Compare row counts
    if len(df_primary) != len(df_replicated):
        differences.append(
            f"Row count: Primary={len(df_primary)}, Replicated={len(df_replicated)}"
        )

    # Find missing/extra records by eligibility ID
    if "id" in df_primary.columns and "id" in df_replicated.columns:
        primary_ids = set(df_primary["id"].dropna())
        replicated_ids = set(df_replicated["id"].dropna())

        missing_in_replicated = primary_ids - replicated_ids
        extra_in_replicated = replicated_ids - primary_ids

        if missing_in_replicated:
            differences.append(
                f"Records in primary but NOT in replicated: {len(missing_in_replicated)}"
            )
            result["differences"]["missing_in_replicated"] = sorted(
                list(missing_in_replicated)
            )[:20]

            missing_records = df_primary[df_primary["id"].isin(missing_in_replicated)]
            result["differences"]["primary_only_sample"] = (
                missing_records.head(5).to_dict("records")
            )

        if extra_in_replicated:
            differences.append(
                f"Records in replicated but NOT in primary: {len(extra_in_replicated)}"
            )
            result["differences"]["extra_in_replicated"] = sorted(
                list(extra_in_replicated)
            )[:20]

            extra_records = df_replicated[
                df_replicated["id"].isin(extra_in_replicated)
            ]
            result["differences"]["replicated_only_sample"] = (
                extra_records.head(5).to_dict("records")
            )

            # Check for potential old data using created_at
            if (
                "created_at" in extra_records.columns
                and "created_at" in df_primary.columns
            ):
                try:
                    extra_created = pd.to_datetime(
                        extra_records["created_at"], errors="coerce"
                    )
                    primary_created = pd.to_datetime(
                        df_primary["created_at"], errors="coerce"
                    )

                    primary_min = primary_created.min()
                    extra_min = extra_created.min()

                    if (
                        pd.notna(extra_min)
                        and pd.notna(primary_min)
                        and extra_min < primary_min
                    ):
                        differences.append(
                            "Extra records contain OLD DATA "
                            "(created before primary's time range)"
                        )
                        result["differences"]["likely_time_filter_issue"] = True
                except Exception:
                    pass

        # Compare matching records
        common_ids = primary_ids & replicated_ids
        if common_ids:
            df_primary_common = (
                df_primary[df_primary["id"].isin(common_ids)]
                .set_index("id")
                .sort_index()
            )
            df_replicated_common = (
                df_replicated[df_replicated["id"].isin(common_ids)]
                .set_index("id")
                .sort_index()
            )

            compare_cols = [
                col
                for col in df_primary_common.columns
                if col
                not in (
                    "created_at",
                    "updated_at",
                    "deposited_file_id",
                    "potentials_id",
                    "row_number",
                )
                and col in df_replicated_common.columns
            ]

            records_with_diffs, columns_with_diffs = compare_matching_records(
                df_primary_common,
                df_replicated_common,
                compare_cols,
                common_ids,
                normalize_fn=normalize_text,
            )

            if records_with_diffs > 0:
                differences.append(
                    f"Matching records with data differences: {records_with_diffs}"
                )
                result["differences"]["columns_with_diffs"] = columns_with_diffs

    result["differences"]["summary"] = differences
    result["summary"] = "; ".join(differences) if differences else "Match"

    return result
