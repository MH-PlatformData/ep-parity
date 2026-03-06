"""Specialized comparator for issues files (9-issues-potentials.psv, 10-issues-ueps.psv).

Uses ``row_number`` as the matching key to compare equivalent records between
the primary and replicated outputs.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

from .base_comparator import compare_matching_records, normalize_text

logger = get_logger("comparison.issues")


def compare_issues(
    df_primary: pd.DataFrame,
    df_replicated: pd.DataFrame,
    filename: str,
    config: AppConfig,
) -> dict[str, Any]:
    """Compare issues DataFrames keyed on ``row_number``.

    Returns a result dict compatible with the report writer, with the extra
    flag ``is_issues_file = True``.
    """
    result: dict[str, Any] = {
        "filename": filename,
        "primary_rows": len(df_primary),
        "replicated_rows": len(df_replicated),
        "match": False,
        "differences": {},
        "summary": "",
        "is_issues_file": True,
    }

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

    # Use row_number as matching key if available
    if "row_number" in df_primary.columns and "row_number" in df_replicated.columns:
        primary_row_nums = set(df_primary["row_number"].dropna().astype(str))
        replicated_row_nums = set(df_replicated["row_number"].dropna().astype(str))

        missing_in_replicated = primary_row_nums - replicated_row_nums
        extra_in_replicated = replicated_row_nums - primary_row_nums

        if missing_in_replicated:
            differences.append(
                f"Row_numbers in primary but NOT in replicated: "
                f"{len(missing_in_replicated)}"
            )
            result["differences"]["missing_in_replicated"] = sorted(
                [int(x) for x in missing_in_replicated if x.isdigit()]
            )

            missing_records = df_primary[
                df_primary["row_number"].isin(missing_in_replicated)
            ]
            result["differences"]["primary_only_sample"] = (
                missing_records.head(5).to_dict("records")
            )

        if extra_in_replicated:
            differences.append(
                f"Row_numbers in replicated but NOT in primary: "
                f"{len(extra_in_replicated)}"
            )
            result["differences"]["extra_in_replicated"] = sorted(
                [int(x) for x in extra_in_replicated if x.isdigit()]
            )

            extra_records = df_replicated[
                df_replicated["row_number"].isin(extra_in_replicated)
            ]
            result["differences"]["replicated_only_sample"] = (
                extra_records.head(5).to_dict("records")
            )

        # Compare matching row_numbers
        common_row_nums = primary_row_nums & replicated_row_nums
        if common_row_nums:
            compare_cols = [
                col
                for col in df_primary.columns
                if col
                not in (
                    "created_at",
                    "updated_at",
                    "issue_id",
                    "assignable_id",
                    "potentials_id",
                    "row_number",
                )
                and col in df_replicated.columns
            ]

            # Build indexed DataFrames by taking first occurrence per row_number
            # (handles potential duplicates)
            df1_indexed = (
                df_primary[df_primary["row_number"].astype(str).isin(common_row_nums)]
                .drop_duplicates(subset=["row_number"], keep="first")
                .set_index(df_primary["row_number"].astype(str))
            )
            # The set_index above uses the full series; re-index properly
            df1_for_compare = (
                df_primary.copy()
                .assign(row_number=df_primary["row_number"].astype(str))
            )
            df1_for_compare = (
                df1_for_compare[df1_for_compare["row_number"].isin(common_row_nums)]
                .drop_duplicates(subset=["row_number"], keep="first")
                .set_index("row_number")
                .sort_index()
            )
            df2_for_compare = (
                df_replicated.copy()
                .assign(row_number=df_replicated["row_number"].astype(str))
            )
            df2_for_compare = (
                df2_for_compare[df2_for_compare["row_number"].isin(common_row_nums)]
                .drop_duplicates(subset=["row_number"], keep="first")
                .set_index("row_number")
                .sort_index()
            )

            records_with_diffs, columns_with_diffs = compare_matching_records(
                df1_for_compare,
                df2_for_compare,
                compare_cols,
                common_row_nums,
                normalize_fn=normalize_text,
            )

            if records_with_diffs > 0:
                differences.append(
                    f"Matching row_numbers with data differences: {records_with_diffs}"
                )
                result["differences"]["columns_with_diffs"] = columns_with_diffs

    result["differences"]["summary"] = differences
    result["summary"] = "; ".join(differences) if differences else "Match"

    return result
