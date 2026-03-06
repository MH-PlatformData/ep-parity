"""Generic DataFrame comparator for files that do not match a specialized pattern.

Handles both row_number-keyed comparison (when row_number exists and is unique)
and full-row comparison as a fallback.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

from .base_comparator import compare_matching_records

logger = get_logger("comparison.generic")


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    filename: str,
    config: AppConfig,
) -> dict[str, Any]:
    """Compare two DataFrames and return detailed results.

    Tries row_number-based matching first (if the column exists and values are
    unique in both frames).  Falls back to positional / outer-merge comparison.
    """
    result: dict[str, Any] = {
        "filename": filename,
        "primary_rows": len(df1),
        "replicated_rows": len(df2),
        "columns_compared": (
            df1.columns.tolist() if not df1.empty else df2.columns.tolist()
        ),
        "match": False,
        "differences": {},
        "summary": "",
    }

    # Check for identical DataFrames
    if df1.equals(df2):
        result["match"] = True
        result["summary"] = "Perfect match"
        return result

    differences: list[str] = []

    # Row count
    if len(df1) != len(df2):
        differences.append(
            f"Row count: Primary={len(df1)}, Replicated={len(df2)}"
        )

    # Column differences
    if not df1.empty and not df2.empty:
        if set(df1.columns) != set(df2.columns):
            primary_only = set(df1.columns) - set(df2.columns)
            replicated_only = set(df2.columns) - set(df1.columns)
            if primary_only:
                differences.append(f"Columns only in primary: {primary_only}")
            if replicated_only:
                differences.append(f"Columns only in replicated: {replicated_only}")

    # Detailed row-by-row comparison (requires data + same columns)
    if not df1.empty and not df2.empty and set(df1.columns) == set(df2.columns):
        try:
            use_row_number_comparison = False

            if "row_number" in df1.columns and "row_number" in df2.columns:
                df1_copy = df1.copy()
                df2_copy = df2.copy()
                df1_copy["row_number"] = df1_copy["row_number"].astype(str)
                df2_copy["row_number"] = df2_copy["row_number"].astype(str)

                df1_row_num_unique = not df1_copy["row_number"].duplicated().any()
                df2_row_num_unique = not df2_copy["row_number"].duplicated().any()

                if df1_row_num_unique and df2_row_num_unique:
                    use_row_number_comparison = True
                    _compare_by_row_number(
                        df1_copy, df2_copy, differences, result, config
                    )

            # Fallback: full-row comparison
            if not use_row_number_comparison:
                _compare_full_rows(df1, df2, differences, result, config)

        except Exception as e:
            differences.append(f"Error in detailed comparison: {str(e)}")

    result["differences"]["summary"] = differences
    result["summary"] = "; ".join(differences)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compare_by_row_number(
    df1_copy: pd.DataFrame,
    df2_copy: pd.DataFrame,
    differences: list[str],
    result: dict[str, Any],
    config: AppConfig,
) -> None:
    """row_number-keyed comparison (mutates *differences* and *result* in place)."""
    primary_row_nums = set(df1_copy["row_number"].dropna())
    replicated_row_nums = set(df2_copy["row_number"].dropna())

    missing_in_replicated = primary_row_nums - replicated_row_nums
    extra_in_replicated = replicated_row_nums - primary_row_nums

    if missing_in_replicated:
        differences.append(
            f"Records in primary but NOT in replicated: {len(missing_in_replicated)}"
        )
        result["differences"]["primary_only_count"] = len(missing_in_replicated)
        sample_rows = df1_copy[
            df1_copy["row_number"].isin(list(missing_in_replicated)[:5])
        ]
        result["differences"]["primary_only_sample"] = sample_rows.to_dict("records")

    if extra_in_replicated:
        differences.append(
            f"Records in replicated but NOT in primary: {len(extra_in_replicated)}"
        )
        result["differences"]["replicated_only_count"] = len(extra_in_replicated)
        sample_rows = df2_copy[
            df2_copy["row_number"].isin(list(extra_in_replicated)[:5])
        ]
        result["differences"]["replicated_only_sample"] = sample_rows.to_dict("records")

    # Compare matching records field-by-field
    common_row_nums = primary_row_nums & replicated_row_nums
    if common_row_nums:
        df1_common = (
            df1_copy[df1_copy["row_number"].isin(common_row_nums)]
            .set_index("row_number")
            .sort_index()
        )
        df2_common = (
            df2_copy[df2_copy["row_number"].isin(common_row_nums)]
            .set_index("row_number")
            .sort_index()
        )

        compare_cols = [
            col for col in df1_common.columns if col in df2_common.columns
        ]

        records_with_diffs, columns_with_diffs = compare_matching_records(
            df1_common,
            df2_common,
            compare_cols,
            common_row_nums,
            normalize_fn=None,  # generic comparator uses raw equality
        )

        if records_with_diffs > 0:
            differences.append(
                f"Matching records with data differences: {records_with_diffs}"
            )
            result["differences"]["columns_with_diffs"] = columns_with_diffs


def _compare_full_rows(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    differences: list[str],
    result: dict[str, Any],
    config: AppConfig,
) -> None:
    """Positional / outer-merge fallback (mutates *differences* and *result*)."""
    if len(df1) == len(df2):
        diff_mask = (df1 != df2).any(axis=1)
        diff_count = diff_mask.sum()
        if diff_count > 0:
            differences.append(f"Rows with data differences: {diff_count}")
            sample_size = min(config.max_sample_differences, diff_count)
            result["differences"]["sample_different_rows"] = {
                "primary": df1[diff_mask].head(sample_size).to_dict("records"),
                "replicated": df2[diff_mask].head(sample_size).to_dict("records"),
            }
    else:
        # Different lengths - outer merge to find unique rows
        df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

        df1_dedupe = df1_sorted.drop_duplicates()
        df2_dedupe = df2_sorted.drop_duplicates()

        merged = df1_dedupe.merge(
            df2_dedupe, on=list(df1.columns), how="outer", indicator=True
        )
        primary_only = merged[merged["_merge"] == "left_only"]
        replicated_only = merged[merged["_merge"] == "right_only"]

        if len(primary_only) > 0:
            differences.append(
                f"Records in primary but NOT in replicated: {len(primary_only)}"
            )
            result["differences"]["primary_only_count"] = len(primary_only)
            result["differences"]["primary_only_sample"] = (
                primary_only.drop("_merge", axis=1).head(5).to_dict("records")
            )

        if len(replicated_only) > 0:
            differences.append(
                f"Records in replicated but NOT in primary: {len(replicated_only)}"
            )
            result["differences"]["replicated_only_count"] = len(replicated_only)
            result["differences"]["replicated_only_sample"] = (
                replicated_only.drop("_merge", axis=1).head(5).to_dict("records")
            )

        # Find matching records with differences
        both = merged[merged["_merge"] == "both"]
        if len(both) > 0 and len(df1) != len(df2):
            matching_with_diffs = min(len(df1), len(df2)) - len(both)
            if matching_with_diffs > 0:
                differences.append(
                    f"Matching records with data differences: {matching_with_diffs}"
                )
