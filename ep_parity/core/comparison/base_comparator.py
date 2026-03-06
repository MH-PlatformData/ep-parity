"""Shared comparison primitives used by all specialized comparators.

Module-level functions that take explicit parameters rather than relying on
class state.  Every comparator module imports from here.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

logger = get_logger("comparison.base")


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

def detect_separator(file_path: Path) -> str:
    """Detect the separator used in a CSV/PSV file by examining the header.

    Falls back to extension-based detection when the header is ambiguous.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            header_line = f.readline().strip()

        pipe_count = header_line.count("|")
        comma_count = header_line.count(",")

        if pipe_count > 0:
            separator = "|"
            logger.debug(
                f"Detected pipe separator in {file_path.name} header "
                f"(pipes: {pipe_count}, commas: {comma_count})"
            )
        elif comma_count > 0:
            separator = ","
            logger.debug(
                f"Detected comma separator in {file_path.name} header "
                f"(pipes: {pipe_count}, commas: {comma_count})"
            )
        else:
            separator = "," if file_path.suffix.lower() == ".csv" else "|"
            logger.debug(
                f"No clear separator detected in {file_path.name} header, "
                f"using extension-based default: '{separator}'"
            )
        return separator

    except Exception as e:
        logger.warning(f"Error detecting separator for {file_path.name}: {e}")
        return "," if file_path.suffix.lower() == ".csv" else "|"


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

def normalize_text(text: Any, normalize_none_string: bool = False) -> str:
    """Normalize *text* for comparison.

    * ``None``, ``NaN``, and empty/whitespace strings all become ``""``.
    * Lower-cased, whitespace collapsed, underscores/hyphens replaced by spaces.
    * When *normalize_none_string* is ``True`` the word ``none`` (case-insensitive,
      word-boundary delimited) is also stripped.
    """
    if pd.isna(text) or text is None or str(text).strip() == "":
        return ""
    normalized = str(text).lower()
    if normalize_none_string:
        normalized = re.sub(r"\bnone\b", "", normalized)
    normalized = " ".join(normalized.split())
    normalized = normalized.replace("_", " ").replace("-", " ")
    return normalized.strip()


# ---------------------------------------------------------------------------
# DataFrame loading
# ---------------------------------------------------------------------------

def load_and_prepare_dataframe(
    file_path: Path,
    ignore_columns: list[str],
    config: AppConfig,
) -> pd.DataFrame:
    """Load a CSV/PSV file and prepare it for comparison.

    * Auto-detects separator
    * Drops *ignore_columns* that are present
    * Applies column-level normalization per *config*
    * Fills NaN with ``""`` and sorts deterministically
    """
    try:
        separator = detect_separator(file_path)
        df = pd.read_csv(file_path, sep=separator, dtype=str)
        logger.debug(
            f"Loaded {file_path.name}: {df.shape[0]} rows, "
            f"{df.shape[1]} columns, separator: '{separator}'"
        )

        cols_to_drop = [col for col in ignore_columns if col in df.columns]
        df = df.drop(columns=cols_to_drop)
        if cols_to_drop:
            logger.debug(f"Dropped columns from {file_path.name}: {cols_to_drop}")

        # Apply text normalization to configured columns
        normalize_cols = config.get_normalize_columns(file_path.name)
        for col in normalize_cols:
            if col in df.columns:
                logger.debug(f"Normalizing column '{col}' in {file_path.name}")
                df[col] = df[col].apply(normalize_text)

        # Apply "None"-string normalization to configured columns
        normalize_none_cols = config.get_normalize_none_string_columns(file_path.name)
        for col in normalize_none_cols:
            if col in df.columns:
                logger.debug(
                    f"Normalizing 'None' string in column '{col}' in {file_path.name}"
                )
                df[col] = df[col].apply(
                    lambda x: normalize_text(x, normalize_none_string=True)
                )

        df = df.fillna("")

        if not df.empty:
            df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)

        return df

    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise


# ---------------------------------------------------------------------------
# Person info extraction (used by activities comparator and others)
# ---------------------------------------------------------------------------

def get_person_info(df_rows: pd.DataFrame) -> dict[str, str]:
    """Extract person identifying information from activity/record rows."""
    if df_rows.empty:
        return {}

    row = df_rows.iloc[0]
    info: dict[str, str] = {}
    for col in ("first_name", "last_name", "born_on", "row_number"):
        if col in row.index and pd.notna(row[col]):
            info[col] = str(row[col])
    return info


# ---------------------------------------------------------------------------
# Reusable record-matching diff tracker
# ---------------------------------------------------------------------------

def compare_matching_records(
    df1_indexed: pd.DataFrame,
    df2_indexed: pd.DataFrame,
    compare_cols: list[str],
    common_keys: set[Any],
    normalize_fn: Callable[[Any], str] | None = None,
) -> tuple[int, dict[str, dict[str, Any]]]:
    """Compare rows that share a common key between two indexed DataFrames.

    Both *df1_indexed* and *df2_indexed* must already be ``set_index()``-ed on
    the matching key and sorted.

    Parameters
    ----------
    df1_indexed:
        Primary DataFrame, indexed by the matching key.
    df2_indexed:
        Replicated DataFrame, indexed by the matching key.
    compare_cols:
        Column names to compare for each matching record.
    common_keys:
        Set of key values present in both DataFrames.
    normalize_fn:
        Optional callable applied to each cell value before comparison.
        When ``None``, raw equality (with NaN handling) is used.

    Returns
    -------
    (records_with_diffs, columns_with_diffs)
        *records_with_diffs* is the count of keys that have at least one
        column difference.  *columns_with_diffs* maps column name to a dict
        with ``rows``, ``primary_has_value_replicated_empty``,
        ``primary_empty_replicated_has_value``, and
        ``both_have_different_values``.
    """
    records_with_diffs = 0
    columns_with_diffs: dict[str, dict[str, Any]] = {}

    for key in common_keys:
        if key not in df1_indexed.index or key not in df2_indexed.index:
            continue

        primary_row = df1_indexed.loc[key]
        replicated_row = df2_indexed.loc[key]

        has_diff = False
        for col in compare_cols:
            if normalize_fn is not None:
                primary_val = normalize_fn(primary_row[col])
                replicated_val = normalize_fn(replicated_row[col])
                is_different = primary_val != replicated_val
                p_empty = primary_val == ""
                r_empty = replicated_val == ""
            else:
                raw_p = primary_row[col]
                raw_r = replicated_row[col]
                if raw_p != raw_r:
                    # Both NaN counts as equal
                    if pd.isna(raw_p) and pd.isna(raw_r):
                        is_different = False
                    else:
                        is_different = True
                else:
                    is_different = False
                p_empty = pd.isna(raw_p) or raw_p == ""
                r_empty = pd.isna(raw_r) or raw_r == ""

            if is_different:
                has_diff = True
                if col not in columns_with_diffs:
                    columns_with_diffs[col] = {
                        "rows": [],
                        "primary_has_value_replicated_empty": 0,
                        "primary_empty_replicated_has_value": 0,
                        "both_have_different_values": 0,
                    }
                columns_with_diffs[col]["rows"].append(key)

                if p_empty and not r_empty:
                    columns_with_diffs[col]["primary_empty_replicated_has_value"] += 1
                elif not p_empty and r_empty:
                    columns_with_diffs[col]["primary_has_value_replicated_empty"] += 1
                else:
                    columns_with_diffs[col]["both_have_different_values"] += 1

        if has_diff:
            records_with_diffs += 1

    return records_with_diffs, columns_with_diffs
