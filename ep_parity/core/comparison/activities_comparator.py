"""Specialized comparator for activities files (5a-activities-potential.psv).

Each ``row_number`` can have 2-3 actions:
- All rows: ``execute_potential_resolution``, ``execute_employer_setting``
- Dependents only: ``execute_dependent_setting`` (when ``object_changes``
  contains ``"is_dependent": true``)

This module validates that both primary and replicated have the correct set of
actions per row_number.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ep_parity.core.config import AppConfig
from ep_parity.utils.logging import get_logger

from .base_comparator import get_person_info

logger = get_logger("comparison.activities")


def compare_activities(
    df_primary: pd.DataFrame,
    df_replicated: pd.DataFrame,
    filename: str,
    config: AppConfig,
) -> dict[str, Any]:
    """Compare activities DataFrames with per-row action validation.

    Returns a result dict compatible with the report writer, with the extra
    flag ``is_activities_file = True``.
    """
    result: dict[str, Any] = {
        "filename": filename,
        "primary_rows": len(df_primary),
        "replicated_rows": len(df_replicated),
        "match": False,
        "differences": {},
        "summary": "",
        "is_activities_file": True,
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

    # Group activities by row_number and action to compare
    if "row_number" in df_primary.columns and "action" in df_primary.columns:
        primary_row_nums = set(df_primary["row_number"].dropna().astype(str))
        replicated_row_nums = set(df_replicated["row_number"].dropna().astype(str))

        all_row_nums = primary_row_nums | replicated_row_nums

        missing_actions: list[dict[str, Any]] = []
        unexpected_actions: list[dict[str, Any]] = []
        row_nums_with_issues: set[str] = set()

        for row_num in sorted(
            all_row_nums, key=lambda x: int(x) if x.isdigit() else 0
        ):
            primary_rows = df_primary[
                df_primary["row_number"].astype(str) == row_num
            ]
            replicated_rows = df_replicated[
                df_replicated["row_number"].astype(str) == row_num
            ]

            primary_actions = set(primary_rows["action"].dropna())
            replicated_actions = set(replicated_rows["action"].dropna())

            # Determine dependent status from object_changes
            is_dependent = _check_is_dependent(primary_rows, replicated_rows)

            expected_actions = {"execute_potential_resolution", "execute_employer_setting"}
            if is_dependent:
                expected_actions.add("execute_dependent_setting")

            # Missing actions: in primary but not replicated
            missing_in_replicated = primary_actions - replicated_actions
            if missing_in_replicated:
                for action in missing_in_replicated:
                    # Skip reporting missing execute_dependent_setting for
                    # non-dependents (this is expected)
                    if action == "execute_dependent_setting" and not is_dependent:
                        continue

                    missing_actions.append(
                        {
                            "row_number": row_num,
                            "action": action,
                            "is_dependent": is_dependent,
                            "person_info": get_person_info(primary_rows),
                        }
                    )
                    row_nums_with_issues.add(row_num)

            # Unexpected actions: in replicated but not primary
            extra_in_replicated = replicated_actions - primary_actions
            if extra_in_replicated:
                for action in extra_in_replicated:
                    unexpected_actions.append(
                        {
                            "row_number": row_num,
                            "action": action,
                            "is_dependent": is_dependent,
                            "person_info": get_person_info(replicated_rows),
                        }
                    )
                    row_nums_with_issues.add(row_num)

            # Validate replicated has correct actions based on dependent status
            if replicated_actions and not primary_actions.issuperset(expected_actions):
                missing_expected = expected_actions - replicated_actions
                if missing_expected:
                    info_rows = (
                        primary_rows if not primary_rows.empty else replicated_rows
                    )
                    for action in missing_expected:
                        already_tracked = any(
                            ma["action"] == action and ma["row_number"] == row_num
                            for ma in missing_actions
                        )
                        if not already_tracked:
                            missing_actions.append(
                                {
                                    "row_number": row_num,
                                    "action": action,
                                    "is_dependent": is_dependent,
                                    "person_info": get_person_info(info_rows),
                                }
                            )
                            row_nums_with_issues.add(row_num)

        if missing_actions:
            differences.append(
                f"Missing actions in replicated: {len(missing_actions)}"
            )
            result["differences"]["missing_actions"] = missing_actions[:10]

        if unexpected_actions:
            differences.append(
                f"Unexpected actions in replicated: {len(unexpected_actions)}"
            )
            result["differences"]["unexpected_actions"] = unexpected_actions[:10]

        if row_nums_with_issues:
            differences.append(
                f"Row numbers with action mismatches: {len(row_nums_with_issues)}"
            )

    result["differences"]["summary"] = differences
    result["summary"] = "; ".join(differences) if differences else "Match"

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_is_dependent(
    primary_rows: pd.DataFrame,
    replicated_rows: pd.DataFrame,
) -> bool:
    """Return ``True`` if any row's ``object_changes`` indicates a dependent."""
    for rows in (primary_rows, replicated_rows):
        if rows.empty or "object_changes" not in rows.columns:
            continue
        for _, row in rows.iterrows():
            obj_changes = row["object_changes"]
            if pd.notna(obj_changes) and 'is_dependent":  true' in str(
                obj_changes
            ).lower():
                return True
    return False
