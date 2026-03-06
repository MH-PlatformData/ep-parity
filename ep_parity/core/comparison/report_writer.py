"""Generate human-readable parity comparison reports.

Standalone module -- depends only on the result dict structure produced by the
comparator modules, not on any class state.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from ep_parity.utils.logging import get_logger

logger = get_logger("comparison.report")


def generate_report(
    results: list[dict[str, Any]],
    employer_id: str | int,
    output_file: str | Path | None = None,
    run_dir_name: str | None = None,
) -> str:
    """Generate a comprehensive comparison report.

    Parameters
    ----------
    results:
        List of result dicts as returned by the comparator functions.
    employer_id:
        Employer ID displayed in the report header.
    output_file:
        When provided the report is also written to this path.
    run_dir_name:
        Optional run-directory name displayed in the report header.

    Returns
    -------
    The full report text.
    """

    def _sort_key(result: dict[str, Any]) -> tuple[int, str, str]:
        """Extract numeric prefix from filename for sorting."""
        filename = result["filename"]
        match = re.match(r"^(\d+)([a-z]?)", filename.lower())
        if match:
            num = int(match.group(1))
            letter = match.group(2) or ""
            return (num, letter, filename)
        return (999, "", filename)

    results = sorted(results, key=_sort_key)

    total_files = len(results)
    matching_files = len([r for r in results if r.get("match", False)])

    report_lines: list[str] = [
        "=" * 80,
        "PARITY COMPARISON REPORT",
        "=" * 80,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Employer ID: {employer_id}",
    ]

    if run_dir_name:
        report_lines.append(f"Run Directory: {run_dir_name}")

    report_lines.extend(
        [
            f"Total files compared: {total_files}",
            f"Matching files: {matching_files}",
            f"Different files: {total_files - matching_files}",
            "",
            "SUMMARY:",
            "-" * 40,
        ]
    )

    for result in results:
        status = "MATCH" if result.get("match", False) else "DIFF"
        report_lines.append(
            f"{status:10} {result['filename']:40} - "
            f"{result.get('summary', 'N/A')}"
        )

    report_lines.extend(["", "DETAILED RESULTS:", "-" * 40])

    for result in results:
        if not result.get("match", False):
            report_lines.extend(
                [
                    f"\nFile: {result['filename']}",
                    f"Primary rows: {result.get('primary_rows', 'N/A')}",
                    f"Replicated rows: {result.get('replicated_rows', 'N/A')}",
                    "Summary: ",
                ]
            )

            if "differences" in result and "summary" in result["differences"]:
                for diff in result["differences"]["summary"]:
                    report_lines.append(f"  - {diff}")

            # Activities-specific details
            if result.get("is_activities_file", False):
                _append_activities_details(report_lines, result)
            # Eligibility-specific details
            elif result.get("is_eligibility_file", False):
                _append_eligibility_details(report_lines, result)
            else:
                # Standard / generic / issues file details
                _append_standard_details(report_lines, result)

    report_lines.append("=" * 80)

    report_text = "\n".join(report_lines)

    # Print to console
    print(report_text)

    # Save to file if specified
    if output_file:
        output_path = Path(output_file)
        with open(output_path, "w") as f:
            f.write(report_text)
        logger.info(f"Report saved to: {output_path}")

    return report_text


# ---------------------------------------------------------------------------
# Section renderers (private)
# ---------------------------------------------------------------------------


def _append_activities_details(
    lines: list[str], result: dict[str, Any]
) -> None:
    if "differences" not in result:
        return

    diffs = result["differences"]

    if "missing_actions" in diffs:
        missing = diffs["missing_actions"]
        if missing:
            lines.append(
                f"  {len(missing)} actions in primary but NOT in replicated:"
            )
            for item in missing[:10]:
                person = item["person_info"]
                is_dep = "dependent" if item["is_dependent"] else "non-dependent"
                lines.append(
                    f"     - Row {person.get('row_number', '?')} "
                    f"({person.get('first_name', '?')} "
                    f"{person.get('last_name', '?')}, "
                    f"{person.get('born_on', '?')}, {is_dep}): "
                    f"Missing action '{item['action']}'"
                )
            if len(missing) > 10:
                lines.append(f"     ... and {len(missing) - 10} more")

    if "unexpected_actions" in diffs:
        unexpected = diffs["unexpected_actions"]
        if unexpected:
            lines.append(
                f"  {len(unexpected)} actions in replicated but NOT in primary:"
            )
            for item in unexpected[:10]:
                person = item["person_info"]
                is_dep = "dependent" if item["is_dependent"] else "non-dependent"
                lines.append(
                    f"     - Row {person.get('row_number', '?')} "
                    f"({person.get('first_name', '?')} "
                    f"{person.get('last_name', '?')}, "
                    f"{person.get('born_on', '?')}, {is_dep}): "
                    f"Unexpected action '{item['action']}'"
                )
            if len(unexpected) > 10:
                lines.append(f"     ... and {len(unexpected) - 10} more")


def _append_columns_with_diffs(
    lines: list[str], cols_with_diffs: dict[str, Any]
) -> None:
    """Render the column-level diff breakdown shared by eligibility and generic."""
    if not cols_with_diffs:
        return

    lines.append("  Columns with differences in matching records:")
    for col, diff_info in sorted(
        cols_with_diffs.items(), key=lambda x: len(x[1]["rows"]), reverse=True
    ):
        row_numbers = diff_info["rows"]
        row_nums_str = ", ".join(str(r) for r in row_numbers[:10])
        if len(row_numbers) > 10:
            row_nums_str += f", ... ({len(row_numbers) - 10} more)"

        diff_summary_parts: list[str] = []
        if diff_info["primary_has_value_replicated_empty"] > 0:
            diff_summary_parts.append(
                f"{diff_info['primary_has_value_replicated_empty']} "
                "primary has value/replicated empty"
            )
        if diff_info["primary_empty_replicated_has_value"] > 0:
            diff_summary_parts.append(
                f"{diff_info['primary_empty_replicated_has_value']} "
                "primary empty/replicated has value"
            )
        if diff_info["both_have_different_values"] > 0:
            diff_summary_parts.append(
                f"{diff_info['both_have_different_values']} differing values"
            )

        summary_str = "; ".join(diff_summary_parts)
        lines.append(
            f"     - {col}: {len(row_numbers)} records (rows: {row_nums_str})"
        )
        lines.append(f"       {summary_str}")


def _append_record_samples(
    lines: list[str],
    diffs: dict[str, Any],
    missing_key: str,
    sample_key: str,
    label: str,
) -> None:
    """Render missing/extra record counts + sample rows."""
    if missing_key in diffs:
        count = len(diffs[missing_key]) if isinstance(diffs[missing_key], list) else diffs[missing_key]
        lines.append(f"  {count} {label}")
        if sample_key in diffs:
            sample = diffs[sample_key]
            if sample:
                lines.append(f"     Sample (first {len(sample)} records):")
                for i, rec in enumerate(sample, 1):
                    lines.append(f"       {i}. {rec}")


def _append_eligibility_details(
    lines: list[str], result: dict[str, Any]
) -> None:
    if "differences" not in result:
        return

    diffs = result["differences"]

    if "columns_with_diffs" in diffs:
        _append_columns_with_diffs(lines, diffs["columns_with_diffs"])

    _append_record_samples(
        lines,
        diffs,
        "missing_in_replicated",
        "primary_only_sample",
        "records in primary but NOT in replicated",
    )
    _append_record_samples(
        lines,
        diffs,
        "extra_in_replicated",
        "replicated_only_sample",
        "records in replicated but NOT in primary",
    )

    if diffs.get("likely_time_filter_issue", False):
        lines.append(
            "  LIKELY CAUSE: Time interval filter not applied correctly "
            "in replicated query"
        )


def _append_standard_details(
    lines: list[str], result: dict[str, Any]
) -> None:
    if "differences" not in result:
        return

    diffs = result["differences"]

    if "columns_with_diffs" in diffs:
        _append_columns_with_diffs(lines, diffs["columns_with_diffs"])

    if "primary_only_count" in diffs:
        count = diffs["primary_only_count"]
        lines.append(f"  {count} records in primary but NOT in replicated")
        if "primary_only_sample" in diffs:
            sample = diffs["primary_only_sample"]
            if sample:
                lines.append(f"     Sample (first {len(sample)} records):")
                for i, rec in enumerate(sample, 1):
                    lines.append(f"       {i}. {rec}")

    if "replicated_only_count" in diffs:
        count = diffs["replicated_only_count"]
        lines.append(f"  {count} records in replicated but NOT in primary")
        if "replicated_only_sample" in diffs:
            sample = diffs["replicated_only_sample"]
            if sample:
                lines.append(f"     Sample (first {len(sample)} records):")
                for i, rec in enumerate(sample, 1):
                    lines.append(f"       {i}. {rec}")
