"""Comparison engine -- decomposed from the original compare_parity_results.py.

Public API:
    ParityComparison    — orchestrator class (engine.py)
    generate_report     — standalone report writer (report_writer.py)
    compare_eligibilities, compare_issues, compare_activities, compare_dataframes
                        — specialized / generic comparator functions
    detect_separator, normalize_text, load_and_prepare_dataframe,
    get_person_info, compare_matching_records
                        — shared primitives (base_comparator.py)
"""

from .base_comparator import (
    compare_matching_records,
    detect_separator,
    get_person_info,
    load_and_prepare_dataframe,
    normalize_text,
)
from .engine import ParityComparison
from .report_writer import generate_report

__all__ = [
    "ParityComparison",
    "generate_report",
    "compare_matching_records",
    "detect_separator",
    "get_person_info",
    "load_and_prepare_dataframe",
    "normalize_text",
]
