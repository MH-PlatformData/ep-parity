"""Tests for ep_parity.core.comparison.base_comparator — primitives."""

import math

import pandas as pd
import pytest

from ep_parity.core.comparison.base_comparator import (
    compare_matching_records,
    detect_separator,
    normalize_text,
)


# ---------------------------------------------------------------------------
# detect_separator
# ---------------------------------------------------------------------------


class TestDetectSeparator:
    def test_pipe_delimited(self, tmp_path):
        f = tmp_path / "data.psv"
        f.write_text("col_a|col_b|col_c\n1|2|3\n")
        assert detect_separator(f) == "|"

    def test_comma_delimited(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("col_a,col_b,col_c\n1,2,3\n")
        assert detect_separator(f) == ","

    def test_no_clear_separator_csv_extension(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("single_column\nvalue\n")
        assert detect_separator(f) == ","

    def test_no_clear_separator_psv_extension(self, tmp_path):
        f = tmp_path / "data.psv"
        f.write_text("single_column\nvalue\n")
        assert detect_separator(f) == "|"

    def test_pipe_wins_when_both_present(self, tmp_path):
        """When the header has pipes, pipe is chosen regardless of commas."""
        f = tmp_path / "data.psv"
        f.write_text("col_a|col_b,extra|col_c\n1|2|3\n")
        assert detect_separator(f) == "|"

    def test_empty_file_falls_back(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("")
        # Empty header -> no pipes, no commas -> extension fallback
        assert detect_separator(f) == ","


# ---------------------------------------------------------------------------
# normalize_text
# ---------------------------------------------------------------------------


class TestNormalizeText:
    def test_none_returns_empty(self):
        assert normalize_text(None) == ""

    def test_nan_returns_empty(self):
        assert normalize_text(float("nan")) == ""

    def test_empty_string_returns_empty(self):
        assert normalize_text("") == ""

    def test_whitespace_only_returns_empty(self):
        assert normalize_text("   ") == ""

    def test_mixed_case_lowered(self):
        assert normalize_text("Hello World") == "hello world"

    def test_extra_whitespace_collapsed(self):
        assert normalize_text("  hello   world  ") == "hello world"

    def test_underscores_replaced(self):
        assert normalize_text("hello_world") == "hello world"

    def test_hyphens_replaced(self):
        assert normalize_text("hello-world") == "hello world"

    def test_normalize_none_string_false(self):
        result = normalize_text("None value", normalize_none_string=False)
        assert "none" in result

    def test_normalize_none_string_true_removes_none(self):
        result = normalize_text("None value", normalize_none_string=True)
        assert "none" not in result
        assert "value" in result

    def test_normalize_none_string_middle(self):
        result = normalize_text("before None after", normalize_none_string=True)
        assert "none" not in result.lower()
        # Remaining text should be present
        assert "before" in result
        assert "after" in result

    def test_pandas_nat(self):
        assert normalize_text(pd.NaT) == ""


# ---------------------------------------------------------------------------
# compare_matching_records
# ---------------------------------------------------------------------------


class TestCompareMatchingRecords:
    def test_identical_records_no_diffs(self):
        df1 = pd.DataFrame(
            {"key": ["a", "b"], "val": ["1", "2"]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a", "b"], "val": ["1", "2"]}
        ).set_index("key")

        records_with_diffs, cols_with_diffs = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a", "b"}
        )
        assert records_with_diffs == 0
        assert cols_with_diffs == {}

    def test_differing_records_detected(self):
        df1 = pd.DataFrame(
            {"key": ["a", "b"], "val": ["1", "2"]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a", "b"], "val": ["1", "CHANGED"]}
        ).set_index("key")

        records_with_diffs, cols_with_diffs = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a", "b"}
        )
        assert records_with_diffs == 1
        assert "val" in cols_with_diffs
        assert cols_with_diffs["val"]["both_have_different_values"] == 1

    def test_primary_has_value_replicated_empty(self):
        df1 = pd.DataFrame(
            {"key": ["a"], "val": ["hello"]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a"], "val": [""]}
        ).set_index("key")

        _, cols_with_diffs = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a"}
        )
        assert cols_with_diffs["val"]["primary_has_value_replicated_empty"] == 1

    def test_primary_empty_replicated_has_value(self):
        df1 = pd.DataFrame(
            {"key": ["a"], "val": [""]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a"], "val": ["hello"]}
        ).set_index("key")

        _, cols_with_diffs = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a"}
        )
        assert cols_with_diffs["val"]["primary_empty_replicated_has_value"] == 1

    def test_both_nan_counts_as_equal(self):
        df1 = pd.DataFrame(
            {"key": ["a"], "val": [float("nan")]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a"], "val": [float("nan")]}
        ).set_index("key")

        records_with_diffs, _ = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a"}
        )
        assert records_with_diffs == 0

    def test_with_normalize_fn(self):
        df1 = pd.DataFrame(
            {"key": ["a"], "val": ["Hello"]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a"], "val": ["hello"]}
        ).set_index("key")

        # Without normalization -> different
        diffs_raw, _ = compare_matching_records(
            df1, df2, compare_cols=["val"], common_keys={"a"}
        )
        assert diffs_raw == 1

        # With normalization -> same
        diffs_norm, _ = compare_matching_records(
            df1, df2,
            compare_cols=["val"],
            common_keys={"a"},
            normalize_fn=normalize_text,
        )
        assert diffs_norm == 0

    def test_missing_key_skipped(self):
        """Keys in common_keys but missing from an index are silently skipped."""
        df1 = pd.DataFrame(
            {"key": ["a"], "val": ["1"]}
        ).set_index("key")
        df2 = pd.DataFrame(
            {"key": ["a"], "val": ["1"]}
        ).set_index("key")

        records_with_diffs, _ = compare_matching_records(
            df1, df2,
            compare_cols=["val"],
            common_keys={"a", "missing_key"},
        )
        assert records_with_diffs == 0
