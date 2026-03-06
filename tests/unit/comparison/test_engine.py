"""Tests for ep_parity.core.comparison.engine — ParityComparison orchestrator."""

import textwrap

import pytest

from ep_parity.core.comparison.engine import ParityComparison
from ep_parity.core.config import AppConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(tmp_path):
    """Create a minimal AppConfig pointing at tmp_path."""
    paths_ini = tmp_path / "paths_config.ini"
    paths_ini.write_text(
        textwrap.dedent("""\
            [paths]
            base_path = {base_path}
            sql_directory = {sql_dir}
        """).format(
            base_path=str(tmp_path / "results"),
            sql_dir=str(tmp_path / "sql"),
        )
    )
    comp_ini = tmp_path / "comparison_config.ini"
    comp_ini.write_text(
        textwrap.dedent("""\
            [exclude_files]
            excluded.psv = true

            [global_ignore_columns]
            updated_at = true
        """)
    )
    (tmp_path / ".env").write_text("")
    (tmp_path / "results").mkdir(exist_ok=True)
    (tmp_path / "sql").mkdir(exist_ok=True)
    return AppConfig(config_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# get_files_to_compare
# ---------------------------------------------------------------------------


class TestGetFilesToCompare:
    def test_common_files_returned(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        # Files in both directories
        (pri_dir / "a.psv").write_text("h\n")
        (rep_dir / "a.psv").write_text("h\n")
        (pri_dir / "b.psv").write_text("h\n")
        (rep_dir / "b.psv").write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert sorted(files) == ["a.psv", "b.psv"]

    def test_excluded_files_filtered(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        (pri_dir / "good.psv").write_text("h\n")
        (rep_dir / "good.psv").write_text("h\n")
        (pri_dir / "excluded.psv").write_text("h\n")
        (rep_dir / "excluded.psv").write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert "excluded.psv" not in files
        assert "good.psv" in files

    def test_primary_only_files_not_included(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        (pri_dir / "common.psv").write_text("h\n")
        (rep_dir / "common.psv").write_text("h\n")
        (pri_dir / "primary_only.psv").write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert "primary_only.psv" not in files
        assert "common.psv" in files

    def test_replicated_only_files_not_included(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        (pri_dir / "common.psv").write_text("h\n")
        (rep_dir / "common.psv").write_text("h\n")
        (rep_dir / "rep_only.psv").write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert "rep_only.psv" not in files

    def test_results_sorted(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        for name in ["c.psv", "a.psv", "b.psv"]:
            (pri_dir / name).write_text("h\n")
            (rep_dir / name).write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert files == sorted(files)

    def test_exclude_files_case_insensitive(self, tmp_path):
        config = _make_config(tmp_path)
        pri_dir = tmp_path / "primary"
        rep_dir = tmp_path / "replicated"
        pri_dir.mkdir()
        rep_dir.mkdir()

        # The config has "excluded.psv" = true, test with different casing
        (pri_dir / "EXCLUDED.psv").write_text("h\n")
        (rep_dir / "EXCLUDED.psv").write_text("h\n")

        comparison = ParityComparison(config=config, employer_id="123")
        files = comparison.get_files_to_compare(pri_dir, rep_dir)
        assert "EXCLUDED.psv" not in files


# ---------------------------------------------------------------------------
# find_run_directory
# ---------------------------------------------------------------------------


class TestFindRunDirectory:
    def test_finds_most_recent_directory(self, tmp_path):
        config = _make_config(tmp_path)
        results_dir = tmp_path / "results"

        # Create two date folders with matching employer directories
        date1 = results_dir / "01-01-2025"
        date1.mkdir(parents=True)
        old_dir = date1 / "555 01-01-25 0800"
        old_dir.mkdir()

        date2 = results_dir / "02-15-2025"
        date2.mkdir(parents=True)
        new_dir = date2 / "555 02-15-25 1200"
        new_dir.mkdir()

        comparison = ParityComparison(config=config, employer_id="555")
        found = comparison.find_run_directory()
        assert found == new_dir

    def test_finds_directory_with_timestamp(self, tmp_path):
        config = _make_config(tmp_path)
        results_dir = tmp_path / "results"

        date1 = results_dir / "03-01-2025"
        date1.mkdir(parents=True)
        target_dir = date1 / "777 03-01-25 1400"
        target_dir.mkdir()
        other_dir = date1 / "777 03-01-25 0900"
        other_dir.mkdir()

        comparison = ParityComparison(
            config=config, employer_id="777", run_timestamp="1400"
        )
        found = comparison.find_run_directory()
        assert found == target_dir

    def test_raises_when_no_match(self, tmp_path):
        config = _make_config(tmp_path)
        results_dir = tmp_path / "results"
        results_dir.mkdir(exist_ok=True)

        comparison = ParityComparison(config=config, employer_id="99999")
        with pytest.raises(FileNotFoundError, match="No run directories found"):
            comparison.find_run_directory()

    def test_raises_when_results_empty(self, tmp_path):
        config = _make_config(tmp_path)
        # results dir exists but is empty
        comparison = ParityComparison(config=config, employer_id="123")
        with pytest.raises(FileNotFoundError):
            comparison.find_run_directory()
