"""Tests for ep_parity.core.exporter — SQL formatting and output directories."""

import datetime
import textwrap
from unittest.mock import patch

import pytest

from ep_parity.core.config import AppConfig
from ep_parity.core.exporter import build_output_directory, read_and_format_sql_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(tmp_path):
    """Create a minimal AppConfig for exporter tests."""
    paths_ini = tmp_path / "paths_config.ini"
    paths_ini.write_text(
        textwrap.dedent("""\
            [paths]
            base_path = {base_path}
            sql_directory = {sql_dir}

            [output]
            directory_format = {{emp_id}} {{date}} {{time}}
            date_format = %%m-%%d-%%y %%H%%M
        """).format(
            base_path=str(tmp_path / "results"),
            sql_dir=str(tmp_path / "sql"),
        )
    )
    (tmp_path / ".env").write_text("")
    (tmp_path / "results").mkdir(exist_ok=True)
    (tmp_path / "sql").mkdir(exist_ok=True)
    return AppConfig(config_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# read_and_format_sql_file
# ---------------------------------------------------------------------------


class TestReadAndFormatSqlFile:
    def test_employer_id_replaced(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text(
            "SELECT * FROM users WHERE employer_id = {{employer_id}};"
        )
        result = read_and_format_sql_file(sql_file, emp_id="42")
        assert "{{employer_id}}" not in result
        assert "employer_id = 42" in result

    def test_time_interval_replaced(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text(
            "SELECT * FROM logs WHERE age < interval '{{time_interval}}';"
        )
        created = datetime.datetime(
            2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        now = datetime.datetime(
            2025, 1, 1, 14, 30, 0, tzinfo=datetime.timezone.utc
        )
        result = read_and_format_sql_file(
            sql_file, emp_id="99", created_at=created, now=now
        )
        assert "{{time_interval}}" not in result
        # 2.5 hours = 150 minutes
        assert "150 minutes" in result

    def test_time_interval_not_replaced_when_no_timestamps(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text(
            "SELECT * WHERE interval = '{{time_interval}}';"
        )
        result = read_and_format_sql_file(sql_file, emp_id="1")
        # Placeholder should remain because created_at/now were not supplied
        assert "{{time_interval}}" in result

    def test_multiple_placeholders(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text(
            "SELECT {{employer_id}} FROM t WHERE emp = {{employer_id}};"
        )
        result = read_and_format_sql_file(sql_file, emp_id="55")
        assert result.count("55") == 2
        assert "{{employer_id}}" not in result

    def test_no_placeholders(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1;")
        result = read_and_format_sql_file(sql_file, emp_id="1")
        assert result == "SELECT 1;"


# ---------------------------------------------------------------------------
# build_output_directory
# ---------------------------------------------------------------------------


class TestBuildOutputDirectory:
    @patch("ep_parity.core.exporter.datetime")
    def test_creates_correct_path_structure(self, mock_dt, tmp_path):
        config = _make_config(tmp_path)

        fixed_now = datetime.datetime(2025, 3, 15, 14, 30, 0)
        mock_dt.datetime.now.return_value = fixed_now
        # Ensure strftime works on real datetime
        mock_dt.side_effect = lambda *args, **kwargs: datetime.datetime(*args, **kwargs)

        output = build_output_directory(config, emp_id="42")

        # The date folder should use %m-%d-%Y format
        assert "03-15-2025" in str(output)
        # The run folder should contain the employer id
        assert "42" in output.name
        # Directory should exist
        assert output.exists()
        assert output.is_dir()

    @patch("ep_parity.core.exporter.datetime")
    def test_directory_name_format(self, mock_dt, tmp_path):
        config = _make_config(tmp_path)

        fixed_now = datetime.datetime(2025, 7, 4, 9, 5, 0)
        mock_dt.datetime.now.return_value = fixed_now

        output = build_output_directory(config, emp_id="100")

        # Run folder should match "{emp_id} {date} {time}" pattern
        # With date_format = %m-%d-%y %H%M -> date=07-04-25, time=0905
        assert "100" in output.name
        assert "07-04-25" in output.name
        assert "0905" in output.name

    @patch("ep_parity.core.exporter.datetime")
    def test_creates_parent_directories(self, mock_dt, tmp_path):
        config = _make_config(tmp_path)

        fixed_now = datetime.datetime(2025, 12, 25, 8, 0, 0)
        mock_dt.datetime.now.return_value = fixed_now

        output = build_output_directory(config, emp_id="999")

        # The intermediate date directory should also exist
        date_dir = output.parent
        assert date_dir.exists()
        assert "12-25-2025" in date_dir.name
