"""Shared fixtures for ep-parity unit tests."""

import configparser
import textwrap

import pandas as pd
import pytest


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Create a temporary directory with paths_config.ini, comparison_config.ini, and .env.

    Returns the tmp_path so tests can reference files within it.
    """
    # paths_config.ini
    paths_ini = tmp_path / "paths_config.ini"
    paths_ini.write_text(
        textwrap.dedent("""\
            [paths]
            base_path = {base_path}
            sql_directory = {sql_directory}

            [output]
            directory_format = {{emp_id}} {{date}} {{time}}
            date_format = %%m-%%d-%%y %%H%%M

            [defaults]
            db_target = both
            env = qa
            use_aws_secrets = false
        """).format(
            base_path=str(tmp_path / "results"),
            sql_directory=str(tmp_path / "sql"),
        )
    )

    # comparison_config.ini
    comp_ini = tmp_path / "comparison_config.ini"
    comp_ini.write_text(
        textwrap.dedent("""\
            [global_ignore_columns]
            updated_at = true
            created_at = true
            id = true

            [file_specific_ignore_columns]
            1-deposited_files.psv = ended_at,scan_started_at

            [exclude_files]
            2a-deposited_file_rows.psv = true
            3-cleaned_datasets.psv = true

            [normalize_columns]
            9-issues-potentials.psv = description

            [normalize_none_string_columns]
            12-pg_search_docs-potentials.psv = content

            [comparison_settings]
            max_sample_differences = 5
            max_unique_rows_display = 10
            case_sensitive_comparison = false
        """)
    )

    # .env file
    env_file = tmp_path / ".env"
    env_file.write_text(
        textwrap.dedent("""\
            DB_EP15_QA_URI=postgresql://user:pass@primary-host:5432/portal_qa
            DB_EP20_QA_URI=postgresql://user:pass@replicated-host:5432/portal_qa
        """)
    )

    # Create supporting directories
    (tmp_path / "results").mkdir()
    (tmp_path / "sql").mkdir()

    return tmp_path


@pytest.fixture
def sample_dataframe_pair():
    """Return a pair of DataFrames suitable for comparison tests.

    The DataFrames share most data but have controlled differences in
    specific columns to exercise diff-detection logic.
    """
    df1 = pd.DataFrame(
        {
            "user_id": ["1", "2", "3", "4"],
            "first_name": ["Alice", "Bob", "Charlie", "Diana"],
            "last_name": ["Smith", "Jones", "Brown", "White"],
            "status": ["active", "active", "inactive", "active"],
        }
    )
    df2 = pd.DataFrame(
        {
            "user_id": ["1", "2", "3", "4"],
            "first_name": ["Alice", "Bob", "Charlie", "Diana"],
            "last_name": ["Smith", "Jones", "Brown", "White"],
            "status": ["active", "inactive", "inactive", "active"],
        }
    )
    return df1, df2


@pytest.fixture
def identical_dataframe_pair():
    """Return two identical DataFrames for matching-record tests."""
    data = {
        "user_id": ["10", "20", "30"],
        "name": ["Foo", "Bar", "Baz"],
        "value": ["100", "200", "300"],
    }
    return pd.DataFrame(data), pd.DataFrame(data)
