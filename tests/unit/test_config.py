"""Tests for ep_parity.core.config — AppConfig, resolve_db_target, helpers."""

import textwrap

import pytest

from ep_parity.core.config import (
    DB_TARGET_ALIASES,
    DB_TARGET_FOLDER_NAMES,
    AppConfig,
    load_employer_ids_from_file,
    resolve_db_target,
)


# ---------------------------------------------------------------------------
# AppConfig — paths_config.ini loading
# ---------------------------------------------------------------------------


class TestAppConfigPathsLoading:
    """Verify AppConfig reads [paths] and [defaults] sections correctly."""

    def test_base_path_from_ini(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.base_path == tmp_config_dir / "results"

    def test_sql_directory_from_ini(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.sql_directory == tmp_config_dir / "sql"

    def test_defaults_section_loaded(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.get_default("db_target") == "both"
        assert cfg.get_default("env") == "qa"

    def test_get_default_fallback(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.get_default("nonexistent", "fallback_val") == "fallback_val"

    def test_directory_format_from_ini(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.directory_format == "{emp_id} {date} {time}"

    def test_date_format_from_ini(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.date_format == "%m-%d-%y %H%M"  # configparser unescapes %% -> %

    def test_use_aws_secrets_false_by_default(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.use_aws_secrets is False

    def test_missing_paths_config_does_not_raise(self, tmp_path):
        """AppConfig should warn but not crash when paths_config.ini is missing."""
        (tmp_path / ".env").write_text("# empty")
        cfg = AppConfig(config_dir=str(tmp_path))
        # Accessing base_path should now raise because it was never configured
        with pytest.raises(ValueError, match="base_path not configured"):
            _ = cfg.base_path


# ---------------------------------------------------------------------------
# AppConfig — comparison_config.ini loading
# ---------------------------------------------------------------------------


class TestAppConfigComparisonLoading:
    """Verify comparison config properties are parsed correctly."""

    def test_global_ignore_columns(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        ignore = cfg.global_ignore_columns
        assert "updated_at" in ignore
        assert "created_at" in ignore
        assert "id" in ignore

    def test_exclude_files(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        excluded = cfg.exclude_files
        assert "2a-deposited_file_rows.psv" in excluded
        assert "3-cleaned_datasets.psv" in excluded

    def test_file_specific_ignore_columns(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        cols = cfg.get_file_specific_ignore_columns("1-deposited_files.psv")
        assert "ended_at" in cols
        assert "scan_started_at" in cols

    def test_file_specific_ignore_columns_missing_file(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.get_file_specific_ignore_columns("nonexistent.psv") == []

    def test_normalize_columns(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        cols = cfg.get_normalize_columns("9-issues-potentials.psv")
        assert "description" in cols

    def test_normalize_none_string_columns(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        cols = cfg.get_normalize_none_string_columns(
            "12-pg_search_docs-potentials.psv"
        )
        assert "content" in cols

    def test_max_sample_differences(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.max_sample_differences == 5

    def test_max_unique_rows_display(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.max_unique_rows_display == 10

    def test_case_sensitive_comparison(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.case_sensitive_comparison is False

    def test_empty_comparison_config(self, tmp_path):
        """When comparison_config.ini is absent, defaults should apply."""
        (tmp_path / ".env").write_text("")
        paths_ini = tmp_path / "paths_config.ini"
        paths_ini.write_text("[paths]\nbase_path = /tmp\nsql_directory = /tmp\n")
        cfg = AppConfig(config_dir=str(tmp_path))
        assert cfg.global_ignore_columns == []
        assert cfg.exclude_files == []
        assert cfg.max_sample_differences == 5  # fallback default


# ---------------------------------------------------------------------------
# resolve_db_target
# ---------------------------------------------------------------------------


class TestResolveDbTarget:
    """Test resolve_db_target with all alias variants and invalid input."""

    @pytest.mark.parametrize(
        "alias,expected",
        [
            ("pri", "pri"),
            ("primary", "pri"),
            ("rep", "rep"),
            ("replicated", "rep"),
            ("both", "both"),
            ("prod", "prod"),
            ("production", "prod"),
            ("dev", "dev"),
            ("pariveda-dev", "dev"),
        ],
    )
    def test_valid_aliases(self, alias, expected):
        assert resolve_db_target(alias) == expected

    def test_case_insensitive(self):
        assert resolve_db_target("PRIMARY") == "pri"
        assert resolve_db_target("Prod") == "prod"

    def test_strips_whitespace(self):
        assert resolve_db_target("  pri  ") == "pri"

    def test_invalid_input_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown db_target"):
            resolve_db_target("invalid_target")


# ---------------------------------------------------------------------------
# load_employer_ids_from_file
# ---------------------------------------------------------------------------


class TestLoadEmployerIdsFromFile:
    def test_basic_ids(self, tmp_path):
        f = tmp_path / "ids.txt"
        f.write_text("111\n222\n333\n")
        assert load_employer_ids_from_file(str(f)) == ["111", "222", "333"]

    def test_comments_and_blank_lines(self, tmp_path):
        f = tmp_path / "ids.txt"
        f.write_text(
            textwrap.dedent("""\
                # This is a comment
                111

                # Another comment
                222
                333
            """)
        )
        assert load_employer_ids_from_file(str(f)) == ["111", "222", "333"]

    def test_whitespace_stripped(self, tmp_path):
        f = tmp_path / "ids.txt"
        f.write_text("  111  \n  222  \n")
        assert load_employer_ids_from_file(str(f)) == ["111", "222"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "ids.txt"
        f.write_text("")
        assert load_employer_ids_from_file(str(f)) == []


# ---------------------------------------------------------------------------
# get_db_targets
# ---------------------------------------------------------------------------


class TestGetDbTargets:
    def test_both_expands(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.get_db_targets("both") == ["pri", "rep"]

    def test_single_target(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        assert cfg.get_db_targets("primary") == ["pri"]
        assert cfg.get_db_targets("rep") == ["rep"]

    def test_invalid_target_raises(self, tmp_config_dir):
        cfg = AppConfig(config_dir=str(tmp_config_dir))
        with pytest.raises(ValueError):
            cfg.get_db_targets("bad_target")


# ---------------------------------------------------------------------------
# DB_TARGET_ALIASES and DB_TARGET_FOLDER_NAMES mappings
# ---------------------------------------------------------------------------


class TestMappingConstants:
    def test_aliases_contain_expected_keys(self):
        expected_keys = {
            "pri", "primary", "rep", "replicated", "both",
            "dev", "pariveda-dev", "prod", "production",
        }
        assert expected_keys == set(DB_TARGET_ALIASES.keys())

    def test_folder_names_contain_expected_keys(self):
        expected_keys = {"pri", "rep", "dev", "prod"}
        assert expected_keys == set(DB_TARGET_FOLDER_NAMES.keys())

    def test_folder_names_values_are_strings(self):
        for key, value in DB_TARGET_FOLDER_NAMES.items():
            assert isinstance(value, str), f"Folder name for '{key}' is not a string"
