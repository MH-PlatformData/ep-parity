"""Tests for ep_parity.cli.init_cmd — _build_uri, _write_env_file, _write_paths_config, and init command."""

import os
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from ep_parity.cli.init_cmd import _build_uri, _write_env_file, _write_paths_config


# ---------------------------------------------------------------------------
# _build_uri
# ---------------------------------------------------------------------------


class TestBuildUri:
    """Verify _build_uri URL-encodes special characters and builds valid URIs."""

    def test_basic_credentials(self):
        uri = _build_uri("alice", "secret", "db-host.example.com", "portal_qa")
        assert uri == "postgresql://alice:secret@db-host.example.com:5432/portal_qa"

    def test_encodes_at_sign_in_password(self):
        uri = _build_uri("alice", "p@ss", "host", "db")
        assert "p%40ss" in uri
        # Should still be a valid postgresql:// URI
        assert uri.startswith("postgresql://")
        assert "@host:5432/db" in uri

    def test_encodes_hash_in_password(self):
        uri = _build_uri("alice", "p#ss", "host", "db")
        assert "p%23ss" in uri

    def test_encodes_percent_in_password(self):
        uri = _build_uri("alice", "100%done", "host", "db")
        assert "100%25done" in uri

    def test_encodes_spaces_in_password(self):
        uri = _build_uri("alice", "my pass", "host", "db")
        assert "my+pass" in uri or "my%20pass" in uri

    def test_encodes_special_chars_in_username(self):
        uri = _build_uri("user@corp", "pass", "host", "db")
        assert "user%40corp" in uri

    def test_correct_postgresql_format(self):
        uri = _build_uri("u", "p", "h", "d")
        assert uri == "postgresql://u:p@h:5432/d"

    def test_complex_password(self):
        """Password with multiple special characters: P@ss#w0rd%!"""
        uri = _build_uri("admin", "P@ss#w0rd%!", "prod-db.internal", "portal")
        # Verify the URI is parseable — special chars should be encoded
        assert "P%40ss%23w0rd%25%21" in uri
        assert "@prod-db.internal:5432/portal" in uri


# ---------------------------------------------------------------------------
# _write_env_file
# ---------------------------------------------------------------------------


class TestWriteEnvFile:
    """Verify _write_env_file produces valid .env content with per-environment credentials."""

    def test_creates_env_file_with_qa_credentials(self, tmp_path):
        output = tmp_path / ".env"
        env_credentials = {"qa": ("alice", "secret")}
        _write_env_file(output, env_credentials)

        assert output.exists()
        content = output.read_text()
        assert "DB_EP15_QA_URI=" in content
        assert "DB_EP20_QA_URI=" in content
        assert "alice" in content

    def test_unconfigured_environments_are_commented_out(self, tmp_path):
        output = tmp_path / ".env"
        env_credentials = {"qa": ("alice", "secret")}
        _write_env_file(output, env_credentials)

        content = output.read_text()
        # Dev and Prod should be commented out
        assert "# DB_EP15_DEV_URI=" in content
        assert "# DB_EP20_DEV_URI=" in content
        assert "# DB_PRODUCTION_URI=" in content

    def test_all_environments_configured(self, tmp_path):
        output = tmp_path / ".env"
        env_credentials = {
            "dev": ("dev_user", "dev_pass"),
            "qa": ("qa_user", "qa_pass"),
            "prod": ("prod_user", "prod_pass"),
        }
        _write_env_file(output, env_credentials)

        content = output.read_text()
        assert "DB_EP15_DEV_URI=" in content
        assert "DB_EP20_DEV_URI=" in content
        assert "DB_EP15_QA_URI=" in content
        assert "DB_EP20_QA_URI=" in content
        assert "DB_PRODUCTION_URI=" in content
        # None should be commented out
        assert "# DB_EP15_DEV_URI=" not in content

    def test_includes_never_commit_warning(self, tmp_path):
        output = tmp_path / ".env"
        _write_env_file(output, {"qa": ("u", "p")})

        content = output.read_text()
        assert "NEVER commit" in content

    def test_ep20_qa_uses_intentional_hostname_misspelling(self, tmp_path):
        """EP 2.0 QA uses 'parevida' (not 'pariveda') — this is intentional."""
        output = tmp_path / ".env"
        _write_env_file(output, {"qa": ("u", "p")})

        content = output.read_text()
        assert "parevida" in content


# ---------------------------------------------------------------------------
# _write_paths_config
# ---------------------------------------------------------------------------


class TestWritePathsConfig:
    """Verify _write_paths_config produces valid paths_config.ini content."""

    def test_creates_ini_file(self, tmp_path):
        output = tmp_path / "paths_config.ini"
        _write_paths_config(output, "/output/results", "/output/sql", "ep15-qa ep20-qa")

        assert output.exists()
        content = output.read_text()
        assert "[paths]" in content
        assert "base_path = /output/results" in content
        assert "sql_directory = /output/sql" in content

    def test_includes_defaults_section(self, tmp_path):
        output = tmp_path / "paths_config.ini"
        _write_paths_config(output, "/a", "/b", "ep15-qa")

        content = output.read_text()
        assert "[defaults]" in content
        assert "db_target = ep15-qa" in content

    def test_includes_output_section(self, tmp_path):
        output = tmp_path / "paths_config.ini"
        _write_paths_config(output, "/a", "/b", "ep15-qa ep20-qa")

        content = output.read_text()
        assert "[output]" in content
        assert "directory_format" in content
        assert "date_format" in content

    def test_parseable_by_configparser(self, tmp_path):
        """The generated file should be parseable by configparser."""
        import configparser

        output = tmp_path / "paths_config.ini"
        _write_paths_config(output, "/my/results", "/my/sql", "ep15-qa ep20-qa")

        config = configparser.ConfigParser()
        config.read(output)
        assert config.get("paths", "base_path") == "/my/results"
        assert config.get("paths", "sql_directory") == "/my/sql"
        assert config.get("defaults", "db_target") == "ep15-qa ep20-qa"


# ---------------------------------------------------------------------------
# init command — non-interactive mode
# ---------------------------------------------------------------------------


class TestInitNonInteractive:
    """Test the init command in --non-interactive mode."""

    def test_reads_from_env_vars(self, tmp_path):
        """Non-interactive mode should use EP_INIT_{ENV}_USER/PASS environment variables."""
        from ep_parity.cli.main import cli

        runner = CliRunner()
        env = {
            "EP_INIT_QA_USER": "testuser",
            "EP_INIT_QA_PASS": "testpass",
            "EP_INIT_BASE_PATH": str(tmp_path / "results"),
            "EP_INIT_SQL_DIR": str(tmp_path / "sql"),
        }
        with patch.dict(os.environ, env, clear=False):
            result = runner.invoke(
                cli,
                ["--config-dir", str(tmp_path), "init", "--non-interactive"],
                catch_exceptions=False,
            )

        # Should complete without error
        assert result.exit_code == 0, f"Command failed:\n{result.output}"
        # Should create .env and paths_config.ini
        assert (tmp_path / ".env").exists()
        assert (tmp_path / "paths_config.ini").exists()

        # Verify .env contains the expected URIs
        env_content = (tmp_path / ".env").read_text()
        assert "DB_EP15_QA_URI=" in env_content
        assert "DB_EP20_QA_URI=" in env_content
        assert "testuser" in env_content

    def test_fails_when_env_vars_missing(self, tmp_path):
        """Non-interactive mode should fail when no EP_INIT_*_USER/PASS are set."""
        from ep_parity.cli.main import cli

        runner = CliRunner()

        # Clear the relevant env vars
        clean_env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("EP_INIT_")
        }
        with patch.dict(os.environ, clean_env, clear=True):
            result = runner.invoke(
                cli,
                ["--config-dir", str(tmp_path), "init", "--non-interactive"],
            )

        # Should exit with non-zero status
        assert result.exit_code != 0
        assert "FAIL" in result.output or isinstance(result.exception, SystemExit)
