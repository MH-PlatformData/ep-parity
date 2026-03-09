"""Tests for ep_parity.cli.config_cmd — config show command and _mask_uri."""

import os
from unittest.mock import patch

from click.testing import CliRunner

from ep_parity.cli.config_cmd import _mask_uri, config
from ep_parity.cli.main import cli


# ---------------------------------------------------------------------------
# _mask_uri
# ---------------------------------------------------------------------------


class TestMaskUri:
    """Verify _mask_uri hides passwords in postgresql:// URIs."""

    def test_masks_password(self):
        uri = "postgresql://alice:s3cret_P@ss@db-host:5432/mydb"
        masked = _mask_uri(uri)
        assert "s3cret_P@ss" not in masked
        assert "alice:****@" in masked
        assert "db-host:5432/mydb" in masked

    def test_preserves_scheme_and_host(self):
        uri = "postgresql://user:pass@host.example.com:5432/portal_qa"
        masked = _mask_uri(uri)
        assert masked.startswith("postgresql://")
        assert "host.example.com:5432/portal_qa" in masked

    def test_handles_uri_without_password(self):
        """URIs without user:pass@ should pass through unchanged."""
        uri = "postgresql://host.example.com:5432/mydb"
        masked = _mask_uri(uri)
        # No match means no substitution — returned as-is
        assert masked == uri

    def test_handles_empty_string(self):
        assert _mask_uri("") == ""

    def test_handles_encoded_special_chars_in_password(self):
        uri = "postgresql://alice:p%40ss%23word@host:5432/db"
        masked = _mask_uri(uri)
        assert "p%40ss%23word" not in masked
        assert "****" in masked


# ---------------------------------------------------------------------------
# config show command
# ---------------------------------------------------------------------------


class TestConfigShowCommand:
    """Test the 'config show' CLI subcommand via CliRunner."""

    def test_runs_without_error(self, tmp_config_dir):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config-dir", str(tmp_config_dir), "config", "show"],
        )
        assert result.exit_code == 0, f"Command failed:\n{result.output}"
        assert "EP Parity Configuration" in result.output

    def test_displays_database_targets(self, tmp_config_dir):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config-dir", str(tmp_config_dir), "config", "show"],
        )
        assert result.exit_code == 0
        # The tmp_config_dir fixture sets DB_PRIMARY_URI and DB_REPLICATED_URI
        # via its .env file, so they should appear (masked)
        assert "Database Targets:" in result.output

    def test_displays_paths(self, tmp_config_dir):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config-dir", str(tmp_config_dir), "config", "show"],
        )
        assert result.exit_code == 0
        assert "Paths:" in result.output
        assert "base_path:" in result.output
        assert "sql_directory:" in result.output

    def test_displays_not_configured_for_missing_db_uris(self, tmp_path):
        """When .env has no DB URIs, show '(not configured)' for each target."""
        # Create minimal config files with no DB URIs
        (tmp_path / ".env").write_text("")
        (tmp_path / "paths_config.ini").write_text(
            "[paths]\nbase_path = /tmp\nsql_directory = /tmp\n"
        )

        runner = CliRunner()
        # Clear DB env vars that might bleed from the test environment
        clean_env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("DB_")
        }
        with patch.dict(os.environ, clean_env, clear=True):
            result = runner.invoke(
                cli,
                ["--config-dir", str(tmp_path), "config", "show"],
            )
        assert result.exit_code == 0
        assert "not configured" in result.output

    def test_displays_comparison_settings(self, tmp_config_dir):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config-dir", str(tmp_config_dir), "config", "show"],
        )
        assert result.exit_code == 0
        assert "Comparison Settings:" in result.output
        assert "case_sensitive:" in result.output
