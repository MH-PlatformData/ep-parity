"""Tests for DatabaseManager._wrap_db_error and test_connection."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ep_parity.core.database import DatabaseManager


@pytest.fixture
def db_manager(tmp_config_dir):
    """Create a DatabaseManager backed by a temporary config."""
    from ep_parity.core.config import AppConfig

    config = AppConfig(config_dir=str(tmp_config_dir))
    return DatabaseManager(config)


# ---------------------------------------------------------------------------
# _wrap_db_error
# ---------------------------------------------------------------------------


class TestWrapDbError:
    """Verify _wrap_db_error returns actionable ConnectionError for known failures."""

    def test_host_not_found(self, db_manager):
        original = Exception("could not translate host name 'bad-host' to address")
        result = db_manager._wrap_db_error("ep15-qa", original)

        assert isinstance(result, ConnectionError)
        assert "ep15-qa" in str(result)
        assert "VPN" in str(result)

    def test_password_authentication_failed(self, db_manager):
        original = Exception('password authentication failed for user "alice"')
        result = db_manager._wrap_db_error("ep20-qa", original)

        assert isinstance(result, ConnectionError)
        assert "ep20-qa" in str(result)
        assert "credentials" in str(result).lower() or "init" in str(result).lower()

    def test_connection_refused(self, db_manager):
        original = Exception("could not connect to server: Connection refused")
        result = db_manager._wrap_db_error("ep20-dev", original)

        assert isinstance(result, ConnectionError)
        assert "ep20-dev" in str(result)
        assert "host" in str(result).lower() or "port" in str(result).lower()

    def test_timed_out(self, db_manager):
        original = Exception("connection timed out")
        result = db_manager._wrap_db_error("prod", original)

        assert isinstance(result, ConnectionError)
        assert "prod" in str(result)
        assert "VPN" in str(result) or "timed out" in str(result).lower()

    def test_unknown_error_returns_original(self, db_manager):
        original = ValueError("something completely unexpected")
        result = db_manager._wrap_db_error("ep15-qa", original)

        assert result is original
        assert isinstance(result, ValueError)

    @pytest.mark.parametrize(
        "error_msg",
        [
            "could not translate host name 'x' to address",
            "password authentication failed for user 'x'",
            "Connection refused",
            "connection timed out",
        ],
    )
    def test_all_connection_errors_include_target_and_guidance(
        self, db_manager, error_msg
    ):
        """Every wrapped ConnectionError should mention the target name."""
        result = db_manager._wrap_db_error("ep15-qa", Exception(error_msg))
        assert isinstance(result, ConnectionError)
        assert "ep15-qa" in str(result)
        # All should contain some form of actionable guidance
        msg = str(result).lower()
        assert any(
            keyword in msg
            for keyword in ["vpn", "credentials", "host", "port", "init", "check", ".env"]
        )


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


class TestTestConnection:
    """Verify test_connection returns (bool, str) tuples correctly."""

    def test_success_returns_true_with_timing(self, db_manager):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(db_manager, "get_engine", return_value=mock_engine):
            success, message = db_manager.test_connection("ep15-qa")

        assert success is True
        assert "connected" in message
        assert "ms" in message

    def test_failure_returns_false_with_wrapped_message(self, db_manager):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception(
            "could not translate host name 'bad-host' to address"
        )

        with patch.object(db_manager, "get_engine", return_value=mock_engine):
            success, message = db_manager.test_connection("ep15-qa")

        assert success is False
        assert "ep15-qa" in message
        assert "VPN" in message

    def test_failure_with_unknown_error(self, db_manager):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = RuntimeError("weird db error")

        with patch.object(db_manager, "get_engine", return_value=mock_engine):
            success, message = db_manager.test_connection("ep15-qa")

        assert success is False
        assert "weird db error" in message
