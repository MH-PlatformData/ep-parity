"""Database connection management with dual credential modes.

Mode 1 (default): Read DB URIs from .env file via AppConfig.
Mode 2 (optional): Fetch credentials from AWS Secrets Manager at runtime.
"""

import json

import pandas as pd
import sqlalchemy as sa

from ep_parity.core.config import AppConfig, DB_TARGET_ENV_VARS
from ep_parity.utils.logging import get_logger

logger = get_logger("database")


class DatabaseManager:
    """Manages SQLAlchemy engine lifecycle for parity testing databases."""

    def __init__(self, config: AppConfig):
        self._config = config
        self._engines: dict[str, sa.engine.Engine] = {}

    def get_engine(self, target: str) -> sa.engine.Engine:
        """Get or create an engine for the given DB target short code.

        Args:
            target: Internal short code ('pri', 'rep', 'dev', 'prod').
        """
        if target not in self._engines:
            uri = self._resolve_uri(target)
            self._engines[target] = sa.create_engine(uri)
            logger.debug(f"Created engine for target '{target}'")
        return self._engines[target]

    def _resolve_uri(self, target: str) -> str:
        """Resolve connection URI either from .env or AWS Secrets Manager."""
        if self._config.use_aws_secrets:
            return self._fetch_uri_from_secrets(target)
        return self._config.get_db_uri(target)

    def _fetch_uri_from_secrets(self, target: str) -> str:
        """Fetch database URI from AWS Secrets Manager."""
        import boto3

        secret_path = self._config.secret_path
        if not secret_path:
            raise ValueError(
                "aws_secret_path not set in [defaults] section of paths_config.ini"
            )

        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_path)
        secret = json.loads(response["SecretString"])

        # Build connection string from secret fields
        host = secret.get("host", "")
        port = secret.get("port", 5432)
        dbname = secret.get("dbname", "")
        username = secret.get("username", "")
        password = secret.get("password", "")

        return f"postgresql://{username}:{password}@{host}:{port}/{dbname}"

    def execute_query(
        self, target: str, query: str, params: dict | None = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Args:
            target: DB target short code.
            query: SQL query string (may contain :param placeholders).
            params: Parameter dict for parameterized queries.
        """
        try:
            engine = self.get_engine(target)
            if params:
                return pd.read_sql_query(sa.text(query), engine, params=params)
            return pd.read_sql_query(query, engine)
        except Exception as e:
            raise self._wrap_db_error(target, e) from e

    def execute_scalar(self, target: str, query: str, params: dict | None = None):
        """Execute a query returning a single row as a dict, or None."""
        try:
            engine = self.get_engine(target)
            with engine.connect() as conn:
                result = conn.execute(sa.text(query), params or {})
                row = result.fetchone()
                if row is None:
                    return None
                return dict(zip(result.keys(), row))
        except Exception as e:
            raise self._wrap_db_error(target, e) from e

    def test_connection(self, target: str) -> tuple[bool, str]:
        """Test connectivity to a database target.

        Returns:
            Tuple of (success, message). Message includes timing on success
            or actionable error guidance on failure.
        """
        import time

        try:
            engine = self.get_engine(target)
            start = time.time()
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            elapsed_ms = (time.time() - start) * 1000
            return True, f"connected ({elapsed_ms:.0f}ms)"
        except Exception as e:
            wrapped = self._wrap_db_error(target, e)
            return False, str(wrapped)

    def _wrap_db_error(self, target: str, e: Exception) -> Exception:
        """Wrap database exceptions with user-friendly, actionable guidance."""
        error_str = str(e).lower()

        if "could not translate host name" in error_str or "name or service not known" in error_str:
            return ConnectionError(
                f"Cannot reach the '{target}' database. Are you connected to VPN?\n"
                f"  Databases use *.internal.marathon-health.com hostnames that require VPN access.\n"
                f"  Original error: {e}"
            )
        if "password authentication failed" in error_str:
            return ConnectionError(
                f"Authentication failed for '{target}'. Check credentials in .env.\n"
                f"  Run 'ep-parity init' to reconfigure.\n"
                f"  Original error: {e}"
            )
        if "connection refused" in error_str:
            return ConnectionError(
                f"Connection refused for '{target}'. Check host and port in .env.\n"
                f"  Original error: {e}"
            )
        if "timeout" in error_str or "timed out" in error_str:
            return ConnectionError(
                f"Connection timed out for '{target}'. Are you connected to VPN?\n"
                f"  Original error: {e}"
            )
        return e

    def dispose_all(self) -> None:
        """Dispose of all engine connections."""
        for target, engine in self._engines.items():
            engine.dispose()
            logger.debug(f"Disposed engine for target '{target}'")
        self._engines.clear()
