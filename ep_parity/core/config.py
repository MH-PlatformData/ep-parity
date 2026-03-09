"""Unified configuration loader for all EP Parity modules.

Replaces duplicated load_path_config() / load_config() implementations across
parity_testing_args.py, compare_parity_results.py, monitor_dual_processors.py,
and monitor_and_test_parity.py.

Config search order: explicit path > PARITY_CONFIG_DIR env var > CWD > package dir.
"""

import configparser
import os
from pathlib import Path

from dotenv import load_dotenv

from ep_parity.utils.logging import get_logger

logger = get_logger("config")

# Canonical alias mapping: friendly names -> internal short codes
DB_TARGET_ALIASES: dict[str, str] = {
    "pri": "pri",
    "primary": "pri",
    "rep": "rep",
    "replicated": "rep",
    "both": "both",
    "dev": "dev",
    "pariveda-dev": "dev",
    "prod": "prod",
    "production": "prod",
}

# Maps internal short codes to .env variable names
DB_TARGET_ENV_VARS: dict[str, str] = {
    "pri": "DB_PRIMARY_URI",
    "rep": "DB_REPLICATED_URI",
    "dev": "DB_PARIVEDA_DEV_URI",
    "prod": "DB_PRODUCTION_URI",
}

# Maps internal short codes to default output folder names
DB_TARGET_FOLDER_NAMES: dict[str, str] = {
    "pri": "primary-portal-db.dev",
    "rep": "replicated-pariveda-db.qa",
    "dev": "pariveda-dev-db",
    "prod": "production-portal-db",
}


def resolve_db_target(target: str) -> str:
    """Resolve a user-supplied DB target string to an internal short code.

    Accepts both short codes ('pri', 'rep') and friendly names ('primary', 'replicated').
    Raises ValueError for unknown targets.
    """
    normalized = target.lower().strip()
    if normalized not in DB_TARGET_ALIASES:
        valid = ", ".join(sorted(DB_TARGET_ALIASES.keys()))
        raise ValueError(f"Unknown db_target '{target}'. Valid options: {valid}")
    return DB_TARGET_ALIASES[normalized]


def _find_config_dir(explicit_path: str | None = None) -> Path:
    """Find the configuration directory using search order."""
    candidates = []

    if explicit_path:
        candidates.append(Path(explicit_path))

    env_dir = os.environ.get("PARITY_CONFIG_DIR")
    if env_dir:
        candidates.append(Path(env_dir))

    candidates.append(Path.cwd())
    candidates.append(Path(__file__).parent.parent.parent)  # project root

    for path in candidates:
        if path.is_dir() and (path / "paths_config.ini").exists():
            return path
        # Also check if .env exists here even without paths_config.ini
        if path.is_dir() and (path / ".env").exists():
            return path

    # Fallback to CWD even if no config files found
    return Path.cwd()


class AppConfig:
    """Single source of truth for all configuration: paths, DB URIs, comparison settings."""

    def __init__(
        self,
        config_dir: str | None = None,
        env_file: str | None = None,
        paths_config_file: str | None = None,
        comparison_config_file: str | None = None,
    ):
        self._config_dir = _find_config_dir(config_dir)
        self._paths_config = configparser.ConfigParser()
        self._comparison_config = configparser.ConfigParser()
        self._defaults: dict[str, str] = {}

        # Load .env
        env_path = Path(env_file) if env_file else self._config_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
            logger.debug(f"Loaded .env from {env_path}")
        else:
            logger.warning(
                f"No .env file found at {env_path}. "
                "Run 'ep-parity init' to create one, or copy .env.example to .env"
            )

        # Load paths_config.ini
        paths_ini = (
            Path(paths_config_file)
            if paths_config_file
            else self._config_dir / "paths_config.ini"
        )
        if paths_ini.exists():
            self._paths_config.read(paths_ini)
            logger.debug(f"Loaded paths config from {paths_ini}")
        else:
            logger.warning(
                f"paths_config.ini not found at {paths_ini}. "
                "Copy paths_config.ini.example and configure your paths."
            )

        # Load defaults section
        if self._paths_config.has_section("defaults"):
            self._defaults = dict(self._paths_config.items("defaults"))

        # Load comparison_config.ini
        comp_ini = (
            Path(comparison_config_file)
            if comparison_config_file
            else self._config_dir / "comparison_config.ini"
        )
        if comp_ini.exists():
            self._comparison_config.read(comp_ini)
            logger.debug(f"Loaded comparison config from {comp_ini}")

    # --- Path properties ---

    @property
    def base_path(self) -> Path:
        """Base directory for parity testing results."""
        env_override = os.environ.get("PARITY_OUTPUT_DIR")
        if env_override:
            return Path(env_override)
        if self._paths_config.has_option("paths", "base_path"):
            return Path(self._paths_config.get("paths", "base_path"))
        raise ValueError(
            "base_path not configured. Run 'ep-parity init' to set it up, "
            "or add it to paths_config.ini [paths] section."
        )

    @property
    def sql_directory(self) -> Path:
        """Directory containing SQL query files."""
        env_override = os.environ.get("PARITY_SQL_DIR")
        if env_override:
            return Path(env_override)
        if self._paths_config.has_option("paths", "sql_directory"):
            return Path(self._paths_config.get("paths", "sql_directory"))
        raise ValueError(
            "sql_directory not configured. Run 'ep-parity init' to set it up, "
            "or add it to paths_config.ini [paths] section."
        )

    @property
    def directory_format(self) -> str:
        return self._paths_config.get(
            "output", "directory_format", fallback="{emp_id} {date} {time}"
        )

    @property
    def date_format(self) -> str:
        return self._paths_config.get("output", "date_format", fallback="%m-%d-%y %H%M")

    # --- Default settings ---

    def get_default(self, key: str, fallback: str | None = None) -> str | None:
        """Get a default value from the [defaults] section of paths_config.ini."""
        return self._defaults.get(key, fallback)

    # --- Database URI resolution ---

    @property
    def use_aws_secrets(self) -> bool:
        return self._defaults.get("use_aws_secrets", "false").lower() == "true"

    @property
    def secret_path(self) -> str | None:
        return self._defaults.get("aws_secret_path")

    def get_db_uri(self, target: str) -> str:
        """Get the database URI for a given target (short code).

        If use_aws_secrets is enabled, this returns an empty string — the
        DatabaseManager handles secret resolution.
        """
        if self.use_aws_secrets:
            return ""
        env_var = DB_TARGET_ENV_VARS.get(target)
        if not env_var:
            raise ValueError(f"No environment variable mapped for db target '{target}'")
        uri = os.getenv(env_var, "")
        if not uri:
            raise ValueError(
                f"{env_var} not set. Run 'ep-parity init' to configure credentials, "
                f"or add {env_var} to your .env file."
            )
        return uri

    def get_db_targets(self, target: str) -> list[str]:
        """Expand a target (which may be 'both') into a list of short codes."""
        resolved = resolve_db_target(target)
        if resolved == "both":
            return ["pri", "rep"]
        return [resolved]

    def get_folder_name(self, target: str) -> str:
        """Get the output folder name for a DB target short code."""
        return DB_TARGET_FOLDER_NAMES.get(target, target)

    # --- Comparison config properties ---

    @property
    def global_ignore_columns(self) -> list[str]:
        if not self._comparison_config.has_section("global_ignore_columns"):
            return []
        return [
            col
            for col, val in self._comparison_config.items("global_ignore_columns")
            if val.lower() == "true"
        ]

    def get_file_specific_ignore_columns(self, filename: str) -> list[str]:
        if not self._comparison_config.has_section("file_specific_ignore_columns"):
            return []
        raw = self._comparison_config.get(
            "file_specific_ignore_columns", filename, fallback=""
        )
        return [c.strip() for c in raw.split(",") if c.strip()]

    @property
    def exclude_files(self) -> list[str]:
        if not self._comparison_config.has_section("exclude_files"):
            return []
        return [
            f
            for f, val in self._comparison_config.items("exclude_files")
            if val.lower() == "true"
        ]

    def get_normalize_columns(self, filename: str) -> list[str]:
        if not self._comparison_config.has_section("normalize_columns"):
            return []
        raw = self._comparison_config.get("normalize_columns", filename, fallback="")
        return [c.strip() for c in raw.split(",") if c.strip()]

    def get_normalize_none_string_columns(self, filename: str) -> list[str]:
        if not self._comparison_config.has_section("normalize_none_string_columns"):
            return []
        raw = self._comparison_config.get(
            "normalize_none_string_columns", filename, fallback=""
        )
        return [c.strip() for c in raw.split(",") if c.strip()]

    @property
    def max_sample_differences(self) -> int:
        return self._comparison_config.getint(
            "comparison_settings", "max_sample_differences", fallback=5
        )

    @property
    def max_unique_rows_display(self) -> int:
        return self._comparison_config.getint(
            "comparison_settings", "max_unique_rows_display", fallback=10
        )

    @property
    def case_sensitive_comparison(self) -> bool:
        return self._comparison_config.getboolean(
            "comparison_settings", "case_sensitive_comparison", fallback=False
        )


def load_employer_ids_from_file(filepath: str) -> list[str]:
    """Load employer IDs from a text file, one per line.

    Blank lines and lines starting with # are ignored.
    """
    ids = []
    with open(filepath) as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                ids.append(stripped)
    return ids
