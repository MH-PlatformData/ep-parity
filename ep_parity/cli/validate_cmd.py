"""ep-parity validate: Check environment configuration before running parity tests."""

import sys
from pathlib import Path

import click

from ep_parity.utils.logging import get_logger

logger = get_logger("cli.validate")


def _check_python_version() -> bool:
    v = sys.version_info
    ok = v.major >= 3 and v.minor >= 10
    status = "OK" if ok else f"FAIL (need 3.10+)"
    print(f"  {'[pass]' if ok else '[FAIL]'} Python {v.major}.{v.minor}.{v.micro} {'' if ok else status}")
    return ok


def _check_virtual_env() -> bool:
    in_venv = hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )
    if in_venv:
        print("  [pass] Virtual environment is active")
    else:
        print("  [warn] Not running in a virtual environment")
        print("         Run: source .venv/bin/activate")
    return True  # Warning, not a failure


def _check_dependencies() -> bool:
    required = {
        "pandas": "pandas",
        "sqlalchemy": "sqlalchemy",
        "psycopg2": "psycopg2",
        "dotenv": "python-dotenv",
        "click": "click",
        "boto3": "boto3",
        "openpyxl": "openpyxl",
    }
    missing = []
    for module, pkg in required.items():
        try:
            __import__(module)
            print(f"  [pass] {pkg}")
        except ImportError:
            print(f"  [FAIL] {pkg} (not installed)")
            missing.append(pkg)
    if missing:
        print(f"\n  Run: pip install {' '.join(missing)}")
        return False
    return True


def _check_env_file(config_dir: Path) -> bool:
    env_path = config_dir / ".env"
    if not env_path.exists():
        print("  [FAIL] .env file not found")
        print("         Copy .env.example and add your credentials")
        return False

    print("  [pass] .env file exists")
    content = env_path.read_text()
    required_vars = ["DB_PRIMARY_URI", "DB_REPLICATED_URI"]
    all_ok = True
    for var in required_vars:
        if f"{var}=" in content and "your_username:your_password" not in content:
            print(f"  [pass] {var} configured")
        else:
            print(f"  [FAIL] {var} needs configuration")
            all_ok = False
    return all_ok


def _check_paths_config(config_dir: Path) -> bool:
    config_path = config_dir / "paths_config.ini"
    if not config_path.exists():
        print("  [FAIL] paths_config.ini not found")
        print("         Copy paths_config.ini.example and configure your paths")
        return False

    print("  [pass] paths_config.ini exists")
    import configparser

    config = configparser.ConfigParser()
    config.read(config_path)
    if "paths" in config:
        base_path = config.get("paths", "base_path", fallback="")
        sql_dir = config.get("paths", "sql_directory", fallback="")
        if "/path/to/your/" in base_path or "/path/to/your/" in sql_dir:
            print("  [warn] paths_config.ini has example values — update to real paths")
            return False
        print("  [pass] paths configured")
        return True

    print("  [warn] [paths] section missing in paths_config.ini")
    return False


def _check_comparison_config(config_dir: Path) -> bool:
    config_path = config_dir / "comparison_config.ini"
    if not config_path.exists():
        print("  [warn] comparison_config.ini not found (will use defaults)")
    else:
        print("  [pass] comparison_config.ini exists")
    return True


def _check_sql_directory_contents(config_dir: Path) -> bool:
    """Verify SQL query files exist and cross-reference against expected queries."""
    import configparser

    config = configparser.ConfigParser()
    config_path = config_dir / "paths_config.ini"
    if not config_path.exists():
        print("  [skip] Cannot check — paths_config.ini missing")
        return True

    config.read(config_path)
    sql_dir_str = config.get("paths", "sql_directory", fallback="")
    if not sql_dir_str or "/path/to/your/" in sql_dir_str:
        print("  [skip] Cannot check — sql_directory not configured")
        return True

    sql_dir = Path(sql_dir_str)
    if not sql_dir.exists():
        print(f"  [FAIL] sql_directory does not exist: {sql_dir}")
        return False

    from ep_parity.core.exporter import QUERY_MAP

    expected = set(QUERY_MAP.keys())
    found = {f.name for f in sql_dir.glob("*.sql")}
    missing = expected - found

    if not found:
        print(f"  [FAIL] No .sql files found in {sql_dir}")
        return False

    if missing:
        print(f"  [warn] Found {len(found)}/{len(expected)} expected SQL files")
        for name in sorted(missing):
            print(f"         Missing: {name}")
        return True  # Warning, not a failure — some queries may be intentionally excluded

    print(f"  [pass] Found all {len(expected)} expected SQL files")
    return True


def _check_write_permissions(config_dir: Path) -> bool:
    """Verify the output directory is writable."""
    import configparser
    import tempfile

    config = configparser.ConfigParser()
    config_path = config_dir / "paths_config.ini"
    if not config_path.exists():
        print("  [skip] Cannot check — paths_config.ini missing")
        return True

    config.read(config_path)
    base_path_str = config.get("paths", "base_path", fallback="")
    if not base_path_str or "/path/to/your/" in base_path_str:
        print("  [skip] Cannot check — base_path not configured")
        return True

    base_path = Path(base_path_str)
    if not base_path.exists():
        try:
            base_path.mkdir(parents=True, exist_ok=True)
            print(f"  [pass] Created output directory: {base_path}")
            return True
        except OSError as e:
            print(f"  [FAIL] Cannot create output directory: {e}")
            return False

    try:
        with tempfile.NamedTemporaryFile(dir=base_path, delete=True):
            pass
        print(f"  [pass] Output directory is writable: {base_path}")
        return True
    except OSError:
        print(f"  [FAIL] Output directory is not writable: {base_path}")
        return False


def _check_db_connectivity(config_dir: Path) -> bool:
    """Test actual database connectivity (requires VPN)."""
    try:
        from ep_parity.core.config import AppConfig, DB_TARGET_ENV_VARS
        from ep_parity.core.database import DatabaseManager
    except ImportError:
        print("  [skip] Required modules not available")
        return True

    import os

    config = AppConfig(config_dir=str(config_dir))
    db = DatabaseManager(config)
    all_ok = True

    for target, env_var in DB_TARGET_ENV_VARS.items():
        uri = os.getenv(env_var, "")
        if not uri:
            continue  # Skip unconfigured targets

        success, message = db.test_connection(target)
        if success:
            print(f"  [pass] {target:12s} {message}")
        else:
            print(f"  [FAIL] {target:12s} {message}")
            all_ok = False

    db.dispose_all()
    if not any(os.getenv(v) for v in DB_TARGET_ENV_VARS.values()):
        print("  [skip] No database URIs configured")
    return all_ok


@click.command()
@click.option(
    "--check-db/--no-check-db",
    default=False,
    help="Test database connectivity (requires VPN connection).",
)
@click.pass_context
def validate(ctx: click.Context, check_db: bool) -> None:
    """Check if the environment is properly configured for parity testing.

    By default, validates configuration files only. Use --check-db to also
    test actual database connectivity (requires VPN).

    Examples:

        ep-parity validate

        ep-parity validate --check-db
    """
    config_dir = Path(ctx.obj.get("config_dir") or ".")

    print("=" * 60)
    print("EP Parity Testing — Environment Validation")
    print("=" * 60)

    print("\nPython:")
    checks = [_check_python_version()]

    print("\nVirtual Environment:")
    checks.append(_check_virtual_env())

    print("\nDependencies:")
    checks.append(_check_dependencies())

    print("\nCredentials (.env):")
    checks.append(_check_env_file(config_dir))

    print("\nPaths Configuration:")
    checks.append(_check_paths_config(config_dir))

    print("\nSQL Directory:")
    checks.append(_check_sql_directory_contents(config_dir))

    print("\nOutput Directory:")
    checks.append(_check_write_permissions(config_dir))

    print("\nComparison Configuration:")
    checks.append(_check_comparison_config(config_dir))

    if check_db:
        print("\nDatabase Connectivity:")
        checks.append(_check_db_connectivity(config_dir))
    else:
        print("\nDatabase Connectivity:")
        print("  [skip] Use --check-db to test (requires VPN)")

    print("\n" + "=" * 60)
    if all(checks):
        print("[pass] All checks passed. Ready to run parity tests.")
        print("\nNext steps:")
        if not check_db:
            print("  1. Connect to VPN")
            print("  2. ep-parity validate --check-db")
            print("  3. ep-parity export --emp_ids <ID> --db_target both")
        else:
            print("  1. ep-parity export --emp_ids <ID> --db_target both")
    else:
        print("[warn] Some checks need attention. Review the issues above.")
        print("\nFor help:")
        print("  ep-parity init       — Interactive setup wizard")
        print("  ep-parity config show — View current configuration")
    print("=" * 60)
