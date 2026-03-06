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


@click.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Check if the environment is properly configured for parity testing."""
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

    print("\nComparison Configuration:")
    checks.append(_check_comparison_config(config_dir))

    print("\n" + "=" * 60)
    if all(checks):
        print("[pass] All checks passed. Ready to run parity tests.")
        print("\nNext steps:")
        print("  1. Connect to VPN")
        print("  2. ep-parity export --emp_ids <ID> --db_target both")
    else:
        print("[warn] Some checks need attention. Review the issues above.")
        print("\nFor help, see README.md")
    print("=" * 60)
