"""ep-parity config show: Display loaded configuration with secrets masked."""

import os
import re

import click

from ep_parity.cli.common import get_config
from ep_parity.core.config import DB_TARGET_ENV_VARS


def _mask_uri(uri: str) -> str:
    """Mask password in a postgresql:// URI."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", uri)


@click.group()
@click.pass_context
def config(ctx: click.Context) -> None:
    """View and manage configuration."""
    pass


@config.command("show")
@click.pass_context
def config_show(ctx: click.Context) -> None:
    """Display current configuration with secrets masked.

    Useful for debugging configuration issues without exposing credentials.

    Example:

        ep-parity config show
    """
    cfg = get_config(ctx)

    click.echo("\nEP Parity Configuration")
    click.echo("=" * 50)

    # Database targets
    click.echo("\nDatabase Targets:")
    for short_code, env_var in DB_TARGET_ENV_VARS.items():
        uri = os.getenv(env_var, "")
        if uri:
            click.echo(f"  {short_code:12s} {_mask_uri(uri)}")
        else:
            click.echo(f"  {short_code:12s} (not configured -- {env_var} not set)")

    # Paths
    click.echo("\nPaths:")
    try:
        click.echo(f"  base_path:     {cfg.base_path}")
    except ValueError:
        click.echo("  base_path:     (not configured)")
    try:
        click.echo(f"  sql_directory: {cfg.sql_directory}")
    except ValueError:
        click.echo("  sql_directory: (not configured)")

    # Output format
    click.echo("\nOutput Format:")
    click.echo(f"  directory_format: {cfg.directory_format}")
    click.echo(f"  date_format:      {cfg.date_format}")

    # Defaults
    click.echo("\nDefaults:")
    default_keys = [
        "db_target", "env", "aws_profile", "check_interval",
        "max_wait_time", "parallel", "use_aws_secrets",
    ]
    any_defaults = False
    for key in default_keys:
        val = cfg.get_default(key)
        if val is not None:
            click.echo(f"  {key:18s} {val}")
            any_defaults = True
    if not any_defaults:
        click.echo("  (none set -- add a [defaults] section to paths_config.ini)")

    # Comparison settings summary
    click.echo("\nComparison Settings:")
    click.echo(f"  case_sensitive:          {cfg.case_sensitive_comparison}")
    click.echo(f"  max_sample_differences:  {cfg.max_sample_differences}")
    click.echo(f"  max_unique_rows_display: {cfg.max_unique_rows_display}")
    ignore_cols = cfg.global_ignore_columns
    if ignore_cols:
        click.echo(f"  global_ignore_columns:   {', '.join(ignore_cols[:5])}")
        if len(ignore_cols) > 5:
            click.echo(f"                           ... and {len(ignore_cols) - 5} more")
    excluded = cfg.exclude_files
    if excluded:
        click.echo(f"  excluded_files:          {len(excluded)} files")

    click.echo("")
