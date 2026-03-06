"""CLI entry point: ep-parity {subcommand}."""

import click

from ep_parity.utils.logging import setup_logging


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.option(
    "--config-dir",
    type=click.Path(exists=True, file_okay=False),
    envvar="PARITY_CONFIG_DIR",
    help="Directory containing config files (.env, paths_config.ini, comparison_config.ini).",
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool, config_dir: str | None) -> None:
    """EP Parity Testing Tool Suite.

    Export data, compare results, monitor processing, and generate reports
    for eligibility processor parity validation.
    """
    ctx.ensure_object(dict)
    setup_logging(verbose=verbose)
    ctx.obj["config_dir"] = config_dir
    ctx.obj["verbose"] = verbose


# Import and register subcommands
from ep_parity.cli.export_cmd import export  # noqa: E402
from ep_parity.cli.compare_cmd import compare  # noqa: E402
from ep_parity.cli.monitor_cmd import monitor  # noqa: E402
from ep_parity.cli.report_cmd import report  # noqa: E402
from ep_parity.cli.validate_cmd import validate  # noqa: E402

cli.add_command(export)
cli.add_command(compare)
cli.add_command(monitor)
cli.add_command(report)
cli.add_command(validate)
