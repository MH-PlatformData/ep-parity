"""ep-parity report: Generate Excel summary reports from comparison results."""

from pathlib import Path

import click

from ep_parity.cli.common import employer_ids_options, get_config
from ep_parity.core.reporting.excel_summary import generate_excel_summary
from ep_parity.utils.logging import get_logger

logger = get_logger("cli.report")


@click.command()
@employer_ids_options
@click.option(
    "--output",
    type=click.Path(),
    default=None,
    help="Output Excel file path (default: saves alongside comparison reports).",
)
@click.option(
    "--date",
    type=str,
    default=None,
    help="Filter reports by date (e.g., 11-14-25). Default: most recent.",
)
@click.option(
    "--base_path",
    type=click.Path(exists=True),
    default=None,
    help="Base path to search for comparison reports (default: from config).",
)
@click.pass_context
def report(
    ctx: click.Context,
    emp_ids: list[str],
    output: str | None,
    date: str | None,
    base_path: str | None,
) -> None:
    """Generate Excel summary reports from comparison results.

    Parses text comparison reports and creates a multi-sheet Excel workbook
    with executive summary, detailed analysis, and color-coded status.

    Examples:

        ep-parity report --emp_ids 150 289 300

        ep-parity report --emp_ids 150 289 --date 11-14-25

        ep-parity report --emp_ids_file employers.txt --output summary.xlsx
    """
    config = get_config(ctx)

    search_path = Path(base_path) if base_path else config.base_path
    output_path = Path(output) if output else None

    success = generate_excel_summary(
        emp_ids=emp_ids,
        base_path=search_path,
        output_file=output_path,
        date_str=date,
    )

    if success:
        logger.info("Report generation complete.")
    else:
        logger.error("Report generation failed.")
        raise SystemExit(1)
