"""ep-parity export: Export data from databases via SQL queries."""

import time

import click

from ep_parity.cli.common import batch_options, employer_ids_options, get_config
from ep_parity.core.config import resolve_db_target
from ep_parity.core.database import DatabaseManager
from ep_parity.core.exporter import run_export
from ep_parity.utils.logging import get_logger
from ep_parity.utils.runner import TaskResult, print_summary, run_batch

logger = get_logger("cli.export")


def _export_single(
    emp_id: str,
    config,
    db: DatabaseManager,
    db_target: str,
    run_comparison: bool,
) -> TaskResult:
    """Export data for a single employer. Used as task_fn for run_batch."""
    try:
        output_dir, targets = run_export(config, db, emp_id, db_target)

        # Auto-run comparison if exporting from two databases
        if run_comparison and len(targets) == 2:
            try:
                from ep_parity.core.comparison.engine import ParityComparison

                comparison = ParityComparison(config, emp_id)
                results = comparison.run_comparison(primary_dir=None, replicated_dir=None)
                failed = len([r for r in results if not r.get("match", False)])
                comp_msg = f", comparison: {failed} differences" if failed else ", comparison: all match"
            except Exception as e:
                comp_msg = f", comparison error: {e}"
        else:
            comp_msg = ""

        return TaskResult(
            employer_id=emp_id,
            success=True,
            message=f"Exported to {output_dir}{comp_msg}",
            data={"output_dir": str(output_dir), "targets": targets},
        )
    except ConnectionError as e:
        return TaskResult(employer_id=emp_id, success=False, message=str(e))
    except Exception as e:
        msg = str(e)
        if "could not translate host name" in msg.lower():
            msg += "\n  Tip: Are you connected to VPN?"
        return TaskResult(employer_id=emp_id, success=False, message=msg)


@click.command()
@employer_ids_options
@batch_options
@click.option(
    "--db_target",
    type=str,
    default=None,
    help="Database target: primary/pri, replicated/rep, both, dev, production/prod.",
)
@click.option(
    "--compare/--no_compare",
    "run_comparison",
    default=None,
    help="Run comparison after export (default: auto when db_target=both).",
)
@click.pass_context
def export(
    ctx: click.Context,
    emp_ids: list[str],
    parallel: bool,
    max_workers: int | None,
    max_retries: int,
    db_target: str | None,
    run_comparison: bool | None,
) -> None:
    """Export query results from one or two databases.

    Examples:

        ep-parity export --emp_ids 150 --db_target both

        ep-parity export --emp_ids 150 289 300 --db_target primary --parallel

        ep-parity export --emp_ids_file employers.txt --db_target replicated
    """
    config = get_config(ctx)

    # Resolve db_target from CLI, config defaults, or error
    if db_target is None:
        db_target = config.get_default("db_target")
    if db_target is None:
        raise click.UsageError(
            "--db_target is required (or set db_target in [defaults] section of paths_config.ini)."
        )
    resolved_target = resolve_db_target(db_target)

    # Smart default: comparison runs when exporting from two DBs
    if run_comparison is None:
        run_comparison = resolved_target == "both"

    db = DatabaseManager(config)
    start = time.time()

    try:
        results = run_batch(
            employer_ids=emp_ids,
            task_fn=_export_single,
            task_kwargs={
                "config": config,
                "db": db,
                "db_target": resolved_target,
                "run_comparison": run_comparison,
            },
            parallel=parallel,
            max_workers=max_workers,
            max_retries=max_retries,
        )
        print_summary(results, start, "export")

        failed = [r for r in results if not r.success]
        if failed:
            raise SystemExit(1)
    finally:
        db.dispose_all()
