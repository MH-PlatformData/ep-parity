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


def _resolve_targets(raw_targets: tuple[str, ...], config) -> list[str]:
    """Resolve and deduplicate a tuple of user-supplied target strings.

    Handles legacy 'both' default from paths_config.ini by expanding it
    to ep15-qa + ep20-qa with a deprecation warning.
    """
    if not raw_targets:
        default_raw = config.get_default("db_target")
        if default_raw is None:
            raise click.UsageError(
                "--db_target is required (or set db_target in [defaults] "
                "section of paths_config.ini)."
            )
        if default_raw.lower().strip() == "both":
            logger.warning(
                "'both' is deprecated as a db_target default. "
                "Use 'ep15-qa ep20-qa' instead."
            )
            raw_targets = ("ep15-qa", "ep20-qa")
        else:
            raw_targets = tuple(default_raw.split())

    resolved = [resolve_db_target(t) for t in raw_targets]

    # Deduplicate preserving order
    seen = set()
    unique = []
    for t in resolved:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


def _export_single(
    emp_id: str,
    config,
    db: DatabaseManager,
    db_targets: list[str],
    run_comparison: bool,
) -> TaskResult:
    """Export data for a single employer. Used as task_fn for run_batch."""
    try:
        output_dir, targets = run_export(config, db, emp_id, db_targets)

        # Auto-run comparison if exporting from two databases
        if run_comparison and len(targets) == 2:
            try:
                from ep_parity.core.comparison.engine import ParityComparison

                comparison = ParityComparison(config, emp_id)
                results = comparison.run_comparison(left_dir=None, right_dir=None)
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
    multiple=True,
    type=str,
    help="One or more database targets: ep15-dev, ep15-qa, ep20-dev, ep20-qa, prod.",
)
@click.option(
    "--compare/--no_compare",
    "run_comparison",
    default=None,
    help="Run comparison after export (default: auto when 2 targets given).",
)
@click.pass_context
def export(
    ctx: click.Context,
    emp_ids: list[str],
    parallel: bool,
    max_workers: int | None,
    max_retries: int,
    db_target: tuple[str, ...],
    run_comparison: bool | None,
) -> None:
    """Export query results from one or more databases.

    Examples:

        ep-parity export --emp_ids 150 --db_target ep15-qa --db_target ep20-qa

        ep-parity export --emp_ids 150 289 --db_target ep15-qa --parallel

        ep-parity export --emp_ids_file employers.txt --db_target prod
    """
    config = get_config(ctx)

    resolved_targets = _resolve_targets(db_target, config)

    # Smart default: comparison runs when exporting from exactly 2 DBs
    if run_comparison is None:
        run_comparison = len(resolved_targets) == 2

    db = DatabaseManager(config)
    start = time.time()

    try:
        results = run_batch(
            employer_ids=emp_ids,
            task_fn=_export_single,
            task_kwargs={
                "config": config,
                "db": db,
                "db_targets": resolved_targets,
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
