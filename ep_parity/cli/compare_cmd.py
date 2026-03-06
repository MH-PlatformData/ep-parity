"""ep-parity compare: Compare exported parity results between databases."""

import time

import click

from ep_parity.cli.common import batch_options, employer_ids_options, get_config
from ep_parity.core.comparison.engine import ParityComparison
from ep_parity.utils.logging import get_logger
from ep_parity.utils.runner import TaskResult, print_summary, run_batch

logger = get_logger("cli.compare")


def _compare_single(
    emp_id: str,
    config,
    run_timestamp: str | None,
    primary_dir: str | None,
    replicated_dir: str | None,
) -> TaskResult:
    """Compare results for a single employer. Used as task_fn for run_batch."""
    try:
        comparison = ParityComparison(config, emp_id, run_timestamp=run_timestamp)
        results = comparison.run_comparison(
            primary_dir=primary_dir,
            replicated_dir=replicated_dir,
        )

        total = len(results)
        matched = len([r for r in results if r.get("match", False)])
        failed = total - matched

        if failed == 0:
            return TaskResult(
                employer_id=emp_id,
                success=True,
                message=f"All {total} files match",
            )
        return TaskResult(
            employer_id=emp_id,
            success=True,  # Comparison ran successfully even if diffs found
            message=f"{matched}/{total} files match, {failed} with differences",
            data={"matched": matched, "total": total, "failed": failed},
        )
    except Exception as e:
        return TaskResult(employer_id=emp_id, success=False, message=str(e))


@click.command()
@employer_ids_options
@batch_options
@click.option(
    "--run_timestamp",
    type=str,
    default=None,
    help="Specific run timestamp folder (default: most recent).",
)
@click.option(
    "--primary_dir",
    type=click.Path(exists=True),
    default=None,
    help="Explicit primary directory (for comparing pre-existing folders).",
)
@click.option(
    "--replicated_dir",
    type=click.Path(exists=True),
    default=None,
    help="Explicit replicated directory (for comparing pre-existing folders).",
)
@click.pass_context
def compare(
    ctx: click.Context,
    emp_ids: list[str],
    parallel: bool,
    max_workers: int | None,
    max_retries: int,
    run_timestamp: str | None,
    primary_dir: str | None,
    replicated_dir: str | None,
) -> None:
    """Compare parity results between primary and replicated databases.

    Examples:

        ep-parity compare --emp_ids 289

        ep-parity compare --emp_ids 150 289 --run_timestamp "11-14-25 1530"

        ep-parity compare --emp_ids 289 --primary_dir /path/to/primary --replicated_dir /path/to/replicated
    """
    config = get_config(ctx)
    start = time.time()

    results = run_batch(
        employer_ids=emp_ids,
        task_fn=_compare_single,
        task_kwargs={
            "config": config,
            "run_timestamp": run_timestamp,
            "primary_dir": primary_dir,
            "replicated_dir": replicated_dir,
        },
        parallel=parallel,
        max_workers=max_workers,
        max_retries=max_retries,
    )
    print_summary(results, start, "comparison")
