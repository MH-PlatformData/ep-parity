"""ep-parity monitor: Monitor processing completion and trigger parity testing."""

import time

import click

from ep_parity.cli.common import batch_options, employer_ids_options, get_config
from ep_parity.core.database import DatabaseManager
from ep_parity.core.monitoring.base_monitor import check_deposited_file, monitor_until_complete
from ep_parity.core.monitoring.sqs_monitor import SQSQueueMonitor
from ep_parity.core.monitoring.db_monitor import EP15Monitor
from ep_parity.utils.logging import get_logger
from ep_parity.utils.runner import TaskResult, print_summary, run_batch

logger = get_logger("cli.monitor")


def _get_monitor_targets(mode: str, env: str) -> list[str]:
    """Build database target short codes from mode and environment.

    Returns a list of targets like ``["ep15-qa", "ep20-qa"]`` for use by
    the exporter after monitoring completes.
    """
    ep15 = f"ep15-{env}"
    ep20 = f"ep20-{env}"
    if mode == "both":
        return [ep15, ep20]
    elif mode == "ep15_only":
        return [ep15]
    elif mode == "ep20_only":
        return [ep20]
    return [ep15, ep20]


def _monitor_single(
    emp_id: str,
    config,
    env: str,
    mode: str,
    aws_profile: str,
    check_interval: int,
    max_wait_time: int,
    skip_parity: bool,
) -> TaskResult:
    """Monitor and optionally run parity for a single employer."""
    db = DatabaseManager(config)

    try:
        employer_id = int(emp_id)

        ep15_target = f"ep15-{env}"

        # Pre-flight: check deposited file exists
        file_exists, details = check_deposited_file(db, employer_id, target=ep15_target)
        if not file_exists:
            return TaskResult(
                employer_id=emp_id,
                success=False,
                message=f"No deposited file: {details['message']}",
            )
        logger.info(f"Deposited file check passed: {details['message']}")

        # Build list of monitors based on mode
        monitors = []
        if mode in ("both", "ep20_only"):
            sqs = SQSQueueMonitor(
                env=env,
                aws_profile=aws_profile,
                db=db,
                employer_id=employer_id,
            )
            monitors.append(sqs)

        if mode in ("both", "ep15_only"):
            ep15 = EP15Monitor(db=db, employer_id=employer_id, target=ep15_target)
            monitors.append(ep15)

        if not monitors:
            return TaskResult(
                employer_id=emp_id,
                success=False,
                message=f"No monitors configured for mode '{mode}'",
            )

        # Poll until all monitors report complete
        all_complete, summary = monitor_until_complete(
            monitors=monitors,
            check_interval=check_interval,
            max_wait_time=max_wait_time,
        )

        if not all_complete:
            return TaskResult(
                employer_id=emp_id,
                success=False,
                message=f"Monitoring timed out or failed: {summary.get('message', '')}",
            )

        # Check DLQs if SQS monitoring was active
        if mode in ("both", "ep20_only"):
            has_errors, dlq_details = sqs.check_dlqs()
            if has_errors:
                return TaskResult(
                    employer_id=emp_id,
                    success=False,
                    message=f"DLQ errors detected: {dlq_details['total_messages']} messages",
                )

        # Run parity testing if requested
        if not skip_parity:
            db_targets = _get_monitor_targets(mode, env)
            try:
                from ep_parity.core.exporter import run_export

                output_dir, targets = run_export(config, db, emp_id, db_targets)
                parity_msg = f"Exported to {output_dir}"

                # Auto-compare if two targets
                if len(targets) == 2:
                    from ep_parity.core.comparison.engine import ParityComparison

                    comparison = ParityComparison(config, emp_id)
                    results = comparison.run_comparison()
                    failed = len([r for r in results if not r.get("match", False)])
                    parity_msg += f", {failed} differences" if failed else ", all match"
            except Exception as e:
                parity_msg = f"Parity testing error: {e}"
                return TaskResult(
                    employer_id=emp_id, success=False, message=parity_msg
                )

            return TaskResult(
                employer_id=emp_id,
                success=True,
                message=f"Processing complete. {parity_msg}",
            )

        return TaskResult(
            employer_id=emp_id,
            success=True,
            message="Processing complete (parity skipped)",
        )
    finally:
        db.dispose_all()


@click.command()
@employer_ids_options
@batch_options
@click.option(
    "--env",
    type=click.Choice(["qa", "dev"], case_sensitive=False),
    default=None,
    help="Environment to monitor (qa or dev only).",
)
@click.option(
    "--mode",
    type=click.Choice(["both", "ep15_only", "ep20_only"], case_sensitive=False),
    default="both",
    help="Monitor mode: both (default), ep15_only, or ep20_only.",
)
@click.option(
    "--aws_profile",
    type=str,
    default=None,
    help="AWS profile for SQS access (default: DataEngineerQA).",
)
@click.option(
    "--check_interval",
    type=int,
    default=None,
    help="Seconds between queue checks (default: 120).",
)
@click.option(
    "--max_wait_time",
    type=int,
    default=None,
    help="Maximum seconds to wait (default: 7200).",
)
@click.option(
    "--skip_parity",
    is_flag=True,
    help="Skip parity testing after monitoring completes.",
)
@click.pass_context
def monitor(
    ctx: click.Context,
    emp_ids: list[str],
    parallel: bool,
    max_workers: int | None,
    max_retries: int,
    env: str | None,
    mode: str,
    aws_profile: str | None,
    check_interval: int | None,
    max_wait_time: int | None,
    skip_parity: bool,
) -> None:
    """Monitor processing completion and optionally trigger parity testing.

    Supports EP 1.5 (database polling), EP 2.0 (SQS queue monitoring), or both.

    Examples:

        ep-parity monitor --emp_ids 150 --env qa

        ep-parity monitor --emp_ids 150 --env qa --mode ep20_only

        ep-parity monitor --emp_ids 150 289 --env dev --skip_parity
    """
    config = get_config(ctx)

    # Resolve defaults from config
    if env is None:
        env = config.get_default("env")
    if env is None:
        raise click.UsageError(
            "--env is required (or set env in [defaults] section of paths_config.ini)."
        )
    if aws_profile is None:
        aws_profile = config.get_default("aws_profile", "DataEngineerQA")
    if check_interval is None:
        check_interval = int(config.get_default("check_interval", "120"))
    if max_wait_time is None:
        max_wait_time = int(config.get_default("max_wait_time", "7200"))

    start = time.time()

    results = run_batch(
        employer_ids=emp_ids,
        task_fn=_monitor_single,
        task_kwargs={
            "config": config,
            "env": env,
            "mode": mode,
            "aws_profile": aws_profile,
            "check_interval": check_interval,
            "max_wait_time": max_wait_time,
            "skip_parity": skip_parity,
        },
        parallel=parallel,
        max_workers=max_workers,
        max_retries=max_retries,
    )
    print_summary(results, start, "monitoring")

    failed = [r for r in results if not r.success]
    if failed:
        raise SystemExit(1)
