"""Generic multi-employer batch runner with sequential/parallel modes.

Replaces the identical run_sequential/run_parallel/print_summary patterns
from run_parity_multi_employer.py, run_monitor_multi_employer.py, and
run_comparison_multi_employer.py.
"""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

from ep_parity.utils.logging import get_logger

logger = get_logger("runner")


@dataclass
class TaskResult:
    """Result from a single employer task execution."""

    employer_id: str
    success: bool
    message: str
    data: Any = field(default=None, repr=False)


def run_batch(
    employer_ids: list[str],
    task_fn: Callable[..., TaskResult],
    task_kwargs: dict | None = None,
    parallel: bool = False,
    max_workers: int | None = None,
    max_retries: int = 0,
) -> list[TaskResult]:
    """Run a task function for each employer ID, sequentially or in parallel.

    Args:
        employer_ids: List of employer IDs to process.
        task_fn: Callable that accepts (employer_id, **task_kwargs) and returns TaskResult.
        task_kwargs: Additional keyword arguments passed to task_fn.
        parallel: If True, run tasks concurrently using ProcessPoolExecutor.
        max_workers: Max parallel workers (default: min(4, len(employer_ids))).
        max_retries: Number of retry attempts per employer on failure.

    Returns:
        List of TaskResult, one per employer.
    """
    task_kwargs = task_kwargs or {}

    if parallel:
        return _run_parallel(
            employer_ids, task_fn, task_kwargs, max_workers, max_retries
        )
    return _run_sequential(employer_ids, task_fn, task_kwargs, max_retries)


def _run_with_retries(
    employer_id: str,
    task_fn: Callable[..., TaskResult],
    task_kwargs: dict,
    max_retries: int,
) -> TaskResult:
    """Execute task_fn with retry logic."""
    attempt = 0
    last_result = None

    while attempt <= max_retries:
        if attempt > 0:
            logger.info(
                f"Retry attempt {attempt}/{max_retries} for employer {employer_id}"
            )
            time.sleep(5)

        try:
            result = task_fn(employer_id, **task_kwargs)
            if result.success:
                return result
            last_result = result
        except Exception as e:
            logger.error(f"Error processing employer {employer_id}: {e}")
            last_result = TaskResult(
                employer_id=employer_id,
                success=False,
                message=f"Exception: {e}",
            )

        attempt += 1

    return last_result or TaskResult(
        employer_id=employer_id,
        success=False,
        message=f"Failed after {max_retries + 1} attempts",
    )


def _run_sequential(
    employer_ids: list[str],
    task_fn: Callable[..., TaskResult],
    task_kwargs: dict,
    max_retries: int,
) -> list[TaskResult]:
    """Run tasks sequentially."""
    total = len(employer_ids)
    results: list[TaskResult] = []

    logger.info(f"Sequential processing: {total} employer(s)")
    logger.info(f"Employer IDs: {', '.join(employer_ids)}")

    for idx, emp_id in enumerate(employer_ids, 1):
        logger.info(f"\nProcessing employer {idx}/{total}: {emp_id}")
        result = _run_with_retries(emp_id, task_fn, task_kwargs, max_retries)
        results.append(result)
        _log_result(result)

    return results


def _run_parallel(
    employer_ids: list[str],
    task_fn: Callable[..., TaskResult],
    task_kwargs: dict,
    max_workers: int | None,
    max_retries: int,
) -> list[TaskResult]:
    """Run tasks in parallel using ProcessPoolExecutor."""
    if max_workers is None:
        max_workers = min(4, len(employer_ids))

    logger.info(
        f"Parallel processing: {len(employer_ids)} employer(s) with {max_workers} workers"
    )
    logger.info(f"Employer IDs: {', '.join(employer_ids)}")

    results: list[TaskResult] = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_emp = {
            executor.submit(
                _run_with_retries, emp_id, task_fn, task_kwargs, max_retries
            ): emp_id
            for emp_id in employer_ids
        }

        for future in as_completed(future_to_emp):
            emp_id = future_to_emp[future]
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"Exception for employer {emp_id}: {e}")
                result = TaskResult(
                    employer_id=emp_id, success=False, message=f"Exception: {e}"
                )
            results.append(result)
            _log_result(result)

    return results


def _log_result(result: TaskResult) -> None:
    if result.success:
        logger.info(f"  OK: {result.message}")
    else:
        logger.error(f"  FAIL: {result.message}")


def print_summary(
    results: list[TaskResult], start_time: float, task_name: str = "processing"
) -> None:
    """Print standardized batch summary."""
    duration = time.time() - start_time
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    logger.info(f"\n{'='*70}")
    logger.info(f"{task_name.upper()} SUMMARY")
    logger.info(f"{'='*70}")
    logger.info(f"Total employers: {len(results)}")
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(failed)}")
    logger.info(f"Duration: {duration / 60:.1f} minutes")

    if successful:
        logger.info("\nSuccessful:")
        for r in successful:
            logger.info(f"  {r.employer_id}")

    if failed:
        logger.info("\nFailed:")
        for r in failed:
            logger.info(f"  {r.employer_id}: {r.message}")

    logger.info(f"{'='*70}\n")
