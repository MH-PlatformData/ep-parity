"""Shared monitoring infrastructure for EP processing monitors.

Provides:
- check_deposited_file() — verify a deposited file exists for today
- monitor_until_complete() — poll multiple monitors until all report complete
"""

import datetime
import time
from typing import Protocol

from ep_parity.core.database import DatabaseManager
from ep_parity.utils.logging import get_logger

logger = get_logger("monitoring.base")


class ProcessingMonitor(Protocol):
    """Interface that all processing monitors must implement."""

    def check_processing_complete(self) -> tuple[bool, dict]:
        """Check whether processing is complete.

        Returns:
            tuple of (is_complete, details_dict).
        """
        ...


def check_deposited_file(
    db: DatabaseManager, employer_id: int
) -> tuple[bool, dict]:
    """Check if a deposited file for *employer_id* was created today.

    Uses the shared DatabaseManager instead of creating its own engine.

    Args:
        db: Shared DatabaseManager instance.
        employer_id: Employer to look up.

    Returns:
        tuple of (file_exists_today, details_dict).
    """
    query = """
    SELECT id,
           employer_id,
           user_id,
           data,
           created_at,
           updated_at,
           state,
           scan_ended_at,
           scan_started_at
    FROM deposited_files
    WHERE employer_id = :employer_id
    ORDER BY created_at DESC
    LIMIT 1
    """

    try:
        row = db.execute_scalar("pri", query, {"employer_id": employer_id})

        if not row:
            return False, {
                "message": f"No deposited_file found for employer {employer_id}"
            }

        created_at = row["created_at"]

        # Normalize timestamps for comparison (remove timezone info)
        if hasattr(created_at, "tzinfo") and created_at.tzinfo is not None:
            created_at_normalized = created_at.replace(tzinfo=None)
        else:
            created_at_normalized = created_at

        today = datetime.datetime.now().date()
        created_date = created_at_normalized.date()

        if created_date != today:
            return False, {
                "file_id": row["id"],
                "state": row["state"],
                "created_at": created_at,
                "created_date": created_date,
                "today": today,
                "message": (
                    f"Found deposited_file {row['id']}, but it was created "
                    f"on {created_date}, not today ({today})"
                ),
            }

        return True, {
            "file_id": row["id"],
            "state": row["state"],
            "created_at": created_at,
            "message": (
                f"Found deposited_file {row['id']} created today "
                f"(state: {row['state']})"
            ),
        }

    except Exception as e:
        logger.error(f"Error checking deposited_files: {e}")
        return False, {
            "message": f"Error checking deposited_files: {str(e)}"
        }


def monitor_until_complete(
    monitors: list[ProcessingMonitor],
    check_interval: int = 120,
    max_wait_time: int = 7200,
) -> tuple[bool, dict]:
    """Poll monitors until ALL report complete or timeout is reached.

    Each monitor in the list must implement ``check_processing_complete()``
    returning ``(bool, dict)``.

    Args:
        monitors: List of monitor objects to poll.
        check_interval: Seconds between polling cycles.
        max_wait_time: Maximum total seconds to wait before giving up.

    Returns:
        tuple of (all_complete, summary_dict).  ``summary_dict`` contains
        per-monitor status keyed by class name and a ``total_elapsed``
        field.
    """
    start_time = time.time()
    completed = {id(m): False for m in monitors}
    latest_details: dict[str, dict] = {}

    while True:
        elapsed = time.time() - start_time

        if elapsed > max_wait_time:
            logger.warning(
                f"Maximum wait time ({max_wait_time}s) exceeded after "
                f"{elapsed / 60:.1f} minutes"
            )
            return False, {
                "monitors": latest_details,
                "total_elapsed": elapsed,
                "timed_out": True,
            }

        logger.info(
            f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] "
            f"Checking status... (elapsed: {elapsed / 60:.1f} min)"
        )

        for monitor in monitors:
            monitor_key = type(monitor).__name__

            if completed[id(monitor)]:
                continue

            is_done, details = monitor.check_processing_complete()
            latest_details[monitor_key] = details

            if is_done:
                completed[id(monitor)] = True
                logger.info(
                    f"  {monitor_key}: COMPLETE - "
                    f"{details.get('message', '')}"
                )
            else:
                logger.info(
                    f"  {monitor_key}: PROCESSING - "
                    f"{details.get('message', '')}"
                )

        if all(completed.values()):
            logger.info("All monitors report processing complete.")
            return True, {
                "monitors": latest_details,
                "total_elapsed": time.time() - start_time,
                "timed_out": False,
            }

        logger.info(f"  Waiting {check_interval} seconds before next check...")
        time.sleep(check_interval)
