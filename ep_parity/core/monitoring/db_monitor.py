"""Database monitor for EP 1.5 processing completion.

Polls the ``cleaned_datasets`` table to determine when the EP 1.5
eligibility processor has finished processing a file for a given employer.
"""

from __future__ import annotations

from ep_parity.core.database import DatabaseManager
from ep_parity.utils.logging import get_logger

logger = get_logger("monitoring.db")


class EP15Monitor:
    """Monitor EP 1.5 processing via cleaned_datasets state."""

    def __init__(self, db: DatabaseManager, employer_id: int, target: str = "ep15-qa"):
        """Initialise the EP 1.5 monitor.

        Args:
            db: Shared DatabaseManager instance.  The DB lifecycle is
                owned by the caller, not this monitor.
            employer_id: Employer ID to monitor.
            target: Database target short code to query (e.g. 'ep15-qa', 'ep15-dev').
        """
        self.db = db
        self.employer_id = employer_id
        self.target = target
        logger.info(f"EP 1.5 Monitor initialised for employer {employer_id} on {target}")

    def check_processing_complete(self) -> tuple[bool, dict]:
        """Check if EP 1.5 processing is complete.

        Queries the most recent ``cleaned_datasets`` row for the latest
        deposited file belonging to ``self.employer_id`` and checks
        whether its ``state`` column equals ``'processed'``.

        Returns:
            tuple of (is_complete, details_dict).
        """
        query = """
        SELECT id,
               state,
               deposited_file_id,
               created_at,
               updated_at,
               active,
               processor,
               clean_job_id,
               process_job_id,
               start_cleaning_at,
               start_processing_at
        FROM cleaned_datasets
        WHERE deposited_file_id = (
            SELECT id
            FROM deposited_files
            WHERE employer_id = :employer_id
            ORDER BY 1 DESC
            LIMIT 1
        )
        ORDER BY created_at
        """

        try:
            row = self.db.execute_scalar(
                self.target, query, {"employer_id": self.employer_id}
            )

            if not row:
                return False, {
                    "status": "no_dataset",
                    "message": (
                        f"No cleaned_dataset found for employer "
                        f"{self.employer_id}"
                    ),
                }

            state = row["state"]
            is_complete = state == "processed"

            return is_complete, {
                "status": state,
                "dataset_id": row["id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "message": f"EP 1.5 status: {state}",
            }

        except Exception as e:
            logger.error(f"Error checking EP 1.5 status: {e}")
            return False, {
                "status": "error",
                "message": f"Error: {str(e)}",
            }

    def close(self) -> None:
        """No-op. DB lifecycle is managed by DatabaseManager."""
