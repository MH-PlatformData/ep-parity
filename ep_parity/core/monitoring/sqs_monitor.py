"""SQS queue monitor for EP 2.0 batch processing.

Monitors the eligibility processor SQS queues and dead-letter queues to
determine when batch processing has completed.

Queue flow:
1. Deposited (DepositedQueue) -- Ingestion Lambda
2. PotentialsAndIssues (PotentialsAndIssuesQueue) -- InputAndValidation Lambda
   - Employees go here immediately
   - NonEmployees go to NonEmployeePotentialsAndIssues (with delay)
3. Potentials (PotentialsQueue) -- PersonFinder Lambda -> PotentialsSorter Lambda
4. Promoted (PromotedQueue) -- Promoter Lambda (final step)
"""

from __future__ import annotations

import boto3

from ep_parity.core.database import DatabaseManager
from ep_parity.core.monitoring.base_monitor import check_deposited_file
from ep_parity.utils.logging import get_logger

logger = get_logger("monitoring.sqs")


class SQSQueueMonitor:
    """Monitor EP 2.0 SQS queues for batch processing completion."""

    def __init__(
        self,
        env: str,
        aws_profile: str,
        db: DatabaseManager | None = None,
        employer_id: int | None = None,
    ):
        """Initialise the SQS queue monitor.

        Args:
            env: Environment name (``qa`` or ``dev``).
            aws_profile: AWS profile name for boto3 session.
            db: Optional shared DatabaseManager for deposited-file checks.
            employer_id: Optional employer ID for deposited-file checks.
        """
        self.env = env.lower()
        if self.env not in ("qa", "dev"):
            raise ValueError("Only 'qa' and 'dev' environments are supported")

        self.db = db
        self.employer_id = employer_id

        # Initialise boto3 session with the given profile
        session = boto3.Session(profile_name=aws_profile)
        self.sqs = session.client("sqs", region_name="us-east-1")

        # Queue names from CloudFormation template
        self.queue_patterns: dict[str, str] = {
            "deposited": f"eligibilityProcessor-Deposited-{self.env}.fifo",
            "potentials_and_issues": (
                f"eligibilityProcessor-PotentialsAndIssues-{self.env}.fifo"
            ),
            "nonemployee": (
                f"eligibilityProcessor-NonEmployeePotentialsAndIssues-"
                f"{self.env}.fifo"
            ),
            "potentials": f"eligibilityProcessor-Potentials-{self.env}.fifo",
            "promoted": f"eligibilityProcessor-Promoted-{self.env}.fifo",
        }

        # DLQ naming: eligibilityProcessor-{QueueName}-DLQ-{env}.fifo
        self.dlq_patterns: dict[str, str] = {
            "deposited_dlq": (
                f"eligibilityProcessor-Deposited-DLQ-{self.env}.fifo"
            ),
            "potentials_and_issues_dlq": (
                f"eligibilityProcessor-PotentialsAndIssues-DLQ-{self.env}.fifo"
            ),
            "nonemployee_dlq": (
                f"eligibilityProcessor-NonEmployeePotentialsAndIssues-DLQ-"
                f"{self.env}.fifo"
            ),
            "potentials_dlq": (
                f"eligibilityProcessor-Potentials-DLQ-{self.env}.fifo"
            ),
            "promoted_dlq": (
                f"eligibilityProcessor-Promoted-DLQ-{self.env}.fifo"
            ),
        }

        self.queue_urls: dict[str, str] = {}
        self.dlq_urls: dict[str, str] = {}
        self._discover_queue_urls()

    # ------------------------------------------------------------------
    # Queue discovery
    # ------------------------------------------------------------------

    def _discover_queue_urls(self) -> None:
        """Discover queue URLs using the SQS paginator."""
        logger.info(
            f"Discovering EP 2.0 queues for {self.env} environment..."
        )

        try:
            paginator = self.sqs.get_paginator("list_queues")

            for page in paginator.paginate(
                QueueNamePrefix="eligibilityProcessor"
            ):
                for url in page.get("QueueUrls", []):
                    queue_name = url.split("/")[-1]

                    # Match against regular queue patterns
                    for key, pattern in self.queue_patterns.items():
                        if queue_name == pattern:
                            self.queue_urls[key] = url
                            logger.info(f"  Found {key}: {queue_name}")

                    # Match against DLQ patterns
                    for key, pattern in self.dlq_patterns.items():
                        if queue_name == pattern:
                            self.dlq_urls[key] = url
                            logger.info(f"  Found {key}: {queue_name}")

            if not self.queue_urls:
                logger.warning(
                    "No queues found. Verify the AWS profile and permissions."
                )

        except Exception as e:
            logger.error(f"Error discovering queues: {e}")
            raise

    # ------------------------------------------------------------------
    # Queue attribute helpers
    # ------------------------------------------------------------------

    def _get_queue_attributes(self, queue_url: str) -> dict:
        """Return message-count attributes for a single queue."""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )
            return response["Attributes"]
        except Exception as e:
            logger.error(f"Error getting queue attributes: {e}")
            return {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check_deposited_file(self) -> tuple[bool, dict]:
        """Check whether a deposited file exists for today.

        Delegates to :func:`base_monitor.check_deposited_file`.  If no
        ``DatabaseManager`` or ``employer_id`` was provided at init time,
        the check is skipped.
        """
        if not self.db or not self.employer_id:
            return True, {
                "message": (
                    "Skipping deposited_files check "
                    "(no db or employer_id provided)"
                )
            }
        return check_deposited_file(self.db, self.employer_id)

    def check_processing_complete(self) -> tuple[bool, dict]:
        """Check if all queues are empty (processing complete).

        Returns:
            tuple of (is_complete, details_dict).
        """
        if not self.queue_urls:
            return False, {"error": "No queues discovered"}

        all_empty = True
        queue_status: dict[str, dict] = {}
        total_messages = 0

        for queue_name, queue_url in self.queue_urls.items():
            attrs = self._get_queue_attributes(queue_url)

            visible = int(
                attrs.get("ApproximateNumberOfMessages", 0)
            )
            in_flight = int(
                attrs.get("ApproximateNumberOfMessagesNotVisible", 0)
            )
            delayed = int(
                attrs.get("ApproximateNumberOfMessagesDelayed", 0)
            )

            total = visible + in_flight + delayed
            total_messages += total

            queue_status[queue_name] = {
                "visible": visible,
                "in_flight": in_flight,
                "delayed": delayed,
                "total": total,
            }

            if total > 0:
                all_empty = False

        return all_empty, {
            "queues": queue_status,
            "total_messages": total_messages,
            "message": f"EP 2.0 total messages: {total_messages}",
        }

    def check_dlqs(self) -> tuple[bool, dict]:
        """Check all DLQs for messages.

        Returns:
            tuple of (has_errors, details_dict).  ``has_errors`` is True
            when any DLQ contains messages.
        """
        if not self.dlq_urls:
            return False, {"message": "No DLQs found"}

        dlq_status: dict[str, int] = {}
        total_dlq_messages = 0

        for dlq_name, dlq_url in self.dlq_urls.items():
            try:
                response = self.sqs.get_queue_attributes(
                    QueueUrl=dlq_url,
                    AttributeNames=["ApproximateNumberOfMessages"],
                )
                message_count = int(
                    response["Attributes"].get(
                        "ApproximateNumberOfMessages", 0
                    )
                )
                dlq_status[dlq_name] = message_count
                total_dlq_messages += message_count
            except Exception as e:
                logger.error(f"Error checking DLQ {dlq_name}: {e}")
                dlq_status[dlq_name] = -1  # Indicate error

        has_errors = total_dlq_messages > 0

        return has_errors, {
            "dlqs": dlq_status,
            "total_messages": total_dlq_messages,
            "message": f"DLQ total messages: {total_dlq_messages}",
        }
