"""Tests for ep_parity.utils.runner — batch runner, retries, summary."""

from unittest.mock import patch

import pytest

from ep_parity.utils.runner import (
    TaskResult,
    _run_with_retries,
    print_summary,
    run_batch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_success_task(employer_id, **kwargs):
    return TaskResult(employer_id=employer_id, success=True, message="OK")


def _make_failure_task(employer_id, **kwargs):
    return TaskResult(employer_id=employer_id, success=False, message="Failed")


def _make_exception_task(employer_id, **kwargs):
    raise RuntimeError("boom")


# ---------------------------------------------------------------------------
# run_batch — sequential mode
# ---------------------------------------------------------------------------


class TestRunBatchSequential:
    def test_all_success(self):
        results = run_batch(
            employer_ids=["100", "200", "300"],
            task_fn=_make_success_task,
            parallel=False,
        )
        assert len(results) == 3
        assert all(r.success for r in results)

    def test_mixed_success_failure(self):
        call_count = 0

        def alternating_task(employer_id, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                return TaskResult(employer_id=employer_id, success=False, message="Fail")
            return TaskResult(employer_id=employer_id, success=True, message="OK")

        results = run_batch(
            employer_ids=["1", "2", "3"],
            task_fn=alternating_task,
            parallel=False,
        )
        assert len(results) == 3
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 2
        assert len(failures) == 1

    def test_task_kwargs_passed_through(self):
        received = {}

        def capturing_task(employer_id, **kwargs):
            received.update(kwargs)
            return TaskResult(employer_id=employer_id, success=True, message="OK")

        run_batch(
            employer_ids=["1"],
            task_fn=capturing_task,
            task_kwargs={"db_target": "pri", "env": "qa"},
            parallel=False,
        )
        assert received["db_target"] == "pri"
        assert received["env"] == "qa"


# ---------------------------------------------------------------------------
# run_batch — with retries
# ---------------------------------------------------------------------------


class TestRunBatchWithRetries:
    @patch("ep_parity.utils.runner.time.sleep")  # skip real sleep
    def test_retries_on_failure(self, mock_sleep):
        attempts = []

        def fail_then_succeed(employer_id, **kwargs):
            attempts.append(employer_id)
            if len(attempts) < 2:
                return TaskResult(
                    employer_id=employer_id, success=False, message="Transient"
                )
            return TaskResult(employer_id=employer_id, success=True, message="OK")

        results = run_batch(
            employer_ids=["42"],
            task_fn=fail_then_succeed,
            parallel=False,
            max_retries=1,
        )
        assert len(results) == 1
        assert results[0].success is True
        assert len(attempts) == 2  # initial + 1 retry

    @patch("ep_parity.utils.runner.time.sleep")
    def test_exhausts_retries(self, mock_sleep):
        results = run_batch(
            employer_ids=["42"],
            task_fn=_make_failure_task,
            parallel=False,
            max_retries=2,
        )
        assert len(results) == 1
        assert results[0].success is False


# ---------------------------------------------------------------------------
# _run_with_retries
# ---------------------------------------------------------------------------


class TestRunWithRetries:
    @patch("ep_parity.utils.runner.time.sleep")
    def test_stops_on_first_success(self, mock_sleep):
        attempts = []

        def succeed_second(employer_id, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                return TaskResult(
                    employer_id=employer_id, success=False, message="Fail"
                )
            return TaskResult(employer_id=employer_id, success=True, message="OK")

        result = _run_with_retries("99", succeed_second, {}, max_retries=3)
        assert result.success is True
        assert len(attempts) == 2  # stopped after success, did not exhaust retries

    @patch("ep_parity.utils.runner.time.sleep")
    def test_handles_exception(self, mock_sleep):
        result = _run_with_retries("99", _make_exception_task, {}, max_retries=0)
        assert result.success is False
        assert "Exception" in result.message

    @patch("ep_parity.utils.runner.time.sleep")
    def test_retries_after_exception(self, mock_sleep):
        attempts = []

        def throw_then_succeed(employer_id, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("transient error")
            return TaskResult(employer_id=employer_id, success=True, message="OK")

        result = _run_with_retries("55", throw_then_succeed, {}, max_retries=1)
        assert result.success is True
        assert len(attempts) == 2


# ---------------------------------------------------------------------------
# print_summary
# ---------------------------------------------------------------------------


class TestPrintSummary:
    def test_summary_output(self, caplog):
        """Verify print_summary logs counts without raising."""
        import time as time_mod

        results = [
            TaskResult(employer_id="1", success=True, message="OK"),
            TaskResult(employer_id="2", success=False, message="Fail"),
            TaskResult(employer_id="3", success=True, message="OK"),
        ]
        start = time_mod.time() - 120  # simulate 2-minute run
        with caplog.at_level("INFO"):
            print_summary(results, start, task_name="export")

        combined = caplog.text
        assert "EXPORT SUMMARY" in combined
        assert "Total employers: 3" in combined
        assert "Successful: 2" in combined
        assert "Failed: 1" in combined

    def test_summary_all_success(self, caplog):
        import time as time_mod

        results = [
            TaskResult(employer_id="10", success=True, message="OK"),
        ]
        start = time_mod.time()
        with caplog.at_level("INFO"):
            print_summary(results, start, task_name="compare")
        assert "Failed: 0" in caplog.text
