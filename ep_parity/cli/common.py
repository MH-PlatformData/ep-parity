"""Shared CLI options and utilities for all subcommands."""

import functools

import click

from ep_parity.core.config import AppConfig, load_employer_ids_from_file, resolve_db_target


def employer_ids_options(f):
    """Decorator adding --emp_ids and --emp_ids_file options to a command."""

    @click.option(
        "--emp_ids",
        multiple=True,
        type=str,
        help="One or more employer IDs (e.g., --emp_ids 150 289 300).",
    )
    @click.option(
        "--emp_ids_file",
        type=click.Path(exists=True),
        help="Text file with one employer ID per line (# comments and blank lines ignored).",
    )
    @functools.wraps(f)
    def wrapper(*args, emp_ids, emp_ids_file, **kwargs):
        merged = list(emp_ids)
        if emp_ids_file:
            merged.extend(load_employer_ids_from_file(emp_ids_file))
        if not merged:
            raise click.UsageError(
                "At least one employer ID required. Use --emp_ids or --emp_ids_file."
            )
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for eid in merged:
            if eid not in seen:
                seen.add(eid)
                unique.append(eid)
        kwargs["emp_ids"] = unique
        return f(*args, **kwargs)

    return wrapper


def batch_options(f):
    """Decorator adding --parallel and --max_workers options."""

    @click.option("--parallel", is_flag=True, help="Run employers in parallel.")
    @click.option(
        "--max_workers",
        type=int,
        default=None,
        help="Max parallel workers (default: min(4, num_employers)).",
    )
    @click.option(
        "--max_retries",
        type=int,
        default=0,
        help="Number of retry attempts per employer on failure.",
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def get_config(ctx: click.Context) -> AppConfig:
    """Build an AppConfig from the CLI context."""
    return AppConfig(config_dir=ctx.obj.get("config_dir"))
