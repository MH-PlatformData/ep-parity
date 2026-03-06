import logging
import sys


def setup_logging(verbose: bool = False, log_file: str = None) -> logging.Logger:
    """Configure logging for the application. Called once from CLI entry point."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s - %(levelname)s - %(message)s"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
    return logging.getLogger("ep_parity")


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the ep_parity namespace."""
    return logging.getLogger(f"ep_parity.{name}")
