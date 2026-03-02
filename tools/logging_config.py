"""
Project-wide logging configuration.

Call ``setup_logging()`` once at application startup (e.g. in ``main.py`` or
``api/main.py``) to configure all loggers consistently.

Individual modules should use::

    import logging
    logger = logging.getLogger(__name__)

The module-level loggers automatically inherit the root configuration set
here.
"""

import logging
import sys

LOG_FORMAT = "%(asctime)s [%(levelname)-5s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Per-module default levels (can be overridden via env vars in the future)
MODULE_LEVELS: dict[str, int] = {
    "trading": logging.INFO,
    "agent": logging.INFO,
    "tools": logging.INFO,
    "api": logging.INFO,
    "data.database": logging.WARNING,
    "httpx": logging.WARNING,
    "httpcore": logging.WARNING,
    "urllib3": logging.WARNING,
}

_configured = False


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger and per-module log levels.

    Safe to call multiple times — subsequent calls are no-ops.

    Args:
        level: Root log level (default: INFO).
    """
    global _configured
    if _configured:
        return
    _configured = True

    root = logging.getLogger()
    root.setLevel(level)

    # Remove any pre-existing handlers on root
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    root.addHandler(handler)

    # Apply per-module levels
    for module, mod_level in MODULE_LEVELS.items():
        logging.getLogger(module).setLevel(mod_level)
