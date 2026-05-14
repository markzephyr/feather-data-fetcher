import logging
import sys
from typing import Any

# Configure standard logger for the open-source package
logger = logging.getLogger("feather_fetcher")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert any value to a float, returning default on failure."""
    try:
        if val is None:
            return default
        f = float(val)
        if f != f:  # Check for NaN
            return default
        return f
    except (ValueError, TypeError):
        return default
