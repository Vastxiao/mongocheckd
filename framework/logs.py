
__all__ = ["logger"]

from sys import stderr
from loguru import logger

from .config import get_settings

settings = get_settings()

logger.configure(
    handlers=[
        dict(sink=stderr, level=settings.log_level, enqueue=True),
        # dict(sink=stderr, level="INFO", enqueue=True),
        # dict(sink="file_{time}.log", enqueue=True, serialize=True, rotation="500 MB", retention=5, level="INFO")
    ]
)
