from loguru import logger

logger.add(
    "logs/runtime_{time}.log",
    rotation="10 MB",    # create a new log file every 10 MB
    retention="7 days",  # keep logs for 7 days
    enqueue=True         # safe across threads/processes
)