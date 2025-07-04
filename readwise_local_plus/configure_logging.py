import logging
from logging.handlers import RotatingFileHandler

from rich.logging import RichHandler

from readwise_local_plus.config import UserConfig, fetch_user_config


def setup_logging(user_config: UserConfig = fetch_user_config()) -> None:
    """
    Setup logging.

    Parameters
    ----------
    user_config: UserConfig
        A user config object to supply the log file path.
    """

    # Prevent reconfiguration if already set up.
    if logging.getLogger().hasHandlers():
        return

    log_file = user_config.log_path
    log_file.parent.mkdir(parents=True, exist_ok=True)

    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    console_handler = RichHandler(rich_tracebacks=True)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))

    file_handler = RotatingFileHandler(log_file, maxBytes=2_000_000)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(FORMAT))

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
