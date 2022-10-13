import logging
from datetime import datetime

from .config import get_logdir


def get_logger() -> logging.Logger:
    """sets the preferences for logging for this project

    Returns:
        logging.Logger: logger object
        shout out to Naser Tamimi
        https://towardsdatascience.com/stop-using-print-and-start-using-logging-a3f50bc8ab0
    """
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    # our first handler is a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler_format = "%(asctime)s | %(levelname)s: %(message)s"
    console_handler.setFormatter(logging.Formatter(console_handler_format))
    logger.addHandler(console_handler)

    # the second handler is a file handler
    filename = get_logdir() / datetime.today().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
    file_handler.setFormatter(logging.Formatter(file_handler_format))
    logger.addHandler(file_handler)
    return logger


log = get_logger()
