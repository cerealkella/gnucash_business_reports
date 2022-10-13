import logging


def get_logger() -> logging.Logger:
    """sets the preferences for logging for this project

    Returns:
        logging.Logger: logger object
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
    file_handler = logging.FileHandler(f"{__package__}.log")
    file_handler.setLevel(logging.INFO)
    file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
    file_handler.setFormatter(logging.Formatter(file_handler_format))
    logger.addHandler(file_handler)
    return logger
