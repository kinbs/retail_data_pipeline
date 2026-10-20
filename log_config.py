import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Make sure the log directory exists; RotatingFileHandler fails otherwise.
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Avoid duplicate handlers when a module is imported more than once
    # (Airflow re-parses DAG files, pytest re-imports, ...).
    if logger.handlers:
        return logger

    file_handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
