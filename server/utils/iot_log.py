# coding: utf-8
import os
import re

import logging
from logging.handlers import TimedRotatingFileHandler


def setup_log(log_name):
    logger = logging.getLogger(log_name)
    log_path = os.path.join("C:\\project\\backend\\log", log_name)
    logger.setLevel(logging.DEBUG)
    file_handler = TimedRotatingFileHandler(filename=log_path, when="MIDNIGHT", interval=1, backupCount=30, encoding="utf-8")
    file_handler.suffix = "%Y-%m-%d.log"
    file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s"))
    logger.addHandler(file_handler)
    return logger


logger = setup_log("iot_cloud")

