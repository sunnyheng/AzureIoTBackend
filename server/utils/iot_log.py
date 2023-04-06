# coding: utf-8
import os
import logging
from logging.handlers import RotatingFileHandler


def setup_log(log_name):
    log = logging.getLogger(log_name)
    abs_file = os.path.join("D:\\IoT_project\\server\\log", log_name)
    log.setLevel(logging.DEBUG)

    handler = RotatingFileHandler(abs_file, maxBytes=1024*1024*5, backupCount=10, encoding="utf-8")
    formatter = logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log


logger = setup_log("iot_log")


def setup_handler(log_name="cloud-web"):
    logging.basicConfig(level=logging.INFO)
    abs_file = os.path.join("D:\\IoT_project\\server\\log", log_name)

    handler = RotatingFileHandler(abs_file, maxBytes=1024*1024*5, backupCount=10, encoding="utf-8")
    formatter = logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    return handler
