# coding: utf-8
import logging

import pymongo
import os

# from mongo_utils import MongoDBClient
# from cloud_operation import update_delete_id
#
#
# def get_mongo_client(db_name, coll_name):
#     mongo_db = MongoDBClient("127.0.0.1", 27017)
#     mongo_db.set_db(db_name)
#     mongo_db.set_collection(coll_name)
#     return mongo_db

# client = get_mongo_client("scenario_db", "resend_message")
# client.update_data({"user_id":"12"}, {'$addToSet':{"deletedID":{"$each":["0","9"]}}})
# re = client.insert_data({"user_id":"12", "deletedID":["55"]})
# client.delete_data({"user_id":"12345"})
# update_delete_id(client, "123", "car", ["1023"])

# print(re)
# import datetime
# print(datetime.datetime.now())

from logging.handlers import RotatingFileHandler
log_file = "D:/IoT_project/server/log/cloudweb"


logger = logging.getLogger("test")

logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")

handler = RotatingFileHandler(log_file, maxBytes=1024, backupCount=10, encoding="utf-8")

handler.setFormatter(formatter)

logger.addHandler(handler)

for i in range(50):
    logger.debug("Testing 12322144244214214214............"+ str(i))
