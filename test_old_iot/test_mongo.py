# coding: utf-8

import pymongo

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
import datetime
print(datetime.datetime.now())

