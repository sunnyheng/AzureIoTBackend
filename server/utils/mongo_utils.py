# coding: utf-8


import pymongo

class MongoDBClient(object):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        self.client = self.create_client()
        self.db = None
        self.collection = None

    def create_client(self):
        try:
            return pymongo.MongoClient(host=self.host, port=self.port)

        except Exception as e:
            print("Failed to create mongodb client:" + str(e))

    def set_db(self, db_name):
        try:
            if self.client.get_database(db_name):
                self.db = self.client.get_database(db_name)
        except Exception as e:
            print("Failed to set DB, more details:" + set(e))

    def set_collection(self, coll_name):
        if self.db:
            if self.db.get_collection(coll_name) is not None:
                self.collection = self.db.get_collection(coll_name)
        else:
            print("Does not exist the database, please check if it is created.")

    def insert_data(self, doc):
        if self.collection:
            self.collection.insert_one(doc)

    def delete_data(self, condition):
        if self.collection:
            self.collection.delete_one(condition)

    def select_data(self, condition):
        if self.collection:
            return self.collection.find_one(condition)

    # if record exists, update the record, else insert a new record
    # condition = {"user_id":"12345", "device_id": "car123"}
    # doc = {"deletedID":["1031", "1032"]}
    # append to deletedID if it exists {'$addToSet':{"deletedID":{"$each":["1030","1035"]}}}
    def update_data(self, condition, doc):
        if self.collection:
            self.collection.update(condition, doc)

    def get_re(self):
        return {"user_id":"12345", "car":["car"], "phone":["phone"], "hub_conn_str":"HostName=issec-dev.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec="}

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                print("Failed to close mongo object client.")


def update_delete_id(mongo_client, user_id, device_id, deleted_id_list):
    condition = {"user_id": user_id, "device_id": device_id}
    doc = {"user_id": user_id, "device_id": device_id, "deleted_id": deleted_id_list}
    updated_doc = {'$addToSet':{"deleted_id":{"$each": deleted_id_list}}}
    if mongo_client.select_data(condition):
        mongo_client.update_data(condition, updated_doc)
    else:
        mongo_client.insert_data(doc)



def get_is_bought(mongo_client, condition, square_id_list):
    try:
        select_re = mongo_client.select_data(condition)
        if select_re:
            re_square_id_list = select_re.get("square_ids")
            if not square_id_list[0] in re_square_id_list:
                return False
            return True
        else:
            return None
    except Exception as e:
        print("Failed to update the square id of is bought.")
        return None


def update_is_bought(mongo_client, user_id, square_id_list):
    condition = {"user_id": user_id}
    status = get_is_bought(mongo_client, condition, square_id_list)

    doc = {"user_id": user_id, "square_ids": square_id_list, "is_bought": True}
    updated_doc = {'$addToSet': {"square_ids": {"$each": square_id_list}}}
    if status is None:
        mongo_client.insert_data(doc)
    if status is False:
        mongo_client.update_data(condition, updated_doc)


def get_mongo_client(db_name, coll_name):
    mongo_db = MongoDBClient("127.0.0.1", 27017)
    mongo_db.set_db(db_name)
    mongo_db.set_collection(coll_name)
    return mongo_db


def get_buy_count(mongo_client, condition):
    count = 0
    try:
        result = mongo_client.select_data(condition)
        if result:
            count = result.get("count")
    except Exception as e:
        print("Failed to update the value of buy count.")
    return count


def update_buy_count(mongo_client, square_id):
    condition = {"square_id": square_id}
    count = get_buy_count(mongo_client, condition)
    update_doc = {"$inc": {"count": 1}}
    if count == 0:
        mongo_client.insert_data({"square_id": square_id, "count": 1})
    else:
        mongo_client.update_data(condition, update_doc)
    count = count + 1
    return count
