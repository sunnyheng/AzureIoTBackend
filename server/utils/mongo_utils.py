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
        return {"user_id":"12345", "car":["car"], "phone":["phone"], "hub_conn_str":"HostName=issec-iot.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI="}

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                print("Failed to close mongo object client.")