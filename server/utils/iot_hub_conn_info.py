# coding: utf-8

import pymongo


class IoTHubConnInfo(object):
    def __init__(self, user_id, device_type, device_id, operation_type):
        super().__init__()
        self.user_id = user_id
        self.device_type = device_type
        self.device_id = device_id
        self.operation_type = operation_type
        self.mongo_client = None
        self.conn_str = None
        self.record = {}
        self.car_list = []
        self.phone_list = []

    def parse_record(self):
        self.set_conn_str()
        self.set_phone_list()
        self.set_car_list()

    def get_result_by_user_id(self, condition={}):
        # result = db_client.select_data_by_user_id(condition)
        self.record = {"user_id":"12345", "car":["car"], "phone":["phone"],
                "hub_conn_str": "HostName=issec-dev.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec="
                }
        return self.record

    def set_conn_str(self):
        self.conn_str = self.record.get("hub_conn_str")

    def set_car_list(self):
        self.car_list = self.record.get("car")
        print('self.car_list ' , self.car_list )

    def set_phone_list(self):
        self.phone_list = self.record.get("phone")


def get_info(user_id, received_device_type, received_device_id, received_op_type):
    iothub_conn_info_obj = IoTHubConnInfo(user_id, received_device_type, received_device_id, received_op_type)
    iothub_conn_info_obj.get_result_by_user_id()

    iothub_conn_info_obj.parse_record()
    return iothub_conn_info_obj
