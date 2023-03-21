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
        self.record = {"user_id":123456, "car":["car"], "phone":["phone"],
                "hub_conn_str": "HostName=issec-iot.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI="
                }
        return self.record

    def set_conn_str(self):
        self.conn_str = self.record.get("hub_conn_str")

    def set_car_list(self):
        self.car_list = self.record.get("car")

    def set_phone_list(self):
        self.phone_list = self.record.get("phone")

