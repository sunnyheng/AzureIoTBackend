# coding: utf-8

#######################################################################################################################################
############################  The service is used to call c2d function from azure sdk  ################################################
#######################################################################################################################################

import time

from azure.iot.hub import IoTHubRegistryManager
from iot_log import logger



class IoTHubManager(object):
    def __init__(self, conn_str):
        super().__init__()
        self.conn_str = conn_str
        self.client = self.create_client()

    def create_client(self):
        return IoTHubRegistryManager(self.conn_str)

    # c2d message expire_time is timestamp
    def iot_send_message(self, device_id, data_str, props):
        self.client.send_c2d_message(device_id, data_str, properties=props)


    def get_device_info(self, device_id):
        return self.client.get_device(device_id)

    def purge_queue_message(self, device_id):
        res = self.client.protocol.cloud_to_device_messages.purge_cloud_to_device_message_queue(device_id)
        purged_count = res.total_messages_purged
        return purged_count

    def delete_client(self):
        if self.client:
            del self.client