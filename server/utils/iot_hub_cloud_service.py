# coding: utf-8

#######################################################################################################################################
############################  The service is used to call c2d function from azure sdk  ################################################
#######################################################################################################################################

import time

from azure.iot.hub import IoTHubRegistryManager


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



def check_device_status(resp, device_id, iothub_client, force_clean=False):
    if (resp.connection_state == "Disconnected" or force_clean) and resp.cloud_to_device_message_count >= 20:
        clean_message_queue(device_id, iothub_client)


def clean_message_queue(device_id, iothub_client):
    try:
        count = iothub_client.purge_queue_message(device_id)
        print("The device: " + device_id + "is disconnected, and purge the queue message:" + str(count))
        # logger.info("The device: " + device_id + "is disconnected, and purge the queue message:" + str(count))
    except Exception as pe:
        print("Failed to check the d2c message:" + device_id + ". More details:" + str(pe))

def generate_props(operation_type):
    cur_timestamp = int(time.time())
    stamp = (cur_timestamp + 500) * 1000
    props = {}
    props.update(expiryTimeUtc=stamp)
    props.update(contentType="application/json")
    props.update(operation_type=operation_type)
    props.update(currentTime=cur_timestamp)
    return props


def check_and_send_c2d_message(iothub_client, device_id, mes_str, mes_type, logger):
    try:
        resp = iothub_client.get_device_info(device_id)
        check_device_status(resp, device_id, iothub_client)
        logger.info("[" + mes_type + "]" + "The device (" + device_id + ") status is:" + resp.connection_state)
        if resp.connection_state == "Connected":
            props = generate_props(mes_type)
            logger.info("[" + mes_type + "]" + "Send data:" + mes_str)
            iothub_client.iot_send_message(device_id, mes_str, props)
            logger.info("[" + mes_type + "]" + "Send c2d message to " + device_id+ " successfully")
            return True
        else:
            return False

    except Exception as se:
        logger.error("[" + mes_type + "]" + "Failed to send c2d message to " + device_id + ". More details:" + str(se))
        print("[" + mes_type + "]" + "Failed to send c2d message to " + device_id)
        return None
