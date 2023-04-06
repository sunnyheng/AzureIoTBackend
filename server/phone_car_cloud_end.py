# coding: utf-8

# -------------------------------------------------------------------------------------------------
# User: Yangyang Zhao
# Mudule: Azure SDK for Python, azure-eventhub, azure-iot-device, azure-iot-hub, azure-storage-blob
#
# -------------------------------------------------------------------------------------------------

"""
The functionality of this module is to save/read the azure iot hub data to azure blob.
It depends on the content of message that receive from iot hub, and will do the coresponding operation
initial, sync, update, delete
Enhancement of the cloud_end, this service handle andriod and car side, use this to test three sides communication
"""

import threading
import time

from azure.eventhub import EventHubConsumerClient

from connction_string import CONN_EVENT_HUB, CONSUMER_GROUP
from cloud_operation import action_by_received


from utils.iot_log import logger


thread_lock = threading.Lock()

# event attr: [ 'body', 'body_as_json', 'body_as_str', 'body_type', 'content_type', 'correlation_id', 'enqueued_time', 'from_message_content',
# 'message', 'message_id', 'offset', 'partition_key', 'properties', 'raw_amqp_message', 'sequence_number', 'system_properties']
def on_event_batch(partition_context, events):
    for event in events:
        thread_lock.acquire()
        logger.info("[iot] Telemetry received: " + event.body_as_str())
        print('Start')

        # logger.info("[iot] user id:"+ event.correlation_id)
        # print("user id:" + event.correlation_id)
        try:
            action_by_received(event)
        except Exception as ae:
            logger.error("Failed to resolve the event. More details:" + str(ae))

        thread_lock.release()
        partition_context.update_checkpoint(event)


# An error reminder if there is any error during read message from iothub eventhub
def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        logger.error("[iot] An exception: %s occurred during receiving from Partition: %s." %(
            partition_context.partition_id, error))
    else:
        logger.error("[iot] An exception: %s occurred during the load balance process." % error)


class ConnThread(threading.Thread):
    def __init__(self, thread_id, event_hub_conn, consumer_group):
        super().__init__(name=thread_id)
        # self.thread_id = thread_id
        self.event_hub_conn = event_hub_conn
        self.consumer_group = consumer_group
        # self.iothub_conn = iothub_conn
        # self.device_id = device_id

    def run(self):
        print("The thread %s start." % self.name)
        logger.info("[iot] The thread %s start." % self.name)
        event_hub_conn_client(self.event_hub_conn, self.consumer_group)


def event_hub_conn_client(event_hub_conn, consumer_group):

    client = EventHubConsumerClient.from_connection_string(
        conn_str=event_hub_conn,
        consumer_group=consumer_group)
    try:
        with client:
            time.sleep(1)
            client.receive_batch(
                on_event_batch=on_event_batch,
                on_error=on_error)
    except KeyboardInterrupt:
        print("Receiving has stopped by keyboard interrtpt")
        logger.warning("[iot] Receiving has stopped by keyboard interrtpt")


if __name__ == '__main__':
    thread_iot = ConnThread("iot backend", CONN_EVENT_HUB, CONSUMER_GROUP)
    # thread_car = ConnThread("car", CONN_EVENT_HUB_CAR, consumer_group)
    thread_iot.start()
    # thread_car.start()