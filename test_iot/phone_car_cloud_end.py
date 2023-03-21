# coding: utf-8

# --------------------------------------------------------------------------------------------
# User: Yangyang Zhao
# Mudule: Azure SDK for Python, azure-eventhub, azure-device, azure-storage-blob
#
# --------------------------------------------------------------------------------------------

"""
The functionality of this module is to save/read the azure iot hub data to azure blob.
It depends on the content of message that receive from iot hub, and will do the coresponding operation
initial, sync, update, delete
Enhancement of the cloud_end, this service handle andriod and car side, use this to test three sides communication
"""

import json, threading
from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient
from azure.iot.hub import IoTHubRegistryManager

from init_logger import logger

# event_hub is specified
# read the message from event hub in iot hub
CONN_EVENT_HUB_PHONE="Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
CONN_EVENT_HUB_CAR="Endpoint=sb://iothub-ns-iot-1584346-5c22537e4b.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4=;EntityPath=iot"
consumer_group="$Default"

# connect iothub string, attention the string position in list
IOTHUB_PHONE_CONN = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
IOTHUB_CAR_CONN = "HostName=iot.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4="
IOTHUB_CONNS = [IOTHUB_PHONE_CONN, IOTHUB_CAR_CONN]
PHONE_DEVICE_ID = "mydevice"
CAR_DEVICE_ID = "device20221220"
DEVICE_IDS = [PHONE_DEVICE_ID, CAR_DEVICE_ID]

thread_lock = threading.Lock()


def on_event_batch(partition_context, events):
    for event in events:
        thread_lock.acquire()
        # logger.info("Received event from partition: {}.".format(partition_context.partition_id))
        # event.body_as_str()
        logger.info("Telemetry received: "+ event.body_as_str())

        # logic: when the message send from car, the message should be re-send to phone by server
        # logic: when the message send from phone, the message should be re-send to car by server
        # when correlation_id(use as tag to define target side to send message) is phone, the save need to send the message to phone
        logger.info("correlation id:"+ event.correlation_id)
        print("correlation id:" + event.correlation_id)
        if event.correlation_id.lower() == "car":
            action_by_json(event.body_as_str(), IOTHUB_PHONE_CONN, PHONE_DEVICE_ID)
        if event.correlation_id.lower() == "phone":
            action_by_json(event.body_as_str(), IOTHUB_CAR_CONN, CAR_DEVICE_ID)
        # print("Properties (set by device): ", event.properties)
        # print("System properties (set by IoT Hub): ", event.system_properties)
        thread_lock.release()
    partition_context.update_checkpoint()


# An error reminder if there is any error during read message from iothub eventhub
def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        logger.error("An exception: %s occurred during receiving from Partition: %s." %(
            partition_context.partition_id, error))
    else:
        logger.error("An exception: %s occurred during the load balance process." % error)


# this function need to input iothub_conn_str and device_id to send message to specified device
# this time the devices were create in different iothub(Maybe they should be create in the same iothub, because they belong to one user)
# Currently, only two devices(phone and car), when receive upload sign from car, then server will send data to phone
# If receive upload sign from phone, server will send data to car.
# If the action parameter is "SYNC", server will send data to both car and phone.
def action_by_json(event_str, iothub_conn_str, device_id):
    event_json = event_str
    # print("Start do operation by json:", event_str)
    if isinstance(event_str, str):
        event_str.encode("utf-8")
        event_json = json.loads(event_str)
        logger.info("Uploaded message is string, already finished parsing to json")
    action = event_json.get("operation", None)
    # need to send
    if action is None:
        # option 1. new json upload
        # save data to blob;
        logger.info("Start save data and send back to iot hub")
        # print("Start save data and send back to iot hub")
        save_str_to_blobs(event_str)
        # sent data back to iot.
        send_mes_to_iothub(event_str, iothub_conn_str, device_id)

        # option 2. modify the json file
        # should check the if the data exists in db, use arg overwrite=True to solve it

    if action == "SYNC":
        # read date from blob and sent to iot
        blob_str = list_blobs()
        send_mes_to_multi_iothub(blob_str, IOTHUB_CONNS, DEVICE_IDS)

    if action == "DELELTE":
        # mark the data in target db/blob
        pass


BLOB_CONN_STR='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
CONTAINER_NAME = "iotcontainer"
BLOB_FILE_NAME = "test1.json"
def save_str_to_blobs(event_data):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    # print('start tp get json content...')
    event_json = json.loads(event_data)
	# blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
	# blob_str = event_data
    for key, value in event_json.items():
        container_client.upload_blob(key, value, overwrite=True)
        logger.info("Finished to upload file:" + key)
        print("Finished to upload file:" + key)
    blob_service_client.close()


# ================upload a single blob=============================================
# def save_str_to_blob(event_data):
# 	blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
# 	container_client = blob_service_client.get_container_client(CONTAINER_NAME)
# 	blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
# 	# blob_str = event_data
# 	try:
# 		blob_client.upload_blob(data=event_data)
# 	except Exception as e:
# 		print("Failed to upload data string to blob, more details:", str(e))
# 	blob_client.close()
# ==================================================================================
# ================read a single file from blob======================================
# def read_blob():
# 	blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
# 	container_client = blob_service_client.get_container_client(CONTAINER_NAME)
# 	blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
# 	blob_str = blob_client.download_blob().readall().decode("utf-8")
# 	print("Testing get blob str:")
# 	print(blob_str)
# 	blob_client.close()
# 	return blob_str
# ==================================================================================

# def list_blobs_backup():
#     print("Start list blob from storage container.")
#     blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
#     container_client = blob_service_client.get_container_client(CONTAINER_NAME)
#     blobs = container_client.list_blobs()
#     data_dict = {}
#     for blob in blobs:
#         print("List the blob:", blob.name)
#         data_dict[blob.name] = str(blob.values)
#     print('data_dict:', data_dict)
#     blob_service_client.close()
#     return json.dumps(data_dict, ensure_ascii=False)


def list_blobs_test():
    print("Start list blob from storage container.")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs()
    data_dict = {}
    blob_client = container_client.get_blob_client("user_vin_1001")
    blobstr = blob_client.download_blob().readall().decode("utf-8")
    data_dict["user_vin_1001"] = blobstr
    print('data_dict:', data_dict)
    blob_service_client.close()
    return json.dumps(data_dict)


# It is plan B if the list_blobs cannot work via the blob.values
def list_blobs():
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs()
    data_dict = {}
    for blob in blobs:
        logger.info("List the blob:" + blob.name)
        blob_client = container_client.get_blob_client(blob.name)
        blobstr = blob_client.download_blob().readall().decode("utf-8")
        data_dict[blob.name] = blobstr
        blob_client.close()
    blob_service_client.close()
    # print("data_dict:", json.dumps(data_dict))
    return json.dumps(data_dict)


def send_mes_to_multi_iothub(blob_str, iothub_conn_list, device_id_list):
    if isinstance(iothub_conn_list, list) and isinstance(device_id_list, list):
        for ind, conn_str in enumerate(iothub_conn_list):
            send_mes_to_iothub(blob_str, conn_str, device_id_list[ind])


#device_id should be get from db, the mapping file should be stored in db
def send_mes_to_iothub(blob_str, iothub_conn_str, device_id):
    try:
        # Create IoTHubRegistryManager
        registry_manager = IoTHubRegistryManager(iothub_conn_str)
        # need to encode charset if Chinese characters are included
        # blob_str = json.dumps(blob_str)
        # print("Start to covernt to utf-8.")
        blob_str.encode("utf-8")
        props = {}
        # optional: assign system properties
        props.update(messageId="message_1")
        props.update(correlationId=device_id)
        props.update(contentType="application/json")
        try:
            registry_manager.send_c2d_message(device_id, blob_str, properties=props)
            logger.info("send message(C2D) to device:%s successfully." % device_id)
            print("send message(C2D) to device:%s successfully." % device_id)
        except Exception as e:
            print("Failed to send c2d message, more details:"+ e)
            logger.error("Failed to send c2d message, more details:"+ str(e))
            logger.error("Failure message:" + blob_str)
        # delete the registry manager client after finish sending message
        del registry_manager

    except Exception as ex:
        print("Unexpected error {0}" % ex)
        logger.error("Unexpected error: %s" % ex)
        logger.error("Unexpected json string:" + blob_str)
    except KeyboardInterrupt:
        print("IoT Hub C2D Messaging service sample stopped")
        logger.warning("IoT Hub C2D Messaging service sample stopped")


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
        logger.info("The thread %s start." % self.name)
        event_hub_conn_client(self.event_hub_conn, self.consumer_group)



def event_hub_conn_client(event_hub_conn, consumer_group):
    logger.info("Start to create client to connect the Event hub that built-in iot hub.")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=event_hub_conn,
        consumer_group=consumer_group)
    try:
        with client:
            client.receive_batch(
                on_event_batch=on_event_batch,
                on_error=on_error)
    except KeyboardInterrupt:
        print("Receiving has stopped by keyboard interrtpt")
        logger.warning("Receiving has stopped by keyboard interrtpt")


if __name__ == '__main__':
    thread_phone = ConnThread("phone", CONN_EVENT_HUB_PHONE, consumer_group)
    thread_car = ConnThread("car", CONN_EVENT_HUB_CAR, consumer_group)
    thread_phone.start()
    thread_car.start()