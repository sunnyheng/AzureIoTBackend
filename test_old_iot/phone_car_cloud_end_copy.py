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
from azure.core.exceptions import ResourceNotFoundError

# moved to send_c2d_message.py
# from azure.iot.hub import IoTHubRegistryManager

from connction_string import IOTHUB_PHONE_CONN, IOTHUB_CAR_CONN, IOTHUB_CONNS, PHONE_DEVICE_ID, CAR_DEVICE_ID, DEVICE_IDS,\
    BLOB_CONN_STR, CONTAINER_NAME, SCENARIO_TMPL_NAME, CONN_EVENT_HUB, CONSUMER_GROUP
from send_c2d_message import send_mes_to_multi_iothub, send_mes_to_iothub

# from init_logger import logger
from init_logger import setup_log
logger = setup_log("cloud_iot")

# event_hub is specified
# read the message from event hub in iot hub
# CONN_EVENT_HUB_PHONE="Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
# CONN_EVENT_HUB_CAR="Endpoint=sb://iothub-ns-iot-1584346-5c22537e4b.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4=;EntityPath=iot"
consumer_group="$Default"

# [moved to connction_string.py] connect iothub string, attention the string position in list
#
# IOTHUB_PHONE_CONN = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
# IOTHUB_CAR_CONN = "HostName=iot.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4="
# IOTHUB_CONNS = [IOTHUB_PHONE_CONN, IOTHUB_CAR_CONN]
# PHONE_DEVICE_ID = "mydevice"
# CAR_DEVICE_ID = "device20221220"
# DEVICE_IDS = [PHONE_DEVICE_ID, CAR_DEVICE_ID]

thread_lock = threading.Lock()


def on_event_batch(partition_context, events):
    # 'body', 'body_as_json', 'body_as_str', 'body_type', 'content_type', 'correlation_id', 'enqueued_time',
    # 'from_message_content', 'message', 'message_id', 'offset', 'partition_key', 'properties', 'raw_amqp_message', 'sequence_number', 'system_properties'
    for event in events:
        thread_lock.acquire()
        # logger.info("Received event from partition: {}.".format(partition_context.partition_id))
        # event.body_as_str()
        logger.info("[iot] Telemetry received: "+ event.body_as_str())

        # logic: when the message send from car, the message should be re-send to phone by server
        # logic: when the message send from phone, the message should be re-send to car by server
        # when correlation_id(use as tag to define target side to send message) is phone, the save need to send the message to phone
        logger.info("[iot] correlation id:"+ event.correlation_id)
        print("correlation id:" + event.correlation_id)
        if event.correlation_id.lower() == "car":
            action_by_json(event.body_as_str(), IOTHUB_PHONE_CONN, PHONE_DEVICE_ID)
        if event.correlation_id.lower() == "phone":
            action_by_json(event.body_as_str(), IOTHUB_CAR_CONN,
                           CAR_DEVICE_ID)
        # print("Properties (set by device): ", event.properties)
        # print("System properties (set by IoT Hub): ", event.system_properties)
        thread_lock.release()
        # partition_context.update_checkpoint()
        partition_context.update_checkpoint(event)


# An error reminder if there is any error during read message from iothub eventhub
def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        logger.error("[iot] An exception: %s occurred during receiving from Partition: %s." %(
            partition_context.partition_id, error))
    else:
        logger.error("[iot] An exception: %s occurred during the load balance process." % error)


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
        logger.info("[iot] Uploaded message is string, already finished parsing to json")
    action = event_json.get("operation", None)
    # need to send

    if action == "ADD":
        # option 1. new json upload
        # save data to blob;
        logger.info("[iot] Start save data and send back to iot hub")
        # print("Start save data and send back to iot hub")
        try:
            save_str_to_blobs(event_str)
            # sent data back to iot.
            data_with_tag = json.dumps(event_json)
            send_mes_to_iothub(data_with_tag, iothub_conn_str, device_id, logger)
        except Exception as add_e:
            logger.error("Failed to handle added scenario, more details:" + str(add_e))

        # option 2. modify the json file
        # should check the if the data exists in db, use arg overwrite=True to solve it

    if action == "SYNC":
        # read date from blob and sent to iot
        try:
            blob_str = list_azure_blobs()
            send_mes_to_multi_iothub(blob_str, IOTHUB_CONNS, DEVICE_IDS, logger)
        except Exception as syc_e:
            logger.error("Failed to handle sync scenario, more details:" + str(syc_e))


    if action == "DELETE":
        # mark the data in target db/blob
        try:
            delete_resp = {}
            scenario_id_list = event_json.get('id')
            id_list_json = json.loads(scenario_id_list)
            container_name = CONTAINER_NAME
            deleted_id_list = del_azure_blob(id_list_json, container_name)
            if deleted_id_list:
                delete_resp["id"] = json.dumps(deleted_id_list)
                delete_resp["operation"] = "DELETE"
                delete_resp["ScenarioType"] = "ScenarioUser"
                message = json.dumps(delete_resp)
                logger.info("Delete message:" + message)
                send_mes_to_multi_iothub(message, IOTHUB_CONNS, DEVICE_IDS, logger)
        except Exception as del_e:
            logger.error("Failed to handle delete scenario, more details:" + str(del_e))


# BLOB_CONN_STR='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
# CONTAINER_NAME = "iotcontainer"
# BLOB_FILE_NAME = "test1.json"
def save_str_to_blobs(event_data):
    logger.info("Start to save blob list to azure")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    print('start tp get json content...')
    event_json = json.loads(event_data)
	# blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
	# blob_str = event_data
    for key, value in event_json.items():
        if key == "ScenarioUser":
            parseBlob(value, container_client)
    blob_service_client.close()


def parseBlob(str_val, container_client):
    if str_val:
        val_list = json.loads(str_val)
        if not isinstance(val_list, list):
            return
        for blob in val_list:

            file_id = str(blob.get('id'))
            print("file_id:", file_id)
            container_client.upload_blob(file_id, json.dumps(blob), overwrite=True)
            logger.info("[iot] Finished to upload file:" + file_id)
            print("Finished to upload file:" + file_id)


def del_azure_blob(id_list, container_name, container_client=None):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(container_name)
    deleted_id = []
    for id in id_list:
        try:
            logger.info("Start delete the azure blob:" + str(id))
            blob_client = container_client.get_blob_client(str(id))
            blob_client.delete_blob()
            deleted_id.append(str(id))
        except ResourceNotFoundError as ne:
            logger.warning("Not found the blob, maybe cache data in local(phone or car)")
            deleted_id.append(str(id))

        except Exception as e:
            print('Failed to delete azure blob:' + str(id))
            print('Failed to delete azure blob2:' + str(e))

    return deleted_id

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


# def list_blobs_test():
#     print("Start list blob from storage container.")
#     blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
#     container_client = blob_service_client.get_container_client(CONTAINER_NAME)
#     blobs = container_client.list_blobs()
#     data_dict = {}
#     blob_client = container_client.get_blob_client("user_vin_1001")
#     blobstr = blob_client.download_blob().readall().decode("utf-8")
#     data_dict["user_vin_1001"] = blobstr
#     print('data_dict:', data_dict)
#     blob_service_client.close()
#     return json.dumps(data_dict)


# It is plan B if the list_blobs cannot work via the blob.values
def list_azure_blobs():
    data = {}
    user_dict = list_scenario_blobs(CONTAINER_NAME, "ScenarioUser")
    logger.info('user_dict:' + str(user_dict))
    tmp_dict = list_scenario_blobs(SCENARIO_TMPL_NAME, "ScenarioSquare")
    user_dict.update(tmp_dict)
    user_dict["operation"] = "SYNC"
    # use for feedback info
    user_dict["iothub-ack"] = "negative"

    return json.dumps(user_dict)


def list_scenario_blobs(container_name, type):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs()
    data_dict = {}
    data_list = []
    for blob in blobs:
        logger.info("[iot] List the blob:" + blob.name)
        blob_client = container_client.get_blob_client(blob.name)
        blobstr = blob_client.download_blob().readall().decode("utf-8")
        blob_json = json.loads(blobstr)
        data_list.append(blob_json)
        blob_client.close()
    data_dict[type] = json.dumps(data_list)
    blob_service_client.close()
    # print("data_dict:", json.dumps(data_dict))
    return data_dict


# ==========================moved to [send_c2d_message.py]========================================
# def send_mes_to_multi_iothub(blob_str, iothub_conn_list, device_id_list):
#     if isinstance(iothub_conn_list, list) and isinstance(device_id_list, list):
#         for ind, conn_str in enumerate(iothub_conn_list):
#             send_mes_to_iothub(blob_str, conn_str, device_id_list[ind])
#
#
# #device_id should be get from db, the mapping file should be stored in db
# def send_mes_to_iothub(blob_str, iothub_conn_str, device_id):
#     try:
#         logger.info("[iot] start create iothub client.")
#         # Create IoTHubRegistryManager
#         registry_manager = IoTHubRegistryManager(iothub_conn_str)
#         # need to encode charset if Chinese characters are included
#         # blob_str = json.dumps(blob_str)
#         # print("Start to covernt to utf-8.")
#         blob_str.encode("utf-8")
#         props = {}
#         # optional: assign system properties
#         props.update(messageId="message_1")
#         props.update(correlationId=device_id)
#         props.update(contentType="application/json")
#         try:
#             logger.info("[iot] Start send c2d message.")
#             print("Start send c2d message.")
#             registry_manager.send_c2d_message(device_id, blob_str, properties=props)
#             logger.info("[iot] send message(C2D) to device:%s successfully." % device_id)
#             print("send message(C2D) to device:%s successfully." % device_id)
#         except Exception as e:
#             print("Failed to send c2d message, more details:"+ e)
#             logger.error("[iot] Failed to send c2d message, more details:"+ str(e))
#             logger.error("[iot] Failure message:" + blob_str)
#         # delete the registry manager client after finish sending message
#         del registry_manager
#
#     except Exception as ex:
#         print("Unexpected error {0}" % ex)
#         logger.error("[iot] Unexpected error: %s" % ex)
#         logger.error("[iot] Unexpected json string:" + blob_str)
#     except KeyboardInterrupt:
#         print("IoT Hub C2D Messaging service sample stopped")
#         logger.warning("[iot] IoT Hub C2D Messaging service sample stopped")
# ==========================moved to [send_c2d_message.py]========================================

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
    logger.info("[iot] Start to create client to connect the Event hub that built-in iot hub.")
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
        logger.warning("[iot] Receiving has stopped by keyboard interrtpt")


if __name__ == '__main__':
    thread_iot = ConnThread("iot backend", CONN_EVENT_HUB, CONSUMER_GROUP)
    # thread_car = ConnThread("car", CONN_EVENT_HUB_CAR, consumer_group)
    thread_iot.start()
    # thread_car.start()