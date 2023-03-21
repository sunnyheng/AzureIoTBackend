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
"""

import json
from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient
from azure.iot.hub import IoTHubRegistryManager


# event_hub is specified
# read the message from event hub in iot hub
CONN_EVENT_HUB_STR="Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
consumer_group="$Default"
def on_event_batch(partition_context, events):
    for event in events:
        print("Received event from partition: {}.".format(partition_context.partition_id))
        # event.body_as_str()
        print("Telemetry received: ", event.body_as_str())
        action_by_json(event.body_as_str())
        # print("Properties (set by device): ", event.properties)
        # print("System properties (set by IoT Hub): ", event.system_properties)
    partition_context.update_checkpoint()


# An error reminder if there is any error during read message from iothub eventhub
def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id, error))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


def action_by_json(event_str):
    event_json = event_str
    print("Start do operation by json:", event_str)
    if isinstance(event_str, str):
        # print('event is str:', event_str)
        event_json = json.loads(event_str)
    print("Finish parse json.")
    action = event_json.get("operation", None)
    if action is None:
        # option 1. new json upload
        # save data to blob;
        print("Start save data and send back to iot hub")
        save_str_to_blobs(event_str)
        # sent data back to iot.
        send_mes_to_iothub(event_str)

        # option 2. modify the json file
        # should check the if the data exists in db, use arg overwrite=True to solve it

    if action == "SYNC":
        # read date from blob and sent to iot
        blob_str = list_blobs()
        send_mes_to_iothub(blob_str)

    if action == "DELELTE":
        # mark the data in target db/blob
        pass


BLOB_CONN_STR='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
CONTAINER_NAME = "iotcontainer"
BLOB_FILE_NAME = "test1.json"
def save_str_to_blobs(event_data):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    print('start tp get json content...')
    event_json = json.loads(event_data)
	# blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
	# blob_str = event_data
    for key, value in event_json.items():
        print("Start to upload file:", key)
        container_client.upload_blob(key, value, overwrite=True)
        print("Finish to upload file.")
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
        print("List the blob:", blob.name)
        blob_client = container_client.get_blob_client(blob.name)
        blobstr = blob_client.download_blob().readall().decode("utf-8")
        data_dict[blob.name] = blobstr
        blob_client.close()
    blob_service_client.close()
    print("data_dict:", json.dumps(data_dict))
    return json.dumps(data_dict)


IOTHUB_CONN_STR = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
#device_id should be get from db, the mapping file should be stored in db
DEVICE_ID = "mydevice"
def send_mes_to_iothub(blob_str):
    try:
        # Create IoTHubRegistryManager
        registry_manager = IoTHubRegistryManager(IOTHUB_CONN_STR)
        # need to encode charset if Chinese characters are included
        # blob_str = json.dumps(blob_str)
        blob_str.encode("utf-8")
        props = {}
        # optional: assign system properties
        props.update(messageId="message_1")
        props.update(correlationId="correlation_1")
        props.update(contentType="application/json")

        registry_manager.send_c2d_message(DEVICE_ID, blob_str, properties=props)
        print("send message to iot hub successfully.")

    except Exception as ex:
        print("Unexpected error {0}" % ex)
    except KeyboardInterrupt:
        print("IoT Hub C2D Messaging service sample stopped")


def main():
    print("Start to create client to connect the Event hub that built-in iot hub.")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONN_EVENT_HUB_STR,
        consumer_group=consumer_group)
    try:
        with client:
            client.receive_batch(
                on_event_batch=on_event_batch,
                on_error=on_error)
    except KeyboardInterrupt:
        print("Receiving has stopped by keyboard interrtpt")


if __name__ == '__main__':
    main()
    # blob_str = list_blobs()
    # send_mes_to_iothub(blob_str)