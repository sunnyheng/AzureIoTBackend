# coding: utf-8

import json
import asyncio
from azure.eventhub import EventHubConsumerClient



BLOB_CONN_STR='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'

def save_to_blob():
	file_path = r"C:\Users\sgm_issec\Desktop\test.json"
	blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blob_client = container_client.get_blob_client("test.json")
	with open(file_path, "rb") as blob_file:
		print("Testing upload blob")
		blob_client.upload_blob(data=blob_file)
		print("finish......")
	blob_client.close()


def save_str_to_blob(event_data):
	blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blob_client = container_client.get_blob_client("test.json")
	# blob_str = event_data
	try:
		blob_client.upload_blob(data=event_data)
	except Exception as e:
		print("Failed to upload data string to blob, more details:", str(e))
	blob_client.close()


EVENTHUB_CONN_STR="Endpoint=sb://ihsuprodhkres016dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI=;EntityPath=iothub-ehub-issec-iot-24732222-7b8696b292"
consumer_group='$Default'



def get_event():
	event_data = None
	try:
		client = EventHubConsumerClient.from_connection_string(EVENTHUB_CONN_STR, consumer_group)
		# partition_id = 4
		partition_ids = client.get_partition_ids()
		# get event data
	except Exception as e:
		print("Failed to get event from iot, more details:", str(e))
	client.close()
	return event_data

def save_data():
	event_data = get_event()
	save_str_to_blob(event_data)




