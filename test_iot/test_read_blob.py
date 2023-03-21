# coding:utf-8

from azure.storage.blob import BlobServiceClient


def test_read():
	connection_string='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blob_client = container_client.get_blob_client("test.json")
	blobstr = blob_client.download_blob().readall().decode("utf-8")
	print("Testing get blob str:")
	print(blobstr)
	print("end------")
	blob_client.close()


def test_list():
	connection_string = 'DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blobs = container_client.list_blobs()
	items = []
	for blob in blobs:
		print("Test get the blob:", blob.name)
		# print("Test get the blob dir:", dir(blob))
		print("Test get the blob dir:", blob.values)
		items.append(blob.name)


	return items



test_list()