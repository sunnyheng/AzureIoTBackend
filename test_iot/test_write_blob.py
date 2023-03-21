# coding: utf-8

from azure.storage.blob import BlobServiceClient


connection_string='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'


def test_save():
	file_path = r"C:\Users\sgm_issec\Desktop\test.json"
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blob_client = container_client.get_blob_client("test.json")
	with open(file_path, "rb") as blob_file:
		print("Testing upload blob")
		blob_client.upload_blob(data=blob_file)
		print("finish......")
		
	blob_client.close()


def test_override_blob():
	print('==========================')
	json_str = "{'sunny_abc':'sunny_value123'}"
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_client = blob_service_client.get_container_client("iotcontainer")
	container_client.upload_blob("test.json", json_str, overwrite=True)
	# blob_client = container_client.get_blob_client("test.json")
	container_client.close()
	print('==========================')

	
test_override_blob()