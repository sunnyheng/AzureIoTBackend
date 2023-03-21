# coding: utf-8

import json
import random
import sys
from azure.storage.blob import BlobServiceClient
from azure.iot.hub import IoTHubRegistryManager


def read_blob():
	connection_string='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_client = blob_service_client.get_container_client("iotcontainer")
	blob_client = container_client.get_blob_client("test.json")
	blobstr = blob_client.download_blob().readall().decode("utf-8")
	print("Testing get blob str:")
	print(blobstr)
	blob_client.close()
	return blobstr


CONNECTION_STRING_old = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
#CONNECTION_STRING = "{IoTHubConnectionString}"
DEVICE_ID = "mydevice"

class DataModel(object):
    def __init__(self, val):
        super().__init__()
        self.value = val


def parse_dict(data):
    print('dict:', data.__dict__)

# def iothub_messaging_sample_run():
def iothub_send_message():
    try:
        # Create IoTHubRegistryManager
        registry_manager = IoTHubRegistryManager(CONNECTION_STRING_old)
        #===============Testing==========================================
        # json_str = json.dumps(read_blob())
        # data = DataModel("test_value_123")
        # json_data = {}
        # json_data["test_key"] = data.__dict__
        # try:
        #     json_str = json.dumps(json_data)
        # except Exception as e:
        #     print("failed to convert to str:", str(e))
        json_str = {"test_key":"test_value"}
        # ===============Testing==========================================
        props = {}
        # optional: assign system properties
        props.update(messageId="message_1")
        props.update(correlationId="correlation_1")
        props.update(contentType="application/json")
        # props.update(expiryTimeUtc=1)
        # props.update(iothubAck = "full")
        props["iothub-ack"] = "full"

        # # optional: assign application properties
        # prop_text = "PropMsg_%d" % i
        # props.update(testProperty=prop_text)
# 可以先用ack来获取消息 消费状态，如果没有消费，则用get_device 方法去取连接状态。

        # reps = registry_manager.get_device('car')
        # print(reps)
        registry_manager.send_c2d_message(DEVICE_ID, json.dumps(json_str), properties=props)
        # reps = registry_manager.get_device('car')
        # print(reps)
        print("send message to iot hub successfully.")
        return 1

    except Exception as ex:
        print("Unexpected error {0}" % ex)
    except KeyboardInterrupt:
        print("IoT Hub C2D Messaging service sample stopped")
    return 0


iothub_send_message()
# data = DataModel("test_dunn")
# parse_dict(data)