# coding: utf-8

import json
import random
import sys
from azure.iot.hub import IoTHubRegistryManager


# import module (iot package)
# import blob related module
#---------------------------------------------------


# from tempfile import NamedTemporaryFile
# from azure.storage.blob.blockblobservice import BlockBlobService
#
# entry_path = conf['entry_path']
# container_name = conf['container_name']
# blob_service = BlockBlobService(
#             account_name=conf['account_name'],
#             account_key=conf['account_key'])
#
# def get_file(filename):
#     local_file = NamedTemporaryFile()
#     blob_service.get_blob_to_stream(container_name, filename, stream=local_file,
#     max_connections=2)
#
#     local_file.seek(0)
#     return local_file


from azure.storage.blob import BlobClient

# account_name: demoiotstorage
# container: iotcontainer
# blob_name: config.json
# account_access_key: 4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==

blob = BlobClient(account_url="https://<;account_name>.blob.core.windows.net",
                  container_name="<container_name>",
                  blob_name="<blob_name>",
                  credential="<account_key>")

data = blob.download_blob()

#----------------------------------------------------------------------------------
from azure.storage.blob import BlobServiceClient
connection_string='DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client("<container name>")
blob_client = container_client.get_blob_client("<blob name>")
blobstr = blob_client.download_blob().readall().decode("utf-8")
blob_client.close()

#----------------------------------------------------------------------------------

def retrieve_config_file():
    # call blob interface
    return


def send_data_to_iot():
    # save user information to db
    # save json string to azure blob
    return


class IoTService():
    CONNECTION_STRING = "HostName=iot1.azure-devices.cn;DeviceId=mydevice;SharedAccessKey=BClp1n7JHHtblo4PvTCNpMEHR3byA4NZMEgwhMzTrEY="
    # CONNECTION_STRING = "{IoTHubConnectionString}"
    DEVICE_ID = "mydevice"

    def __init__(self, user_id, vin):
        super(IoTService, self).__init__()
        self.user_id = user_id
        self.vin = vin
        self.json_string = "{}"


    # this function was executed when get the message(sign) from iot hub
    def get_target_config(self):
        json_str = ""
        # search json file by user_id+vin in blob storage
        # get the config file from blob db, so need to install blob package
        return json_str


    def set_target_config(self, json_str):
        self.json_string = json_str

    def iothub_messaging_sample_run(self):
        try:
            # Create IoTHubRegistryManager
            registry_manager = IoTHubRegistryManager(self.CONNECTION_STRING)

            json_str = self.get_target_config()
            self.set_target_config(json_str)

            props = {}
            # optional: assign system properties
            props.update(messageId="message_%d" % i)
            props.update(correlationId="correlation_%d" % i)
            props.update(contentType="application/json")

            # # optional: assign application properties
            # prop_text = "PropMsg_%d" % i
            # props.update(testProperty=prop_text)

            registry_manager.send_c2d_message(self.DEVICE_ID, self.json_string, properties=props)

            input("Press Enter to continue...\n")

        except Exception as ex:
            print("Unexpected error {0}" % ex)
            return
        except KeyboardInterrupt:
            print("IoT Hub C2D Messaging service sample stopped")