# coding: utf-8


#######################################################################################################################################
############################  The interface function of azure blob sdk  ###############################################################
#######################################################################################################################################

from azure.storage.blob import BlobServiceClient

# from iot_log import logger


class BlobService(object):
    def __init__(self, conn_str):
        super().__init__()
        self.conn_str = conn_str
        self.client = self.creat_client()

    def creat_client(self):
        try:
            return BlobServiceClient.from_connection_string(self.conn_str)
        except Exception as be:
            print("Failed to create blob cient:" + str(e))

    def create_container_client(self, container_name):
        return self.client.get_container_client(container_name)

    def save_data(self, container_name, file_name, blob_str):
        container_client = self.create_container_client(container_name)
        container_client.upload_blob(file_name, blob_str, overwrite=True)

    def delete_data(self, container_name, file_name):

        container_client = self.create_container_client(container_name)
        blob_client = container_client.get_blob_client(file_name)
        blob_client.delete_blob()

    def list_data_blobs(self, container_name, scenario_type=None):
        container_client = self.create_container_client(container_name)
        blobs = container_client.list_blobs()
        return blobs

    def download_data_blobs(self, blob_name, container_name, container_client=None):
        if not container_client:
            container_client = self.create_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blobstr = blob_client.download_blob().readall().decode("utf-8")
        return blobstr

    def list_data(self, container_name, scenario_type):
        container_client = self.create_container_client(container_name)
        blobs = container_client.list_blobs()
        data_dict = {}
        data_list = []
        for blob in blobs:

            blob_client = container_client.get_blob_client(blob.name)
            blobstr = blob_client.download_blob().readall().decode("utf-8")
            blob_json = json.loads(blobstr)
            data_list.append(blob_json)
            blob_client.close()
        data_dict[scenario_type] = json.dumps(data_list)
        return data_dict


    def close_client(self):
        if self.client:
            self.client.close()


def del_azure_blob(blob_client, id_list, container_name, logger):
    deleted_id = []
    for id in id_list:
        try:
            logger.info("Start delete the azure blob:" + str(id))
            blob_client.delete_data(container_name, str(id))
            deleted_id.append(str(id))
        except ResourceNotFoundError as ne:
            logger.warning("Not found the blob, maybe cache data in local(phone or car). Blob name:" + str(id))
            deleted_id.append(str(id))

        except Exception as e:
            print('Failed to delete azure blob:' + str(id))
            logger.error('Failed to delete azure blob:' + str(id) + ". More details:" + str(e))

    return deleted_id
