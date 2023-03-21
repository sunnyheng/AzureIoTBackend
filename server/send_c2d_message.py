# coding: utf-8
from azure.iot.hub import IoTHubRegistryManager

# from init_logger import logger

def send_mes_to_multi_iothub(blob_str, iothub_conn_list, device_id_list, logger):
    result_status = False
    if isinstance(iothub_conn_list, list) and isinstance(device_id_list, list):
        result_status = True
        for ind, conn_str in enumerate(iothub_conn_list):
            status = send_mes_to_iothub(blob_str, conn_str, device_id_list[ind], logger)
            result_status = result_status and status
    # It is better to cover else case
    return result_status


#device_id should be get from db, the mapping file should be stored in db
def send_mes_to_iothub(blob_str, iothub_conn_str, device_id, logger):
    try:
        # logger.info("[iot] start create iothub client.")
        # Create IoTHubRegistryManager
        registry_manager = IoTHubRegistryManager(iothub_conn_str)
        # need to encode charset if Chinese characters are included
        # blob_str = json.dumps(blob_str)
        # print("Start to covernt to utf-8.")
        blob_str.encode("utf-8")
        # logger.info('test:'+ blob_str)
        props = {}
        # optional: assign system properties
        # props.update(messageId="message_1")
        props.update(correlationId=device_id)
        props.update(expiryTimeUtc=10)
        props.update(contentType="application/json")

        try:
            # logger.info("[iot] Start send c2d message.")
            print("Start send c2d message to: " + device_id)
            registry_manager.send_c2d_message(device_id, blob_str, properties=props)
            # logger.info("[iot] send message(C2D) successfully to device:" +device_id)
            print("send message(C2D) to device:%s successfully." % device_id)
            tag = True
        except Exception as e:
            print("Failed to send c2d message, more details:"+ str(e))
            # logger.error("[iot] Failed to send c2d message, more details:"+ str(e))
            # logger.error("[iot] Failure message:" + blob_str)
            tag = False
        # delete the registry manager client after finish sending message
        finally:
            if registry_manager:
                del registry_manager
        return tag


    except Exception as ex:
        print("Unexpected error {0}" % ex)
        # logger.error("[iot] Unexpected error: %s" % ex)
        # logger.error("[iot] Unexpected json string:" + blob_str)
        return False
