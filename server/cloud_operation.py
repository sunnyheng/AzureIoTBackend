# coding: utf-8
import json
#######################################################################################################################################
############################  The functionality logic, depends on received event from iot #############################################
#######################################################################################################################################


import time

from azure.core.exceptions import ResourceNotFoundError

from server.models.data_model import DataModel

from server.utils.iot_hub_conn_info import IoTHubConnInfo
from server.utils.blob_operation import BlobService
from server.utils.iot_hub_cloud_service import IoTHubManager
from connction_string import BLOB_CONN_STR, SCENARIO_USER, SCENARIO_SQUARE
from server.utils.iot_log import logger
from server.utils.mongo_utils import MongoDBClient



def action_by_received(event):
    # the iothub connection data, user, device id are saved in mongodb
    logger.info("Start create mongodb client, used for select/store deleted id.")
    deleted_mes_client = get_mongo_client("scenario_db", "resend_message")

    # user_id, device_type, operation_type, conn_string ,
    logger.info("Start create connection object, select mongodb by user_id.")
    conn_obj = get_event_prop(event)

    logger.info("Start create iothub client.")
    iothub_client = IoTHubManager(conn_obj.conn_str)

    logger.info("Start create azure blob client.")
    blob_service_obj = BlobService(BLOB_CONN_STR)
    blob_client = blob_service_obj
    # device_type
    event_data = event.body_as_json()

    event_json = event_data

    if isinstance(event_data, str):
        event_data.encode("utf-8")
        event_json = json.loads(event_data)
        logger.info("[iot] Uploaded message is string, already finished parsing to json")
    # action = event_json.get("operation", None)
    action = conn_obj.operation_type
    # action = "SYNC"
    logger.info("The operation type is:" + action)
    if action == "ADD":
        update_data(conn_obj, iothub_client, blob_client, event_json)
    if action == "SYNC":
        # todo 哪端sync，给哪端发消息, 如果是刚联网，需要把上次delete 的id 也发送下去
        fetch_data(conn_obj, iothub_client, blob_client, deleted_mes_client, event_json)

    if action == "DELETE":
        # 需要把云端数据删除，并把结果下发给两端
        delete_data(conn_obj, iothub_client, blob_client, deleted_mes_client, event_json)


def get_mongo_client(db_name, coll_name):
    mongo_db = MongoDBClient("127.0.0.1", 27017)
    mongo_db.set_db(db_name)
    mongo_db.set_collection(coll_name)
    return mongo_db


# this is add new data or update the existing data
def update_data(conn_obj, iothub_client, blob_client, event_json):
    # option 1. new json upload
    try:
        logger.info("[ADD]Start save data and send back to iot hub")
        save_str_to_blobs(blob_client, event_json)
    except Exception as be:
        logger.error("[ADD]Failed to save data to azure blob, more details:" + str(be))

    data_str = json.dumps(event_json)
    target_device_list = []
    if conn_obj.device_type == "CAR":
        target_device_list = conn_obj.phone_list
    if conn_obj.device_type == "PHONE":
        target_device_list = conn_obj.car_list
    try:
        for device_id in target_device_list:
            resp = iothub_client.get_device_info(device_id)
            check_device_status(resp, device_id, iothub_client)
            if resp.connection_state == "Connected":
                props = generate_props("ADD")
                try:
                    logger.info("[ADD]Send data:" + data_str)
                    iothub_client.iot_send_message(device_id, data_str, props)
                    logger.info("[ADD]Send c2d message to " + device_id+ " successfully")
                except Exception as se:
                    logger.error("[ADD]Failed to send c2d message(ADD) to " + device_id + ". More details:" + str(se))
                    print("[ADD]Failed to send c2d message(ADD) to " + device_id)
    except Exception as add_e:
        logger.error("[ADD]Failed to handle add scenario, more details:" + str(add_e))


def check_device_status(resp, device_id, iothub_client, force_clean=False):
    if resp.connection_state == "Disconnected" and resp.cloud_to_device_message_count >= 20:
        clean_message_queue(device_id, iothub_client)
    if force_clean and resp.cloud_to_device_message_count >=20:
        clean_message_queue(device_id, iothub_client)

def clean_message_queue(device_id, iothub_client):
    try:
        count = iothub_client.purge_queue_message(device_id)
        print("The device: " + device_id + "is disconnected, and purge the queue message:" + str(count))
        logger.info("The device: " + device_id + "is disconnected, and purge the queue message:" + str(count))
    except Exception as pe:
        logger.warning("Failed to check the d2c message:" + device_id + ". More details:" + str(pe))

def save_str_to_blobs(client, event_json):
    logger.info("[ADD]Start to save blob to azure")
    for key, value in event_json.items():
        # todo currently, no need to handle ScenarioSquare
        if key == "ScenarioUser":
            container_name = SCENARIO_USER
            parse_blob(client, container_name, value)
        if key == "ScenarioSquare":
            # todo currently, no action for square data
            pass


def parse_blob(client, container_name, content_val):
    if content_val:
        blob_list = content_val.get("content")
        if not isinstance(blob_list, list):
            return
        for blob in blob_list:
            file_name = str(blob.get('id'))
            logger.info("file_name:" + file_name)
            client.save_data(container_name, file_name, json.dumps(blob))
            logger.info("[ADD] Finished to upload file:" + file_name)
            print("[ADD]Finished to upload file:" + file_name)


# the car/phone side SYNC data from cloud
def fetch_data(conn_obj, iothub_client, blob_client, mongo_client, event_json):
    # read date from blob and sent to iot
    # logger.debug("[Start]Testing get blobs the datetime:"+ str(datetime.datetime.now()))
    message_dict = get_whole_message(conn_obj, blob_client, mongo_client)
    # logger.debug("[End]Testing get blobs the datetime:" + str(datetime.datetime.now()))
    # todo check logic, the side which request SYNC will repected a response
    # logger.info("[SYNC]Get the c2d message device list:" + str(target_device_list))
    blob_str = json.dumps(message_dict)
    try:
        # logger.debug("[Start]Testing get device status the datetime:" + str(datetime.datetime.now()))
        device_id = conn_obj.device_id
        resp = iothub_client.get_device_info(device_id)
        logger.info("get device status:" + resp.connection_state)
        check_device_status(resp, device_id, iothub_client, force_clean=True)
        # logger.debug("[End]Testing get device status the datetime:" + str(datetime.datetime.now()))
        # logger.debug("[Start]Testing send message the datetime:" + str(datetime.datetime.now()))
        if resp.connection_state == "Connected":
            props = generate_props("SYNC")
            try:
                logger.info("[SYNC]Send c2d message:" + blob_str)
                iothub_client.iot_send_message(device_id, blob_str, props)
                logger.info("[SYNC]Send c2d message to " + device_id + " successfully")
                mongo_client.delete_data({"user_id": conn_obj.user_id, "device_id": device_id})
            except Exception as se:
                logger.error("[SYNC]Failed to send c2d message(SYNC) to " + device_id + ". More details:" + str(se))
                print("[SYNC]Failed to send c2d message(SYNC) to " + device_id)
            # logger.debug("[End]Testing send message the datetime:" + str(datetime.datetime.now()))
    except Exception as syc_e:
        logger.error("[SYNC]Failed to handle sync scenario, more details:" + str(syc_e))


def get_whole_message(conn_obj, blob_client, mongo_client):
    user = covert_message(blob_client, mongo_client, SCENARIO_USER, "ScenarioUser", conn_obj=conn_obj)
    tpm = covert_message(blob_client, mongo_client, SCENARIO_SQUARE, "ScenarioSquare")
    user.update(tpm)
    logger.info("[SYNC]The whole message:" + json.dumps(user))
    return user

def covert_message(blob_client, mongo_client, container_name, scenario_type, conn_obj=None):
    logger.info("Start to get blob data.")
    data = {}
    try:
        blob_list = get_target_blobs(blob_client, container_name, scenario_type)
        data_model = DataModel()
        data_model.set_content(blob_list)
        if conn_obj:
            result = mongo_client.select_data({"user_id": conn_obj.user_id, "device_id": conn_obj.device_id})
            if result and result.get("deleted_id"):
                data_model.set_deleted_id(result["deleted_id"])
        data[scenario_type] = data_model.__dict__
    except Exception as ce:
        logger.warning("failed, more details:" + str(ce))
    return data


def get_target_blobs(blob_client, container_name, scenario_type):
    data_list = []
    try:
        blobs = blob_client.list_data_blobs(container_name, scenario_type)
        for blob in blobs:
            logger.info("Get the blob name:" + str(blob.name))
            blob_str = blob_client.download_data_blobs(blob.name, container_name)
            data_list.append(json.loads(blob_str))
    except Exception as e:
        logger.warning("Failed to list blobs:" + container_name)
        logger.warning("More details:" + str(e))
    return data_list

def delete_data(conn_obj, iothub_client, blob_client, mongo_client, event_json):
    # mark the data in target db/blob

    delete_resp = {}
    user_data = event_json.get("ScenarioUser")
    scenario_id_list = user_data.get('deletedID')
    container_name = SCENARIO_USER
    deleted_id_list = del_azure_blob(blob_client, scenario_id_list, container_name)

    if deleted_id_list:
        mes_dict = set_delete_message(deleted_id_list)
        message_str = json.dumps(mes_dict)

        # todo currently, only single phone and single car
        # if there are multiple cars, should check all of the device status,
        # if it is disconnected, it will be record in mongodb, when it sync data, it will receive the deleted data and sync data
        target_device_list = conn_obj.phone_list
        target_device_list.extend(conn_obj.car_list)
        logger.info("[Delete]Get the c2d message device list:" + str(target_device_list))

        for device_id in target_device_list:
            try:
                resp = iothub_client.get_device_info(device_id)
                # If the device does not online, store the delete information to mongo db
                if resp.connection_state == "Disconnected":
                    update_delete_id(mongo_client, conn_obj.user_id, device_id, deleted_id_list)
                check_device_status(resp, device_id, iothub_client)
                if resp.connection_state == "Connected":
                    props = generate_props("DELETE")
                    try:
                        logger.info("[DELETE]Send c2d message:" + message_str)
                        iothub_client.iot_send_message(device_id, message_str, props)
                        logger.info("[DELETE]Send c2d message to " + device_id + " successfully")
                    except Exception as se:
                        logger.error(
                            "[Delete]Failed to send c2d message(del) to " + device_id + ". More details:" + str(se))
                        print("[Delete]Failed to send c2d message(del) to " + device_id)
            except Exception as de:
                logger.error("[Delete]Failed get device infos, more details:" + str(de))


def set_delete_message(id_list):
    mes_dict = {}
    data_model = DataModel()
    data_model.set_deleted_id(id_list)
    mes_dict["ScenarioUser"] = data_model.__dict__
    return mes_dict

def del_azure_blob(blob_client, id_list, container_name):
    deleted_id = []
    for id in id_list:
        try:
            logger.info("[DELETE]Start delete the azure blob:" + str(id))
            blob_client.delete_data(container_name, str(id))
            deleted_id.append(str(id))
        except ResourceNotFoundError as ne:
            logger.warning("[DELETE]Not found the blob, maybe cache data in local(phone or car). Blob name:" + str(id))
            deleted_id.append(str(id))

        except Exception as e:
            print('[DELETE]Failed to delete azure blob:' + str(id))
            logger.error('[DELETE]Failed to delete azure blob:' + str(id) + ". More details:" + str(e))

    return deleted_id


def generate_props(operation_type):
    stamp = (int(time.time()) + 500) * 1000
    props = {}
    props.update(expiryTimeUtc=stamp)
    props.update(contentType="application/json")
    props.update(operation_type=operation_type)
    # todo
    # props.update(currentTime)
    return props


def convert_to_str(byt):
    if byt:
        return byt.decode("utf-8")
    else:
        return ""


# get user, device information
def get_event_prop(event):
    coll_client = get_mongo_client("scenario_db", "user")
    # sys_props = event.system_properties
    event_props = event.properties
    # device_type = convert_to_str(event_props.get(b"DEVICE_TYPE")) or "PHONE"
    # operation_type = convert_to_str(event_props.get(b"OPERATION_TYPE")) or "SYNC"
    # device_id = convert_to_str(event_props.get(b"$.cdid")) or "phone"
    device_type = convert_to_str(event_props.get(b"DEVICE_TYPE"))
    operation_type = convert_to_str(event_props.get(b"OPERATION_TYPE"))
    device_id = convert_to_str(event_props.get(b"$.cdid"))

    # for item in event.properties.items():
    #     logger.info("show event proper item:")
    #     logger.info(item)
    #
    #     if convert_to_str(item[0]) == "DEVICE_TYPE":
    #         device_type = convert_to_str(item[1])
    #     if convert_to_str(item[0]) == "OPERATION_TYPE":
    #         operation_type = convert_to_str(item[1])
    #     if convert_to_str(item[0]) == "$.cdid":
    #         device_id = convert_to_str(item[1])

    user_id = event.correlation_id

    logger.info("Get user id:" + user_id)
    logger.info( device_type)
    logger.info(operation_type)

    # need to use mongo client to get user information
    iothub_conn_info_obj = IoTHubConnInfo(user_id, device_type, device_id, operation_type)

    iothub_conn_info_obj.get_result_by_user_id(condition={"user_id":user_id})
    iothub_conn_info_obj.parse_record()
    return iothub_conn_info_obj


def update_delete_id(mongo_client, user_id, device_id, deleted_id_list):
    condition = {"user_id": user_id, "device_id": device_id}
    doc = {"user_id": user_id, "device_id": device_id, "deleted_id": deleted_id_list}
    updated_doc = {'$addToSet':{"deleted_id":{"$each": deleted_id_list}}}
    if mongo_client.select_data(condition):
        mongo_client.update_data(condition, updated_doc)
    else:
        mongo_client.insert_data(doc)


