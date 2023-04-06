# coding: utf-8
import json
#######################################################################################################################################
############################  The functionality logic, depends on received event from iot #############################################
#######################################################################################################################################


import time, copy

from azure.core.exceptions import ResourceNotFoundError

from models.data_model import DataModel

from utils.iot_hub_conn_info import IoTHubConnInfo
from utils.blob_operation import BlobService, del_azure_blob

from utils.iot_hub_cloud_service import IoTHubManager
from utils.iot_hub_cloud_service import check_and_send_c2d_message

from connction_string import BLOB_CONN_STR, SCENARIO_USER, SCENARIO_SQUARE
from utils.iot_log import logger

from utils.mongo_utils import get_mongo_client
from utils.mongo_utils import update_delete_id

from models.data_model import convert_data_model



def action_by_received(event):
    # the iothub connection data, user, device id are saved in mongodb
    # currently, one db:scenario_db; four collections: user/resend_message/square_buy/square_count
    # square_buy:{ "user_id" : "test123", "square_name" : "1000", "is_bought" : true }
    # square_count:{"square_name" : "1000", "count" : 0 }
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

    # update add logic, will send the square data from cloud, when car/phone add the customized scenario
    # event_json is {"ScenarioUser":{},"ScenarioSquare":{"addID":""}}
    # 一键添加广场数据
    if action == "OTHER":
        other_data(conn_obj, iothub_client, blob_client, event_json)

    if action == "ADD":
        add_data(conn_obj, iothub_client, blob_client, event_json)
    if action == "SYNC":
        # todo 哪端sync，给哪端发消息, 如果是刚联网，需要把上次delete 的id 也发送下去
        fetch_data(conn_obj, iothub_client, blob_client, deleted_mes_client, event_json)

    if action == "DELETE":
        # 需要把云端数据删除，并把结果下发给两端
        delete_data(conn_obj, iothub_client, blob_client, deleted_mes_client, event_json)

    deleted_mes_client.close()


# this is update the existing data
def add_data(conn_obj, iothub_client, blob_client, event_json):
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
            check_and_send_c2d_message(iothub_client, device_id, data_str, "ADD", logger)
    except Exception as add_e:
        logger.error("[ADD]Failed to handle add scenario, more details:" + str(add_e))


def other_data(conn_obj, iothub_client, blob_client, event_json):
    square_data = square_id = ""
    try:
        square = event_json.get("ScenarioSquare")
        if square and square.get("addID"):
            square_id = str(square["addID"])
            container_name = SCENARIO_SQUARE
            square_data = blob_client.download_data_blobs(square_id, container_name)
        else:
            logger.warning("[OTHER]No required addID attribute from D2C message.")
    except Exception as e:
        logger.warning("[OTHER]Failed to get square data from cloud, square id:"+square_id+". More details:" +str(e))

    if square_data:
        message_str = set_add_message(conn_obj, square_id, square_data)
        target_device_list = conn_obj.phone_list
        target_device_list.extend(conn_obj.car_list)

        logger.info("[OTHER]Get the c2d message device list:" + str(target_device_list))
        for device_id in target_device_list:
            try:
                check_and_send_c2d_message(iothub_client, device_id, message_str, "OTHER", logger)

            except Exception as ae:
                logger.error("[OTHER]Failed get device infos, more details:" + str(ae))


def set_add_message(conn_obj, square_id, blob_str):
    user_json = json.loads(blob_str)
    square_json = copy.deepcopy(user_json)

    mes_dict = {}
    data_list = []
    data_list.append(user_json)
    user_model = DataModel()
    user_model.set_content(data_list)
    mes_dict["ScenarioUser"] = user_model.__dict__

    mongo_client = get_mongo_client("scenario_db", "square_buy")

    # buy_doc = {"user_id":conn_obj.user_id, "square_name":square_id}
    # result = mongo_client.select_data(buy_doc)
    # if not result:
    #     buy_doc["is_bought"]=True
    #     mongo_client.insert_data(buy_doc)

    is_bought = resolve_is_bought(mongo_client, square_id)
    square_json["userBought"] = is_bought

    # mongo_client.set_db("square_count")
    # square_id_doc = {"square_name": square_id}
    # update_value = {"$inc": {"count": 1}}
    # count = 1
    # try:
    #     result = mongo_client.select_data(square_id_doc)
    #     if result:
    #         count = count + result.get("count")
    #         mongo_client.update_data(square_id_doc, update_value)
    #     else:
    #         mongo_client.insert_data({"square_name": square_id, "count": 0})
    # except Exception as e:
    #     # mongo_client.insert_data({"square_name": square_id, "count": 0})
    #     logger.warning("Failed to update the value of buy count.")

    count = resolve_buy_count(mongo_client, square_id)

    square_json["buyCount"] = count
    square_list = []
    square_list.append(square_json)

    square_model = DataModel()
    square_model.set_content(square_list)
    mes_dict["ScenarioSquare"] = square_model.__dict__
    mongo_client.close()
    return json.dumps(mes_dict)


def resolve_buy_count(mongo_client, square_id):
    if mongo_client:
        mongo_client.set_db("square_buy")
    else:
        mongo_client = get_mongo_client("scenario_db", "square_count")
    square_id_doc = {"square_name": square_id}
    update_value = {"$inc": {"count": 1}}
    count = 1
    try:
        result = mongo_client.select_data(square_id_doc)
        if result:
            count = count + result.get("count")
            mongo_client.update_data(square_id_doc, update_value)
        else:
            mongo_client.insert_data({"square_name": square_id, "count": 0})
    except Exception as e:
        # mongo_client.insert_data({"square_name": square_id, "count": 0})
        logger.warning("Failed to update the value of buy count.")
    return count

#
def resolve_is_bought(mongo_client, square_id, conn_obj):
    if mongo_client:
        mongo_client.set_db("square_buy")
    else:
        mongo_client = get_mongo_client("scenario_db", "square_buy")
    buy_doc = {"user_id": conn_obj.user_id, "square_name": square_id}
    result = mongo_client.select_data(buy_doc)
    if not result:
        buy_doc["is_bought"] = True
        mongo_client.insert_data(buy_doc)
    return True



def check_device_status(resp, device_id, iothub_client, force_clean=False):
    if (resp.connection_state == "Disconnected" or force_clean) and resp.cloud_to_device_message_count >= 20:
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

        device_id = conn_obj.device_id
        send_status = check_and_send_c2d_message(iothub_client, device_id, blob_str, "SYNC", logger)

        # todo check if the device is connected
        if send_status:
            mongo_client.delete_data({"user_id": conn_obj.user_id, "device_id": device_id})
        # confirm if it need force_client
        # check_device_status(resp, device_id, iothub_client, force_clean=True)

    except Exception as syc_e:
        logger.error("[SYNC]Failed to handle sync scenario, more details:" + str(syc_e))


def get_whole_message(conn_obj, blob_client, mongo_client):
    user = covert_message(blob_client, mongo_client, SCENARIO_USER, "ScenarioUser", conn_obj)
    tpm = covert_message(blob_client, mongo_client, SCENARIO_SQUARE, "ScenarioSquare", conn_obj)
    user.update(tpm)
    logger.info("[SYNC]The whole message:" + json.dumps(user))
    return user


def covert_message(blob_client, mongo_client, container_name, scenario_type, conn_obj):
    # suppose only user customized data can be deleted
    logger.info("Start to get blob data.")
    data = {}
    try:
        deleted_list = []
        if scenario_type=="ScenarioUser":
            result = mongo_client.select_data({"user_id": conn_obj.user_id, "device_id": conn_obj.device_id})
            if result and result.get("deleted_id"):
                deleted_list = result["deleted_id"]

        blob_list = get_target_blobs(blob_client, container_name, scenario_type, conn_obj)
        data = convert_data_model(blob_list, deleted_list, scenario_type)

    except Exception as ce:
        logger.warning("failed, more details:" + str(ce))
    return data


def get_target_blobs(blob_client, container_name, scenario_type, conn_obj):
    data_list = []
    try:
        blobs = blob_client.list_data_blobs(container_name, scenario_type)
        for blob in blobs:
            logger.info("Get the blob name:" + str(blob.name))
            blob_str = blob_client.download_data_blobs(blob.name, container_name)
            resolve_is_bought()
            blob_json = json.loads(blob_str)
            data_list.append(blob_json)
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
    deleted_id_list = del_azure_blob(blob_client, scenario_id_list, container_name, logger)

    if deleted_id_list:

        mes_dict = convert_data_model([], deleted_id_list, "ScenarioUser")
        message_str = json.dumps(mes_dict)

        # todo currently, only single phone and single car
        # if there are multiple cars, should check all of the device status,
        # if it is disconnected, it will be record in mongodb, when it sync data, it will receive the deleted data and sync data
        target_device_list = conn_obj.phone_list
        target_device_list.extend(conn_obj.car_list)
        logger.info("[Delete]Get the c2d message device list:" + str(target_device_list))

        for device_id in target_device_list:
            try:
                status = check_and_send_c2d_message(iothub_client, device_id, message_str, "DELETE", logger)
                if not status:
                    # If the device does not online, store the delete information to mongo db
                    update_delete_id(mongo_client, conn_obj.user_id, device_id, deleted_id_list)

            except Exception as de:
                logger.error("[Delete]Failed get device infos, more details:" + str(de))


def generate_props(operation_type):
    cur_timestamp = int(time.time())
    stamp = (cur_timestamp + 500) * 1000
    props = {}
    props.update(expiryTimeUtc=stamp)
    props.update(contentType="application/json")
    props.update(operation_type=operation_type)
    props.update(currentTime=cur_timestamp)
    return props


def convert_to_str(byt):
    if byt:
        return byt.decode("utf-8")
    else:
        return ""


# get user, device information
def get_event_prop(event):
    coll_client = get_mongo_client("scenario_db", "user")

    event_props = event.properties
    logger.debug("Testing get the event props")
    logger.debug(event_props)

    device_type = convert_to_str(event_props.get(b"DEVICE_TYPE"))
    operation_type = convert_to_str(event_props.get(b"OPERATION_TYPE"))
    device_id_a = convert_to_str(event_props.get(b"$.cdid"))
    device_id_b = convert_to_str(event_props.get(b"iothub-connection-device-id"))
    device_id = device_id_a or device_id_b


    user_id = event.correlation_id

    logger.info("Get user id:" + user_id)
    logger.info( device_type)
    logger.info(operation_type)

    # need to use mongo client to get user information
    iothub_conn_info_obj = IoTHubConnInfo(user_id, device_type, device_id, operation_type)

    iothub_conn_info_obj.get_result_by_user_id(condition={"user_id":user_id})
    iothub_conn_info_obj.parse_record()
    return iothub_conn_info_obj




