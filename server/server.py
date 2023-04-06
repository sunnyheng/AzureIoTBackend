# coding: utf-8
import os, time
import re

from flask import Flask, render_template, request, Response, make_response
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from flask_cors import CORS

from utils.blob_operation import BlobService, del_azure_blob

import json
from flask import current_app

from utils.iot_log import setup_handler

from connction_string import BLOB_CONN_STR
from connction_string import SCENARIO_SQUARE, SCENARIO_USER

from utils.iot_hub_conn_info import get_info
from models.data_model import convert_data_model

from utils.iot_hub_cloud_service import IoTHubManager, check_and_send_c2d_message
from utils.mongo_utils import get_mongo_client, update_delete_id


app = Flask(__name__)
app.logger.addHandler(setup_handler())
ctx = app.app_context()
ctx.push()
CORS(app)


@app.route("/")
def show_data():
    return render_template("index.html")


@app.route("/showDetails", methods=["GET"])
def show_blob_file():
    file_content = request.args.get("content")
    json_content = json.loads(file_content)
    # add to list
    content_list = []
    content_list.append(json_content)
    return render_template("details.html", data=content_list)


@app.route("/readFile", methods=["GET"])
def get_blob_file():
    # todo should add a category to distinguish scenario or template
    # todo, to know the cotainer name(iotcontainer/tmplscenario)
    print('read file...')
    file_name = request.args.get("name")
    type = request.args.get("type")
    container_name = SCENARIO_USER
    if type == "ScenarioSquare":
        container_name = SCENARIO_SQUARE
    current_app.logger.info("Start read blob file." + container_name)
    blob_list = []
    print("blob file_name")
    print(file_name)
    if file_name and file_name != "undefined":
        blob_service = BlobService(BLOB_CONN_STR)
        blob_client = blob_service.client
        blob_json = read_blob(file_name, container_name, blob_client)
        blob_service.close_client()
        if blob_json:
            blob_list.append(blob_json)
            return blob_list
        else: return Response('Target blob cannot found', status=404)

    else:
        blob_items = list_blob(container_name)

        current_app.logger.info("get blob item:" + str(blob_items))
        blob_list.extend(blob_items)
        return blob_list


@app.route("/readFile1", methods=["GET"])
def get_mongo_test_data():
    file_name = request.args.get("name")
    type = request.args.get("type")
    mongo_client = get_mongo_client("test", "test_collection")
    condition = {"file_name": str(file_name)}
    record = mongo_client.select_data(condition)
    data_str = record.get("data")
    data_json = json.loads(data_str)
    data_content = []
    data_content.append(data_json)
    if data_content:
        return data_content
    else:
        return Response('Target data cannot found', status=404)


@app.route("/publish_square", methods=["POST"])
def publish_square():
    current_app.logger.info("[server.py] Front end call publish function.")
    content = request.json
    if content:
        content_list = []
        content_list.append(content)
        conn_obj = get_info('12345', 'cloud', 'cloud', 'ADD')

        message = convert_data_model(content_list, [], "ScenarioSquare")
        target_list = []
        target_list.extend(conn_obj.car_list)
        target_list.extend(conn_obj.phone_list)
        iot_hub_client = IoTHubManager(conn_obj.conn_str)
        publish_message(iot_hub_client, message, target_list)
    else:
        print("The content is empty.")


@app.route("/publish_user", methods=["POST"])
def publish_user():
    # current_app.logger.info("[server.py] Front end call publish function.")
    content = request.json
    if content:
        content_list = []
        content_list.append(content)
        blob_client = BlobService(BLOB_CONN_STR)
        file_name = content.get("id")
        conn_obj = get_info('12345', 'cloud', 'cloud', 'ADD')

        message = convert_data_model(content_list, [], "ScenarioUser")

        target_list = []
        target_list.extend(conn_obj.car_list)
        target_list.extend(conn_obj.phone_list)
        blob_client.save_data(SCENARIO_USER, file_name, json.dumps(content))
        iot_hub_client = IoTHubManager(conn_obj.conn_str)
        if publish_message(iot_hub_client, message, target_list):
            return Response("Publish successfully!", 200)
        else:
            return Response("Cannot publish, device disconnected!", 500)
    else:
        print("The content is empty.")


def publish_message(iothub_client, message, device_id_list):
    blob_str = json.dumps(message)
    print("test:", device_id_list)
    tag = True
    for device_id in device_id_list:
        try:
            logger = current_app.logger
            status = check_and_send_c2d_message(iothub_client, device_id, blob_str, "PUBLISH", logger)

        except Exception as syc_e:
            current_app.logger.error("[SYNC]Failed to handle sync scenario, more details:" + str(syc_e))
            print("[PUBLISH]Failed to handle sync scenario, more details:" + str(syc_e))
            status = False
        tag = tag and status
    return tag


@app.route("/delete", methods=["DELETE"])
def del_record():
    conn_obj = get_info('12345', 'cloud', 'cloud', 'DELETE')
    iothub_client = IoTHubManager(conn_obj.conn_str)
    deleted_client = get_mongo_client("scenario_db", "resend_message")
    id = request.args.get('id')
    type = request.args.get('type')

    id_list = []
    id_list.append(id)
    container_name = SCENARIO_SQUARE
    folder_name = "ScenarioUser"
    if type == 'ScenarioUser':
        print('user data.')
        container_name = SCENARIO_USER
        folder_name = "ScenarioSquare"

    logger = current_app.logger
    blob_client = BlobService(BLOB_CONN_STR)
    del_id = del_azure_blob(blob_client, id_list, container_name, logger)

    if del_id:
        target_device_list = []
        target_device_list.extend(conn_obj.phone_list)
        target_device_list.extend(conn_obj.car_list)

        tag = True
        # todo should add content list
        del_mes = convert_data_model([], del_id, folder_name)
        message_str = json.dumps(del_mes)

        for device_id in target_device_list:
            status = check_and_send_c2d_message(iothub_client, device_id, message_str, "DELETE", logger)
            tag = tag and status
            if not status:
                update_delete_id(deleted_client, conn_obj.user_id, device_id, del_id)

        if tag:
            return Response("Success", 200)
        else:
            return Response("Failed", 500)


# upload function in cloud web, only upload the scenario template
@app.route("/upload", methods=["GET", "POST"])
def upload_data():

    try:
        if request.method == "POST":
            # if it is form data
            data = request.json
            # file = request.files["file"]
            data_format = format_upload_data(data)
            print('data:'+ str(data_format))
            return upload_content(data_format)

    except Exception as ue:
        print("Error, check json format:" + str(ue))
        # current_app.logger.error("Failed to upload data to azure blob:" + str(e))
        resp = Response('Cannot upload to azure blob', status=400)
        # reps = {'status': '500', 'reason': ' cannot upload to azure blob.'}
        return resp


def upload_content(data):
    if isinstance(data, dict):
        json_data = data
    else:
        json_data = json.loads(data)
    if validate_content(json_data):
        try:
            blob_service = BlobService(BLOB_CONN_STR)
            blob_client = blob_service.client
            file_name = str(json_data["id"])

            if isinstance(data, dict):
                data = json.dumps(data)
            blob_client.save_data(SCENARIO_SQUARE, file_name, data)
            content = []
            content.append(json_data)
            reps = {'status': '200', 'file_name': json_data['id'], 'content': json.dumps(content)}
            return reps
        except Exception as e:
            res = Response('Cannot upload to azure blob', status=500)
            print("Failed to upload the scenario data:" + str(e))
            return res
    else:
        res = Response('Data Not valid', status=400)
        # reps = {'status': '500', 'reason': ' data is not valid.'}
        return res


def format_upload_data(data):
    if isinstance(data, str):
        data_json = json.loads(data)
    else:
        data_json = data
    print("Start to format")
    t = time.localtime()
    time_f = time.strftime("%Y-%m-%d %H:%M:%S GMT+08:00", t)
    data_json["author"] = "PATAC"
    data_json["version"] = 1
    data_json["storeId"] = 0
    data_json["relationScenarios"] = []
    data_json["userId"] = "1234567890"
    data_json["tagType"] = 1
    data_json["createdTime"] = time_f
    data_json["lastUpdatedTime"] = time_f
    print("After to format:", json.dumps(data_json))
    return data_json


@app.route("/upload1", methods=["GET", "POST"])
def upload_1():
    try:
        if request.method == "POST":
            mongo_client = get_mongo_client("test", "test_collection")
            # if it is form data
            data = request.json
            # file = request.files["file"]
            data_format = format_upload_data(data)
            print('data:' + str(data_format))
            return upload_content_1(mongo_client, data_format)

    except Exception as ue:
        print("Error, check json format:" + str(ue))
        # current_app.logger.error("Failed to upload data to azure blob:" + str(e))
        resp = Response('Cannot upload to azure blob', status=400)
        # reps = {'status': '500', 'reason': ' cannot upload to azure blob.'}
        return resp


def upload_content_1(mongo_client, data):
    if isinstance(data, dict):
        json_data = data
    else:
        json_data = json.loads(data)

    try:
        upload_to_mongo(mongo_client, json_data['id'], data)
        content = []
        content.append(json_data)
        reps = {'status': '200', 'file_name': json_data['id'], 'content': json.dumps(content)}
        # reps = Response('success', status=200)
        return reps
    except Exception as e:
        res = Response('Cannot upload to azure blob', status=500)
        print("Failed to upload the scenario data:" + str(e))
        return res


def upload_to_mongo(mongo_client, file_name, data):
    data_str = json.dumps(data)
    mongo_client.insert_data({"file_name":file_name, "data":data_str})


# will check the valid of scenario content which upload from page
def validate_content(json_data):
    id_val = json_data.get('id')
    name = json_data.get('name')
    events = json_data.get('events')
    type = json_data.get('type')
    comb = id_val and name and events and type
    print("Valid data:", comb)
    return True
# {"id":"123131", "name":"abc", "userId":"3425", "events":[{"act":"ee"}], "type":"4" }


def list_blob(container_name):
    current_app.logger.info('[server.py] List blob from azure storage container.')
    blob_service = BlobService(BLOB_CONN_STR)
    blob_client = blob_service.client

    blobs = blob_client.list_data_blobs(container_name)
    items = []
    for blob in blobs:
        blob_json = read_blob(blob.get('name'), container_name, blob_client)
        items.append(blob_json)
    blob_service.close_client()
    return items


def read_blob(file_name, container_name, blob_client):
    try:
        blobstr = blob_client.download_data_blobs(file_name, container_name, blob_client)
        current_app.logger.info("Read blob:" + file_name)
        current_app.logger.info("Blob string: %s" % blobstr)
        blob_json = json.loads(blobstr)
        return blob_json
    except ResourceNotFoundError as ne:
        current_app.logger.warning('Target blob cannot found:' + str(ne))
        return False


if __name__ == "__main__":
    app.run(host="localhost", port=30001, debug=True)
