# coding: utf-8
import os
import re

from flask import Flask, render_template, request, Response, make_response
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from flask_cors import CORS

from utils.blob_operation import BlobService

import json, logging
# from init_logger import setup_log

from send_c2d_message import send_mes_to_multi_iothub
from connction_string import IOTHUB_CONNS, DEVICE_IDS
from connction_string import BLOB_CONN_STR, CONTAINER_NAME, SCENARIO_TMPL_NAME


app = Flask(__name__)
CORS(app)

# logger = setup_log("page")

# LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
# DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
# logging.basicConfig(filename='my.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)


# def setup_log(log_name):
#     logger = logging.getLogger(log_name)
#     log_path = os.path.join("C:\cloud_app\cloud_service\server\static\log", log_name)
#     logger.setLevel(logging.DEBUG)
#     file_handler = TimedRotatingFileHandler(filename=log_path, when="MIDNIGNT", interval=1, backupCount=30)
#     file_handler.suffix = "%Y-%m-%d.log"
#     file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
#     file_handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s"))
#     logger.addHandler(file_handler)
#     return logger


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
    container_name = CONTAINER_NAME
    if type == "ScenarioSquare":
        container_name = SCENARIO_TMPL_NAME
    print(container_name)
    blob_list = []
    print("blob file_name" )
    print(file_name)
    if file_name and file_name != "undefined":

        # file_name = "user_vin_1001"
        blob_json = read_blob(file_name, container_name)
        if blob_json:
            blob_list.append(blob_json)
            return blob_list
        else: return Response('Target blob cannot found', status=404)

    else:
        blob_items = list_blob(container_name)
        # print("get blob item:" + str(blob_items))
        # logger.info("get blob item:" + str(blob_items))
        blob_list.extend(blob_items)
        return blob_list


@app.route("/publish", methods=["POST"])
def publish():
    # logger.info("[server.py] Front end call publish function.")
    content = request.json
    if content:
        data_dict = {}
        # the key(should be blob name) is no meaning hera, app does not use it, just keep the same logic
        key = content.get('id') or "test_key"
        data_dict[key] = json.dumps(content)
        data_dict["ScenarioType"] = "ScenarioSquare"
        # logger.info("[server.py] publish content:" + json.dumps(content))
        print("Start to send c2d message.")
        if send_mes_to_multi_iothub(json.dumps(data_dict), IOTHUB_CONNS, DEVICE_IDS):
            return Response("success", 200)
        else: return Response("failed", 500)
    else:
        print("The cotent is empty.")


@app.route("/delete", methods=["DELETE"])
def del_record():
    id = request.args.get('id')
    type = request.args.get('type')
    container_name = SCENARIO_TMPL_NAME
    if type=='ScenarioUser':
        print('user data.')
        container_name = CONTAINER_NAME
    del_status = del_azure_blob(id, container_name)
    if del_status:
        del_list = []
        delete_resp = {}
        del_list.append(id)
        delete_resp["id"] = str(del_list)
        delete_resp["operation"] = "DELETE"
        delete_resp["ScenarioType"] = "ScenarioUser"
        send_mes_to_multi_iothub(json.dumps(delete_resp), IOTHUB_CONNS, DEVICE_IDS, logger)
        print('Deleted '+ id)
        return Response("success", 200)
    else:
        return Response("failed", 404)

# upload function in cloud web, only upload the scenario template
@app.route("/upload", methods=["GET", "POST"])
def upload_data():

    try:
        if request.method == "POST":
            # if it is form data
            data = request.json
            # file = request.files["file"]
            print('data:'+ str(data))
            return upload_content(data)

    except Exception as ue:
        print("Error, check json format:" + str(ue))
        # logger.error("Failed to upload data to azure blob:" + str(e))
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
            upload_to_azure_blob(json_data['id'], data)
            reps = {'status': '200', 'file_name':json_data['id']}
            # reps = Response('success', status=200)
            return reps
        except Exception as e:
            res = Response('Cannot upload to azure blob', status=500)
            print("Failed to upload the scenario data:" + str(e))
            return res
    else:
        res = Response('Data Not valid', status=400)
        # reps = {'status': '500', 'reason': ' data is not valid.'}
        return res


# will check the valid of scenario content which upload from page
def validate_content(json_data):
    id_val = json_data.get('id')
    name = json_data.get('name')
    events = json_data.get('events')
    type = json_data.get('type')
    comb = id_val and name and events and type
    return comb
# {"id":"123131", "name":"abc", "userId":"3425", "events":[{"act":"ee"}], "type":"4" }

def upload_to_azure_blob(file_name, data):
    # BLOB_CONN_STR = 'DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
    # CONTAINER_NAME = "iotcontainer"
    name = str(file_name)
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(SCENARIO_TMPL_NAME)
    if isinstance(data, dict):
        data = json.dumps(data)
    container_client.upload_blob(name, data, overwrite=True)
    # logger.info("[iot] Finished to upload file:" + key)
    print("Finished to upload file:" + name)


def list_blob(container_name):
    # logger.info('[server.py] List blob from azure storage container.')
    # blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    # container_client = blob_service_client.get_container_client(container_name)
    container_client = create_blob_container_client(container_name)
    blobs = container_client.list_blobs()
    items = []
    for blob in blobs:
        print('Test:' + blob.get('name'))
        blob_json = read_blob(blob.get('name'), container_name, container_client)
        items.append(blob_json)
    return items


def create_blob_container_client(container_name):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client



def read_blob(file_name, container_name, container_client=None):
    # blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    # container_client = blob_service_client.get_container_client(container_name)
    if not container_client:
        container_client = create_blob_container_client(container_name)
    blob_client = container_client.get_blob_client(file_name)
    try:
        blobstr = blob_client.download_blob().readall().decode("utf-8")
        # logger.info("[server.py] Read blob:" + file_name)
        # logger.info("[server.py] blobstr:%s" %blobstr)
        blob_json = json.loads(blobstr)
        blob_client.close()
        return blob_json
    except ResourceNotFoundError as ne:
        # logger.warning('Target blob cannot found:'+ str(ne))
        return False


def del_azure_blob(id, container_name, container_client=None):
    if not container_client:
        container_client = create_blob_container_client(container_name)
    try:
        blob_client = container_client.get_blob_client(id)
        blob_client.delete_blob()
        return True
    except Exception as e:
        print('Failed to delete azure blob:' + str(id))
        print('Failed to delete azure blob2:' + str(e))

        return False


if __name__ == "__main__":
    app.run(host="localhost", port="30001", debug=True)