# coding: utf-8

import json



IOTHUB_PHONE_CONN = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="

from azure.iot.hub.iothub_http_runtime_manager import IoTHubHttpRuntimeManager
iothub_http_runtime_manager = IoTHubHttpRuntimeManager.from_connection_string(IOTHUB_PHONE_CONN)
feedback = iothub_http_runtime_manager.receive_feedback_notification()