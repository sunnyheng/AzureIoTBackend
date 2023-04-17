# coding: utf-8

#=========================================Azure IOT Connection String===================================================
# IOTHUB_PHONE_CONN = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
# IOTHUB_CAR_CONN = "HostName=iot.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4="
# IOTHUB_CONNS = [IOTHUB_PHONE_CONN, IOTHUB_CAR_CONN]
#
# PHONE_DEVICE_ID = "mydevice"
# CAR_DEVICE_ID = "device20221220"
# DEVICE_IDS = [PHONE_DEVICE_ID, CAR_DEVICE_ID]
#
#
#
# #=========================================Azure Blob Connection String==================================================
# BLOB_CONN_STR = 'DefaultEndpointsProtocol=https;AccountName=demoiotstorage;AccountKey=4xzskMGgXFyTO0IUoSSZmBpmn28/OP7oI+VguFiLh7L5mXa6qLMbIVbJwbC55UlYisopUhdZXfTn+AStQQCBnQ==;EndpointSuffix=core.chinacloudapi.cn'
# CONTAINER_NAME = "iotcontainer"
# SCENARIO_TMPL_NAME = "tmplscenario"


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#++++++++++++++++++++++++++++++++++move to new azure env ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# IOTHUB_PHONE_CONN = "HostName=issec-iot.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI="
IOTHUB_PHONE_CONN = "HostName=issec-dev.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec="

# IOTHUB_CAR_CONN = "HostName=issec-iot.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI="
IOTHUB_CAR_CONN = "HostName=issec-dev.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec="


# Endpoint=sb://iothub-ns-issec-dev-24881209-91f2b9a6de.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec=;EntityPath=issec-dev
# CONN_EVENT_HUB="Endpoint=sb://ihsuprodhkres016dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI=;EntityPath=iothub-ehub-issec-iot-24732222-7b8696b292"
CONN_EVENT_HUB="Endpoint=sb://iothub-ns-issec-dev-24881209-91f2b9a6de.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=S3tabkHGbNZkYGUBl/zeyqd9LFhel7tWDqhY3HT7Mec=;EntityPath=issec-dev"
CONSUMER_GROUP="$Default"

BLOB_CONN_STR="DefaultEndpointsProtocol=https;AccountName=issecstorage;AccountKey=yDc44BJGtLYF/4E7JZwARzp/AWkidsVgIZVjd6CzzKY2x8uG0vurTt8va3EUmIoCnLZg2XEMerOr+AStTphecg==;EndpointSuffix=core.windows.net"

SCENARIO_USER = "scenaroiuser"
SCENARIO_SQUARE = "scenariosquare"
