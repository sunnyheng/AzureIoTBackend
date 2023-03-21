# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""
This sample demonstrates how to use the Microsoft Azure Event Hubs Client for Python async API to
read messages sent from a device. Please see the documentation for @azure/event-hubs package
for more details at https://pypi.org/project/azure-eventhub/
For an example that uses checkpointing, follow up this sample with the sample in the
azure-eventhub-checkpointstoreblob package on GitHub at the following link:
https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/eventhub/azure-eventhub-checkpointstoreblob-aio/samples/receive_events_using_checkpoint_store_async.py
"""

import asyncio
from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubConsumerClient

# If you have access to the Event Hub-compatible connection string from the Azure portal, then
# you can skip the Azure CLI commands above, and assign the connection string directly here.
# CONNECTION_STR = f'Endpoint={EVENTHUB_COMPATIBLE_ENDPOINT}/;SharedAccessKeyName=service;SharedAccessKey={IOTHUB_SAS_KEY};EntityPath={EVENTHUB_COMPATIBLE_PATH}'
EVENTHUB_CONN_STR="Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
consumer_group='$Default'

# Define callbacks to process events
async def on_event_batch(partition_context, events):
    for event in events:
        print("Received event from partition: {}.".format(partition_context.partition_id))
        print("Telemetry received: ", event.body_as_str())
        print("Properties (set by device): ", event.properties)
        print("System properties (set by IoT Hub): ", event.system_properties)
        print()
    await partition_context.update_checkpoint()


async def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))

IOTHUB_PHONE_CONN = "HostName=iot1.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI="
IOTHUB_CAR_CONN = "HostName=iot.azure-devices.cn;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4="

CONN_EVENT_HUB_PHONE="Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
CONN_EVENT_HUB_CAR="Endpoint=sb://iothub-ns-iot-1584346-5c22537e4b.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=BVECQWdDdHorYX6I+CbuAjeK6jXjikdaMSLCe2e3Zt4=;EntityPath=iot"


def main():
    loop = asyncio.get_event_loop()
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONN_EVENT_HUB_PHONE,
        consumer_group="$Default",
        # transport_type=TransportType.AmqpOverWebsocket,  # uncomment it if you want to use web socket
    )
    try:
        loop.run_until_complete(client.receive_batch(on_event_batch=on_event_batch, on_error=on_error))
    except KeyboardInterrupt:
        print("Receiving has stopped.")
    finally:
        loop.run_until_complete(client.close())
        loop.stop()


if __name__ == '__main__':
    main()