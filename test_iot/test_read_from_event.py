# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""
This sample demonstrates how to use the Microsoft Azure Event Hubs Client for Python sync API to
read messages sent from a device. Please see the documentation for @azure/event-hubs package
for more details at https://pypi.org/project/azure-eventhub/
For an example that uses checkpointing, follow up this sample with the sample in the
azure-eventhub-checkpointstoreblob package on GitHub at the following link:
https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/eventhub/azure-eventhub-checkpointstoreblob/samples/receive_events_using_checkpoint_store.py
"""

from azure.eventhub import EventHubConsumerClient

#
# CONNECTION_STR=f"Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"
# "Endpoint=sb://iothub-ns-iot1-1603692-f697d3dc4f.servicebus.chinacloudapi.cn/;SharedAccessKeyName=iothubowner;SharedAccessKey=yD9DohWowukZsKZya9ekiBhGa78uRIf+198paU4W/VI=;EntityPath=iot1"

CONNECTION_STR="Endpoint=sb://ihsuprodhkres016dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=IzKVWsLMKvNnnOS55NEZZKUHF4TmSx8cOznJtQy7SJI=;EntityPath=iothub-ehub-issec-iot-24732222-7b8696b292"
# Define callbacks to process events
def on_event_batch(partition_context, events):
    for event in events:
        # print("Received event from partition: {}.".format(partition_context.partition_id))
        print("Telemetry received: ", event.body_as_str())
        print("---------------------------------------------")
        # print("Telemetry received dir event: ", dir(event))
        print("Properties (set by device): ", dir(event.properties))
        # print("Properties (set by device)system_properties: type", type(event.system_properties))
        print("system_properties (set by device) keys:" )
        print(event.system_properties.keys() )
        # print(event.system_properties.fromkeys("iot-hub-connection-device-id"))
        print("-----------------------------------------------------------------1:")
        print(event.system_properties.get(b"iothub-connection-device-id"))
        print("system_properties (set by device)value2:")
        # for i in event.system_properties.items():
        #     print(i)
        #     print(type(i))
        #     # print(i[0])
        #     print("================")
            # print(i[1])

        print("System properties (set by IoT Hub): ", event.system_properties)
        print()
    partition_context.update_checkpoint()

def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


def main():
    print("Starting.....")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default")
    try:
        with client:
            client.receive_batch(
                on_event_batch=on_event_batch,
                on_error=on_error
            )
    except KeyboardInterrupt:
        print("Receiving has stopped.")

if __name__ == '__main__':
    main()
