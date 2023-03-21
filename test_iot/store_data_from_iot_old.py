# coding: utf-8

import json
#import asyncio
#from azure.eventhub.aio import EventHubConsumerClient
#from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore


# import module (iot package)
# pip install azure-eventhub
# pip install azure-eventhub-checkpointstoreblob-aio


data = "{}"



def retrieve_iot_data():
    # call iot interface
    return json.load(data)


def parse_data():
    # save user information to db
    # save json string to azure blob
    return


# async def on_event(partition_context, event):
#     # Print the event data.
#     print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
#     # Update the checkpoint so that the program doesn't read the events
#     # that it has already read when you run it next time.
#     await partition_context.update_checkpoint(event)
#
# async def main():
#     # Create an Azure blob checkpoint store to store the checkpoints.
#     checkpoint_store = BlobCheckpointStore.from_connection_string("AZURE STORAGE CONNECTION STRING", "BLOB CONTAINER NAME")
#
#     # Create a consumer client for the event hub.
#     client = EventHubConsumerClient.from_connection_string("EVENT HUBS NAMESPACE CONNECTION STRING", consumer_group="$Default", eventhub_name="EVENT HUB NAME", checkpoint_store=checkpoint_store)
#     async with client:
#         # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
#         await client.receive(on_event=on_event,  starting_position="-1")
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     # Run the main method.
#     loop.run_until_complete(main())


#-------------------------------------------------------------------------------------------
from azure.storage.blob import BlobServiceClient
connection_string=''
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client("<container name>")
blob_client = container_client.get_blob_client("<blob name>")
with open("/tmp/azure-blob.txt", "rb") as blob_file:
    blob_client.upload_blob(data=blob_file)


#------------------------------------------------------------------------------------------------


#!/usr/bin/env python

# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""
An example to show receiving events from an Event Hub.
"""
import os
from azure.eventhub import EventHubConsumerClient

CONNECTION_STR = os.environ["EVENT_HUB_CONN_STR"]
EVENTHUB_NAME = os.environ['EVENT_HUB_NAME']


def on_event(partition_context, event):
    # Put your code here.
    # If the operation is i/o intensive, multi-thread will have better performance.
    print("Received event from partition: {}.".format(partition_context.partition_id))


def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))



if __name__ == '__main__':
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group='$Default',
        eventhub_name=EVENTHUB_NAME,
    )

    try:
        with consumer_client:
            consumer_client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_initialize,
                on_partition_close=on_partition_close,
                on_error=on_error,
                starting_position="-1",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')
