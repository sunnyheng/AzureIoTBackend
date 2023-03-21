# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
import os, json
import random
import time
from azure.iot.device import IoTHubDeviceClient, Message

# The device connection authenticates your device to your IoT hub. The connection string for
# a device should never be stored in code. For the sake of simplicity we're using an environment
# variable here. If you created the environment variable with the IDE running, stop and restart
# the IDE to pick up the environment variable.
#
# You can use the Azure CLI to find the connection string:
# az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table

# CONNECTION_STRING = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
CONNECTION_STRING='HostName=iot1.azure-devices.cn;DeviceId=mydevice;SharedAccessKeyName=iothubowner;SharedAccessKey=BClp1n7JHHtblo4PvTCNpMEHR3byA4NZMEgwhMzTrEY='


# Define the JSON message to send to IoT Hub.
TEMPERATURE = 20.0
HUMIDITY = 60
# MSG_TXT = {'test.json':'{"Testing_name": "sunny_test1111","test_value": "12344444"}'}
MSG_TXT = '{"test.json": "{\'test\':\'purple\'}","humidity": "humidity"}'


def run_telemetry_sample(client):
    # This sample will send temperature telemetry every second
    print("IoT Hub device sending periodic messages")

    client.connect()
    # json_str = json.dumps(MSG_TXT)
    print("1111111111111:", MSG_TXT)
    message = Message(MSG_TXT)

    # Send the message.
    print("Sending message: {}".format(message))
    client.send_message(message)
    print("Message successfully sent")
    time.sleep(10)


def main():
    print("IoT Hub Quickstart #1 - Simulated device")
    print("Press Ctrl-C to exit")

    # Instantiate the client. Use the same instance of the client for the duration of
    # your application
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

    # Run Sample
    try:
        run_telemetry_sample(client)
    except KeyboardInterrupt:
        print("IoTHubClient sample stopped by user")
    finally:
        # Upon application exit, shut down the client
        print("Shutting down IoTHubClient")
        client.shutdown()

if __name__ == '__main__':
    main()