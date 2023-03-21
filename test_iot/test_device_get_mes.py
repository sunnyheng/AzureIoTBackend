# coding: utf-8

from azure.iot.device import IoTHubDeviceClient
import time

# connection_string='HostName=iot1.azure-devices.cn;DeviceId=mydevice;SharedAccessKeyName=iothubowner;SharedAccessKey=BClp1n7JHHtblo4PvTCNpMEHR3byA4NZMEgwhMzTrEY='
DEVICE_CAR = "HostName=issec-iot.azure-devices.net;DeviceId=car;SharedAccessKeyName=iothubowner;SharedAccessKey=M+5EMgJ/aZJe/Zq3AveMiwRDEr4B8wlm2O1fdgZzoco="


# def message_handler(message):
# 	try:
# 		print(message.data)
# 		print('finished..')
#
# 	except Exception as e:
# 		print(e)
#
#
# def create_client():
# 	client = IoTHubDeviceClient.create_from_connection_string(connection_string)
# 	try:
# 		client.on_message_received =  message_handler
#
# 	except Exception as e:
# 		client.shutdown()
# 		print("error:", str(e))



def message_handler(message):
    print("")
    print("Message received:")

    print(dir(message))
    print("user_id:", message.user_id)
    print("expire time:", message.expiry_time_utc)
    # print("data:")
    # print(message.data)



def main():
    print ("Starting the Python IoT Hub C2D Messaging device sample...")

    # Instantiate the client
    client = IoTHubDeviceClient.create_from_connection_string(DEVICE_CAR)

    print ("Waiting for C2D messages, press Ctrl-C to exit")
    try:
        # Attach the handler to the client
        client.on_message_received = message_handler

        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        print("IoT Hub C2D Messaging device sample stopped")
    finally:
        # Graceful exit
        print("Shutting down IoT Hub Client")
        client.shutdown()


if __name__ == '__main__':
	main()

