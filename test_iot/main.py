# coding: utf-8

# use a web request as a signal to get json data

from azure.eventhub import EventHubConsumerClient

from flask import Flask
from send_data_to_iot import iothub_send_message
from store_data_from_iot import save_data



app = Flask(__name__)



@app.route('/d2cSignal', methods=["POST", "GET"])
def index():
    if request.method == "GET":
        request_type = request.args.get("type", "sendData")
        if request_type == "sendData":
            # todo call method to consumer data from iot
            save_data()
            return "Cloud will save date."
        if request_type == "getData":
            # todo call d2c method to send data to iot
            tag = iothub_send_message()
            if tag == 1:
                return "Cloud send data successfully."
            else:
                return "Failed to send the message"
        else:
            return "Error request"





if __name__ == '__main__':
    app.run(host='0.0.0.0', port="50001")