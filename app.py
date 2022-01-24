from flask import Flask, request, jsonify
import requests
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
import json

load_dotenv()
config_data = {}
config_data["mysql-engine"] = os.getenv('mysql-engine')
engine = create_engine(config_data["mysql-engine"])


app = Flask(__name__)

#@app.route('/up')
@app.route('/up', methods=['POST'])
def forward_up():
    table = 'testacht1'
    print(json.dumps(request.json, indent=4, sort_keys=True))
    df = pd.DataFrame([request.json['uplink_message']['received_at'],
            request.json['end_device_ids']['dev_eui'],
            request.jsond['uplink_message']['decoded_payload']['analog_in_4'],
            request.json['uplink_message']['decoded_payload']['luminosity_6'],
            request.json['uplink_message']['decoded_payload']['temperature_5'],
            request.json['uplink_message']['decoded_payload']['digital_in_1'],
            request.json['uplink_message']['decoded_payload']['digital_in_2'],
            request.json['uplink_message']['decoded_payload']['digital_in_3'],
            request.json['uplink_message']['decoded_payload']['accelerometer_7']['x'],
            request.json['uplink_message']['decoded_payload']['accelerometer_7']['y'],
            request.json['uplink_message']['decoded_payload']['accelerometer_7']['z'],
            request.json['uplink_message']['rx_metadata']['length'],
            request.json['uplink_message']['settings']['data_rate']['lora']['bandwidth'],
            request.json['uplink_message']['settings']['data_rate']['lora']['spreading_factor']],
        columns=[   'timestamp1',
                    'hardware_serial',
                    'battery',
                    'luminosity',
                    'temperature',
                    'digital_in1',
                    'digital_in2',
                    'digital_in3',
                    'acc_x',
                    'acc_y',
                    'acc_z',
                    'gateways',
                    'BW',
                    'SF'])
    pd['timestamp1'] = pd.to_datetime(df['timestamp1'])
    print(df)
    with engine.connect() as con:
        df.to_sql(name=table, con=con, if_exists='replace', index=True)
    engine.dispose()
    print(f"insert done into: {table}")
    return jsonify(success=1, response = "ok")


@app.route("/")
def index():
    return "Hello World!"

def influxdb(payload):
    influx_data = 'aqi,version={} pm25={},pm10={},temperature={},'\
                  'humidity={},voltage={},duration={}' \
        .format(payload['version'], payload['pm25'],
                payload['pm10'], payload['temperature'],
                payload['humidity'], payload['vbatt'],
                payload['duration'])
    return requests.post("http://ax616034.ngrok.io/write?db=mydb",
                         data=influx_data).content
