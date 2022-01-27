from flask import Flask, request, jsonify
import requests
from dotenv import load_dotenv
import os
import sqlalchemy as db
import pandas as pd
import json
import numpy as np

load_dotenv()
config_data = {}
config_data["mysql-engine"] = os.getenv('mysql-engine')
engine = db.create_engine(config_data["mysql-engine"])

app = Flask(__name__)


@app.route('/up', methods=['POST'])
def forward_up():
    table = 'testacht1'
    print(json.dumps(request.json, indent=4, sort_keys=True))
    df = pd.DataFrame(np.array([[request.json['uplink_message']['received_at'],
                                 request.json['end_device_ids']['dev_eui'],
                                 request.json['uplink_message']['decoded_payload']['analog_in_4'],
                                 request.json['uplink_message']['decoded_payload']['luminosity_6'],
                                 request.json['uplink_message']['decoded_payload']['temperature_5'],
                                 request.json['uplink_message']['decoded_payload']['digital_in_1'],
                                 request.json['uplink_message']['decoded_payload']['digital_in_2'],
                                 request.json['uplink_message']['decoded_payload']['digital_in_3'],
                                 request.json['uplink_message']['decoded_payload']['accelerometer_7']['x'],
                                 request.json['uplink_message']['decoded_payload']['accelerometer_7']['y'],
                                 request.json['uplink_message']['decoded_payload']['accelerometer_7']['z'],
                                 len(request.json['uplink_message']['rx_metadata']),
                                 request.json['uplink_message']['settings']['data_rate']['lora']['bandwidth'],
                                 request.json['uplink_message']['settings']['data_rate']['lora']['spreading_factor']]]),
                      columns=['timestamp1',
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
    df['timestamp1'] = pd.to_datetime(df['timestamp1'])
    print(df)
    with engine.connect() as con:
        df.to_sql(name=table, con=con, if_exists='append', index=False)
    engine.dispose()
    print(f"insert done into: {table}")

    with engine.connect() as con:
        df = pd.read_sql_query('''select id from testacht1 order by id desc limit 1''', con)
    engine.dispose()

    df_gatew = pd.DataFrame()
    for rxmetadata in request.json['uplink_message']['rx_metadata']:
        print(rxmetadata)
        df_gatew.append(pd.DataFrame(np.array([[
            df['id'],
            rxmetadata['time'],
            rxmetadata['gateway_ids']['gateway_id'],
            rxmetadata['gateway_ids']['eui'],
            rxmetadata['rssi'],
            rxmetadata['snr']]], dtype="object"),
            columns=['testacht_id', 'timestamp', 'gateway_id', 'eui', 'rssi', 'snr']),
            ignore_index=True)

    table = 'testacht_gateways1'
    with engine.connect() as con:
        df_gatew.to_sql(name=table, con=con, if_exists='append', index=False)
    engine.dispose()
    print(f"insert done into: {table}")

    print(df_gatew)
    return jsonify(success=1, response="ok")


@app.route("/")
def index():
    return "Hello World!"
