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
    #influx_response = influxdb(request.json['payload_fields'])
    #success = len(influx_response) == 0
    #df =
    #with engine.connect() as con:
    #    df.to_sql(name=table, con=con, if_exists='replace', index=True)
    #engine.dispose()
    #print(f"insert done into: {table}")
    print(json.dumps(request.json, indent=4, sort_keys=True))

    return jsonify(success=1, response = "ok")
    #return jsonify(success=success,
    #              response=influx_response.decode('utf-8'))


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
