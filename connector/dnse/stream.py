# python 3.11

# Hướng dẫn: https://hdsd.dnse.com.vn/san-pham-dich-vu/api-lightspeed/iii.-market-data/2.-dac-ta-thong-tin-cac-message/2.2.-topics
# Gói phụ thuộc: paho>=2.0.0 >> Chạy lệnh pip install paho để cài bản mới nhất.

import requests
import json
import logging
import random
import time
import os
import csv
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import MQTTv5
from paho.mqtt.subscribeoptions import SubscribeOptions
import yaml

def append_tick_to_csv(tick_data, filename='tick_data.csv'):
    # fieldnames = ['symbol', 'matchPrice', 'matchQtty', 'time', 'side', 'session']
    fieldnames = ['symbol', 'matchPrice', 'matchQtty', 'time', 'side', 'session', 'low', 'open', 'lastUpdated', 'volume', 'close', 'type', 'high']
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(tick_data)

# Load credentials from creds.yaml
with open('/content/drive/MyDrive/Colab Notebooks/config/dnse_creds.yaml') as f:
# with open('creds.yaml') as f:
    creds = yaml.safe_load(f)
    username = creds['usr']
    password = creds['pwd']

def dnse_auth(username, password):
    url = "https://services.entrade.com.vn/dnse-user-service/api/auth"
    payload = json.dumps({"username": username, "password": password})
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        token = response.json()['token']
        return token

def account_info(jwt_token):
    url = "https://services.entrade.com.vn/dnse-user-service/api/me"
    headers = {
        'Content-Type': 'application/json',
        'authorization': f'Bearer {jwt_token}'
    }
    response = requests.request("GET", url, headers=headers)
    if response.status_code == 200:
        return response.json()

jwt_token = dnse_auth(username, password)
investor_id = account_info(jwt_token)['investorId']

class Config:
    BROKER = 'datafeed-lts.dnse.com.vn'
    PORT = 443
    TOPICS = ("plaintext/quotes/derivative/OHLC/1/VN30F1M", "plaintext/quotes/stock/tick/+")
    CLIENT_ID = f'python-json-mqtt-{random.randint(0, 1000)}'
    USERNAME = investor_id
    PASSWORD = jwt_token

class MQTTClient:
    def __init__(self):
        self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,
                                         Config.CLIENT_ID,
                                         protocol=MQTTv5,
                                         transport='websockets')
        self.client.username_pw_set(Config.USERNAME, Config.PASSWORD)
        self.client.tls_set_context()
        self.client.ws_set_options(path="/wss")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def connect_mqtt(self):
        self.client.connect(Config.BROKER, Config.PORT, keepalive=120)
        return self.client

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
            topic_tuple = [(topic, SubscribeOptions(qos=2)) for topic in Config.TOPICS]
            self.client.subscribe(topic_tuple)
        else:
            logging.error(f'Failed to connect, return code {rc}')

    def on_disconnect(self, client, userdata, rc, properties=None):
        logging.info("Disconnected with result code: %s", rc)

    # def on_message(self, client, userdata, msg):
    #     payload = json.loads(msg.payload.decode())
    #     logging.debug(f"Received tick data: {payload}")
    #     append_tick_to_csv(payload)

    def on_message(self, client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        
        # Perform cleaning and validation on payload here
        # Example: Validate matchPrice and matchQtty exist and are numbers
        
        if 'matchPrice' in payload and 'matchQtty' in payload:
            try:
                payload['matchPrice'] = float(payload['matchPrice'])
                payload['matchQtty'] = float(payload['matchQtty'])
                # Proceed to append data to CSV or directly to database
                append_tick_to_csv(payload)
                logging.debug(f"Received tick data: {payload}")
            except ValueError:
                logging.error("Invalid data format, skipping tick.")


def run():
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
    mqtt_client = MQTTClient()
    client = mqtt_client.connect_mqtt()
    client.loop_forever()

if __name__ == '__main__':
    run()