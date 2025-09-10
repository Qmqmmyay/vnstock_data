import os
import argparse
import requests
import json
import csv
import logging
import random
import time
import yaml
import ast

from paho.mqtt import client as mqtt_client
from paho.mqtt.client import MQTTv5
from paho.mqtt.subscribeoptions import SubscribeOptions


def append_tick_to_csv(tick_data, filename='tick_data.csv'):
    fieldnames = ['symbol', 'matchPrice', 'matchQtty', 'time', 'side', 'session', 'low', 'open', 'lastUpdated', 'volume', 'close', 'type', 'high']
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(tick_data)


def yaml_creds(path: str):
    """
    Đọc thông tin từ file cấu hình định dạng yaml. 
    user name phải là số điện thoại, số lưu ký hoặc email.
    
    Tham số:
        - path: str: Đường dẫn đến file cấu hình.
    
    Trả về:
        - username (str): giá trị username được lấy từ file cấu hình.
        - password: (str): giá trị password được lấy từ file cấu hình.

    Tài liệu API: https://hdsd.dnse.com.vn/san-pham-dich-vu/api-lightspeed/iii.-market-data/2.-dac-ta-thong-tin-cac-message/2.1.-moi-truong
    """
    try:
        with open(path, 'r') as f:
            creds = yaml.safe_load(f)
            if not isinstance(creds, dict):
                raise ValueError("The credentials file format is incorrect. Expected a dictionary.")
            username = creds['usr']
            password = creds['pwd']
        return username, password
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {path} does not exist.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing the YAML file: {e}")
    except KeyError as e:
        raise KeyError(f"Missing required key in the credentials file: {e}")

class Auth:
    def __init__(self, username: str, password: str):
        """
        Xác thực kết nối tới DNSE API.

        Tham số:
            - username (str): giá trị username được lấy từ file cấu hình.
            - password (str): giá trị password được lấy từ file cấu hình.

        Trả về:
            - jwt_token (str): giá trị token được lấy từ DNSE API, có hiệu lực trong 8 tiếng.
            - investor_id (str): giá trị investor_id được lấy từ DNSE API.

        Tài liệu API: https://hdsd.dnse.com.vn/san-pham-dich-vu/api-lightspeed/iii.-market-data/2.-dac-ta-thong-tin-cac-message/2.1.-moi-truong
        """
        self.username = username
        self.password = password
        self.token = self._get_token()
        self.investor_id = self._get_investorid()

    def _get_token(self):
        """
        Extract token key from DNSE API
        """
        url = "https://services.entrade.com.vn/dnse-user-service/api/auth"

        payload = json.dumps({
            "username": self.username,
            "password": self.password
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            return response.json()['token']
        else:
            response.raise_for_status()

    def _get_investorid(self):
        """
        Extract investor id from DNSE API
        """
        url = "https://services.entrade.com.vn/dnse-user-service/api/me"
        headers = {
            'Content-Type': 'application/json',
            'authorization': f'Bearer {self.token}'
        }

        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            investor_data = response.json()
            return investor_data['investorId']  # Adjust this line based on actual JSON structure
        else:
            response.raise_for_status()

class Config:
    """
    Application Configuration Class
    """
    def __init__(self, creds_path: str, topics: tuple):
        self.user_name, self.password = yaml_creds(creds_path)
        self.auth = Auth(self.user_name, self.password)
        self.BROKER = 'datafeed-lts.dnse.com.vn'
        self.PORT = 443
        self.TOPICS = topics
        self.CLIENT_ID = f'python-json-mqtt-ws-sub-{random.randint(0, 1000)}'
        self.USERNAME = self.auth.investor_id
        self.PASSWORD = self.auth.token
        self.FIRST_RECONNECT_DELAY = 1
        self.RECONNECT_RATE = 2
        self.MAX_RECONNECT_COUNT = 12
        self.MAX_RECONNECT_DELAY = 60

class MQTTClient:
    """
    Class encapsulating MQTT Client related functionalities
    """
    def __init__(self, config: Config):
        self.config = config
        self.client = mqtt_client.Client(client_id=self.config.CLIENT_ID, protocol=MQTTv5, transport='websockets')
        self.client.username_pw_set(self.config.USERNAME, self.config.PASSWORD)
        self.client.tls_set_context()
        self.client.ws_set_options(path="/wss")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.FLAG_EXIT = False

    def connect_mqtt(self):
        self.client.connect(self.config.BROKER, self.config.PORT, keepalive=120)
        return self.client

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0 and client.is_connected():
            logging.info("Connected to MQTT Broker!")
            topic_tuple = [(topic, SubscribeOptions(qos=2)) for topic in self.config.TOPICS]
            self.client.subscribe(topic_tuple)
        else:
            logging.error(f'Failed to connect, return code {rc}')

    def on_disconnect(self, client, userdata, rc, properties=None):
        logging.info("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, self.config.FIRST_RECONNECT_DELAY
        while reconnect_count < self.config.MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= self.config.RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, self.config.MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)
        self.FLAG_EXIT = True

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


def run(creds_path, topics):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

    # Parse topics string into a list of strings
    if isinstance(topics, str):
        try:
            topics = ast.literal_eval(topics)
            if not isinstance(topics, list):
                raise ValueError("Topics should be a list of strings.")
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse topics: {e}")
    
    # Ensure topics is a list of strings
    if not all(isinstance(topic, str) for topic in topics):
        raise ValueError("All topics must be strings.")
    
    topics_tuple = tuple(topics)
    logging.debug(f"Parsed topics: {topics_tuple}")

    config = Config(creds_path, topics_tuple)
    my_mqtt_client = MQTTClient(config)
    client = my_mqtt_client.connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MQTT Client CLI App")
    parser.add_argument('creds_path', type=str, help="Path to the creds.yaml file")
    parser.add_argument('--topics', type=str, nargs='+', default=["plaintext/quotes/derivative/OHLC/1/VN30F1M", "plaintext/quotes/stock/tick/+"], help="List of topics to subscribe to")

    args = parser.parse_args()

    run(args.creds_path, args.topics)