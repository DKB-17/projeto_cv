import json
import os
import sqlite3
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

BROKER_HOST = os.getenv('BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('BROKER_PORT', '1883'))
DB_PATH = Path(os.getenv('DB_PATH', BASE_DIR / 'server.db'))


def init_db():
    connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS mqtt_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
        ''',
    )
    connection.commit()
    return connection


DB = init_db()
DB.row_factory = sqlite3.Row


def log_message(topic: str, payload: dict):
    metadata = json.dumps(payload)
    with DB:
        DB.execute(
            'INSERT INTO mqtt_logs (topic, payload, received_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
            (topic, metadata),
        )
    print(f'[MQTT] logged message from topic={topic}')


def on_connect(client, userdata, flags, rc):
    print(f'[MQTT] connected rc={rc}')
    if rc == 0:
        client.subscribe('devices/+/captures', qos=1)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
    except Exception as exc:
        print(f'[MQTT] cannot decode message: {exc}')
        return
    log_message(msg.topic, payload)


def main():
    client = mqtt.Client(client_id=f'server-listener-{os.getpid()}')
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()


if __name__ == '__main__':
    main()
