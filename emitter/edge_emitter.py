import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv
from PIL import Image

try:
    import picamera
except Exception as exc:
    print(f'[INIT] picamera import failed: {exc}')
    picamera = None

try:
    from picamera2 import Picamera2
except Exception as exc:
    print(f'[INIT] picamera2 import failed: {exc}')
    Picamera2 = None

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

DEVICE_ID = os.getenv('DEVICE_ID', 'device-001')
BROKER_HOST = os.getenv('BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('BROKER_PORT', '1883'))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', f'devices/{DEVICE_ID}/captures')
SERVER_UPLOAD_URL = os.getenv('SERVER_UPLOAD_URL', 'http://localhost:8000/upload')
CAPTURE_INTERVAL_SECONDS = int(os.getenv('CAPTURE_INTERVAL_SECONDS', '60'))
RETRY_INTERVAL_SECONDS = int(os.getenv('RETRY_INTERVAL_SECONDS', '20'))
QUEUE_DIR = BASE_DIR / os.getenv('QUEUE_DIR', 'queue')
CAPTURE_SOURCE = os.getenv('CAPTURE_SOURCE', '')
CAMERA_RESOLUTION_WIDTH = int(os.getenv('CAMERA_RESOLUTION_WIDTH', '640'))
CAMERA_RESOLUTION_HEIGHT = int(os.getenv('CAMERA_RESOLUTION_HEIGHT', '480'))
IMAGE_MAX_WIDTH = int(os.getenv('IMAGE_MAX_WIDTH', str(CAMERA_RESOLUTION_WIDTH)))
IMAGE_MAX_HEIGHT = int(os.getenv('IMAGE_MAX_HEIGHT', str(CAMERA_RESOLUTION_HEIGHT)))
IMAGE_QUALITY = int(os.getenv('IMAGE_QUALITY', '75'))
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv('HEARTBEAT_INTERVAL_SECONDS', '30'))
MQTT_HEARTBEAT_TOPIC = os.getenv('MQTT_HEARTBEAT_TOPIC', f'devices/{DEVICE_ID}/heartbeat')
MQTT_CONNECT_RETRY_SECONDS = int(os.getenv('MQTT_CONNECT_RETRY_SECONDS', '10'))
DB_PATH = QUEUE_DIR / 'queue.db'

QUEUE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class PendingCapture:
    capture_id: str
    filename: str
    device_ts: str
    checksum: str
    status: str
    last_attempt: int
    server_timestamp: str | None = None
    error_message: str | None = None


class LocalQueue:
    def __init__(self, path: Path):
        self.path = path
        self.connection = sqlite3.connect(str(self.path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self.lock = threading.Lock()

    def _ensure_schema(self):
        with self.connection:
            self.connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS captures (
                    capture_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    device_ts TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_attempt INTEGER NOT NULL,
                    server_timestamp TEXT,
                    error_message TEXT
                )
                '''
            )

    def enqueue(self, capture_id: str, filename: str, device_ts: str, checksum: str):
        with self.lock, self.connection:
            self.connection.execute(
                '''
                INSERT OR IGNORE INTO captures (
                    capture_id, filename, device_ts, checksum, status, last_attempt
                ) VALUES (?, ?, ?, ?, 'pending', 0)
                ''',
                (capture_id, filename, device_ts, checksum),
            )

    def list_pending(self):
        with self.lock:
            rows = self.connection.execute(
                "SELECT * FROM captures WHERE status IN ('pending', 'failed') ORDER BY last_attempt ASC"
            ).fetchall()
        return [PendingCapture(**row) for row in rows]

    def mark_uploaded(self, capture_id: str, server_timestamp: str):
        with self.lock, self.connection:
            self.connection.execute(
                '''
                UPDATE captures
                SET status = 'confirmed', server_timestamp = ?, error_message = NULL
                WHERE capture_id = ?
                ''',
                (server_timestamp, capture_id),
            )

    def mark_failed(self, capture_id: str, error_message: str):
        with self.lock, self.connection:
            self.connection.execute(
                '''
                UPDATE captures
                SET status = 'failed', last_attempt = last_attempt + 1, error_message = ?
                WHERE capture_id = ?
                ''',
                (error_message, capture_id),
            )


class MqttPublisher:
    def __init__(self, host: str, port: int, topic: str, heartbeat_topic: str, device_id: str):
        self.host = host
        self.port = port
        self.topic = topic
        self.heartbeat_topic = heartbeat_topic
        self.device_id = device_id
        # Corrige o aviso de deprecação do paho-mqtt v2
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f'edge-{device_id}-{uuid.uuid4()}',
        )
        self.connected = False
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = rc == 0
        print(f'[MQTT] connected rc={rc}, connected={self.connected}')

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        print(f'[MQTT] disconnected rc={rc}')

    def _on_publish(self, client, userdata, mid):
        print(f'[MQTT] published mid={mid}')

    def start(self):
        self.client.will_set(
            self.topic,
            json.dumps({'device_id': self.device_id, 'status': 'offline'}),
            qos=1,
            retain=False,
        )
        self.client.loop_start()
        # NÃO chama connect() aqui — deixa o _connect_loop tentar em background
        thread = threading.Thread(target=self._connect_loop, daemon=True)
        thread.start()

    def _connect_loop(self):
        while True:
            if not self.connected:
                try:
                    if self.client._sock is not None:
                        self.client.reconnect()
                    else:
                        self.client.connect(self.host, self.port, keepalive=60)
                    print('[MQTT] connecting...')
                except Exception as exc:
                    print(f'[MQTT] connect attempt failed: {exc}')
            time.sleep(MQTT_CONNECT_RETRY_SECONDS)
            
    def publish(self, payload: dict) -> bool:
        if not self.connected:
            return False
        info = self.client.publish(self.topic, json.dumps(payload), qos=1)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    def publish_heartbeat(self, payload: dict) -> bool:
        if not self.connected:
            return False
        info = self.client.publish(self.heartbeat_topic, json.dumps(payload), qos=1)
        return info.rc == mqtt.MQTT_ERR_SUCCESS


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            digest.update(chunk)
    return digest.hexdigest()


def create_capture_image(source: str, destination: Path) -> None:
    if source and Path(source).exists():
        with Image.open(source) as img:
            img = img.convert('RGB')
            img.thumbnail((IMAGE_MAX_WIDTH, IMAGE_MAX_HEIGHT), Image.LANCZOS)
            img.save(destination, 'JPEG', quality=IMAGE_QUALITY)
        return

    if picamera is not None:
        try:
            with picamera.PiCamera() as camera:
                camera.resolution = (CAMERA_RESOLUTION_WIDTH, CAMERA_RESOLUTION_HEIGHT)
                time.sleep(2)
                camera.capture(str(destination), format='jpeg', quality=IMAGE_QUALITY)
            return
        except Exception as exc:
            print(f'[CAPTURE] picamera capture failed: {exc}')

    if Picamera2 is not None:
        try:
            picam2 = Picamera2()
            config = picam2.create_still_configuration(main={'size': (CAMERA_RESOLUTION_WIDTH, CAMERA_RESOLUTION_HEIGHT)})
            picam2.configure(config)
            picam2.start()
            time.sleep(2)
            picam2.capture_file(str(destination))
            picam2.close()
            return
        except Exception as exc:
            print(f'[CAPTURE] picamera2 capture failed: {exc}')

    img = Image.new('RGB', (IMAGE_MAX_WIDTH, IMAGE_MAX_HEIGHT), (120, 135, 110))
    img.save(destination, 'JPEG', quality=IMAGE_QUALITY)


def upload_capture(metadata: dict, file_path: Path) -> (bool, str):
    try:
        with file_path.open('rb') as image_file:
            files = {'image': (file_path.name, image_file, 'image/jpeg')}
            response = requests.post(
                SERVER_UPLOAD_URL,
                data=metadata,
                files=files,
                timeout=15,
            )
        response.raise_for_status()
        payload = response.json()
        server_ts = payload.get('server_timestamp', datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'))
        return True, server_ts
    except Exception as exc:
        return False, str(exc)


def build_payload(capture_id: str, device_ts: str, checksum: str, filesize: int) -> dict:
    return {
        'device_id': DEVICE_ID,
        'capture_id': capture_id,
        'device_timestamp': device_ts,
        'checksum': checksum,
        'filesize': str(filesize),
        'queue_status': 'pending',
    }


def heartbeat_loop(publisher: MqttPublisher, queue: LocalQueue, interval: int, last_capture_ref: dict):
    while True:
        if publisher.connected:
            payload = {
                'device_id': publisher.device_id,
                'status': 'online',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'queue_pending_count': len(queue.list_pending()),
                'last_capture_timestamp': last_capture_ref.get('last_capture_ts', ''),
            }
            success = publisher.publish_heartbeat(payload)
            if success:
                print('[HEARTBEAT] sent heartbeat')
            else:
                print('[HEARTBEAT] failed sending heartbeat')
        time.sleep(interval)


def main():
    queue = LocalQueue(DB_PATH)
    mqtt_publisher = MqttPublisher(BROKER_HOST, BROKER_PORT, MQTT_TOPIC, MQTT_HEARTBEAT_TOPIC, DEVICE_ID)
    mqtt_publisher.start()

    last_capture_ref = {'last_capture_ts': ''}
    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        args=(mqtt_publisher, queue, HEARTBEAT_INTERVAL_SECONDS, last_capture_ref),
        daemon=True,
    )
    heartbeat_thread.start()

    next_capture = time.monotonic()
    while True:
        now = time.monotonic()
        if now >= next_capture:
            capture_id = str(uuid.uuid4())
            filename = f'{capture_id}.jpg'
            destination = QUEUE_DIR / filename
            device_ts = datetime.utcnow().isoformat() + 'Z'
            create_capture_image(CAPTURE_SOURCE, destination)
            checksum = compute_sha256(destination)
            queue.enqueue(capture_id, filename, device_ts, checksum)
            last_capture_ref['last_capture_ts'] = device_ts
            print(f'[CAPTURE] enqueued {capture_id} {filename} size={destination.stat().st_size}')
            next_capture = now + CAPTURE_INTERVAL_SECONDS

        pending = queue.list_pending()
        for item in pending:
            path = QUEUE_DIR / item.filename
            if not path.exists():
                queue.mark_failed(item.capture_id, 'missing file')
                continue
            payload = build_payload(item.capture_id, item.device_ts, item.checksum, path.stat().st_size)
            published = mqtt_publisher.publish(payload)
            if published:
                print(f'[MQTT] published metadata for {item.capture_id}')
            success, result = upload_capture(payload, path)
            if success:
                queue.mark_uploaded(item.capture_id, result)
                print(f'[UPLOAD] confirmed {item.capture_id} server_ts={result}')
            else:
                queue.mark_failed(item.capture_id, result)
                print(f'[UPLOAD] failed {item.capture_id}: {result}')

        time.sleep(RETRY_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
