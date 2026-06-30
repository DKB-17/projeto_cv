import hashlib
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / 'object_store'))
DB_PATH = Path(os.getenv('DB_PATH', BASE_DIR / 'server.db'))
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title='Agriculture Pest Monitoring Ingest API')


def init_db():
    connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS captures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            capture_id TEXT NOT NULL,
            device_timestamp TEXT NOT NULL,
            server_timestamp TEXT NOT NULL,
            checksum TEXT NOT NULL,
            filepath TEXT NOT NULL,
            filesize INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.commit()
    return connection

DB = init_db()
DB.row_factory = sqlite3.Row


def compute_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            digest.update(chunk)
    return digest.hexdigest()


def make_storage_path(device_id: str, capture_id: str) -> Path:
    now = datetime.utcnow()
    storage_path = DATA_DIR / device_id / now.strftime('%Y') / now.strftime('%m') / now.strftime('%d')
    storage_path.mkdir(parents=True, exist_ok=True)
    return storage_path / f'{capture_id}.jpg'


def insert_capture_record(device_id: str, capture_id: str, device_ts: str, checksum: str, filepath: str, filesize: int, status: str):
    server_ts = datetime.utcnow().isoformat() + 'Z'
    with DB:
        DB.execute(
            '''
            INSERT INTO captures (
                device_id, capture_id, device_timestamp, server_timestamp,
                checksum, filepath, filesize, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (device_id, capture_id, device_ts, server_ts, checksum, filepath, filesize, status, server_ts),
        )
    return server_ts


@app.post('/upload')
async def upload_capture(
    device_id: str = Form(...),
    capture_id: str = Form(...),
    device_timestamp: str = Form(...),
    checksum: str = Form(...),
    filesize: str = Form(...),
    image: UploadFile = File(...),
):
    if not device_id or not capture_id:
        raise HTTPException(status_code=400, detail='device_id and capture_id are required')

    destination = make_storage_path(device_id, capture_id)
    try:
        contents = await image.read()
        destination.write_bytes(contents)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'could not save file: {exc}')

    observed_checksum = compute_sha256(destination)
    if observed_checksum != checksum:
        destination.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail='checksum mismatch')

    server_ts = insert_capture_record(
        device_id=device_id,
        capture_id=capture_id,
        device_ts=device_timestamp,
        checksum=checksum,
        filepath=str(destination),
        filesize=int(filesize),
        status='stored',
    )

    return JSONResponse({
        'status': 'ok',
        'capture_id': capture_id,
        'server_timestamp': server_ts,
        'filepath': str(destination),
    })


@app.get('/health')
async def health():
    return {'status': 'ok', 'server_time': datetime.utcnow().isoformat() + 'Z'}


@app.get('/captures/{device_id}')
async def list_captures(device_id: str, limit: int = 50):
    cursor = DB.execute(
        'SELECT device_id, capture_id, device_timestamp, server_timestamp, checksum, filepath, filesize, status FROM captures WHERE device_id = ? ORDER BY created_at DESC LIMIT ?',
        (device_id, limit),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    return {'device_id': device_id, 'count': len(rows), 'captures': rows}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('server_app:app', host='0.0.0.0', port=8000, reload=False)
