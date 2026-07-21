# Monitoramento Agrícola - Servidor e Emissor

Este projeto contém dois componentes:

- `emitter/` - lógica de borda para Raspberry Pi, buffer local, MQTT e upload HTTP.
- `server/` - backend FastAPI para ingestão de imagens e listener MQTT.

## Como usar

### Emissor
1. Crie `emitter/.env` a partir de `emitter/.env.example`.
2. No Raspberry Pi, habilite a câmera e instale dependências de sistema:
   ```bash
   sudo apt update
   sudo raspi-config nonint do_camera 0
   sudo apt install -y python3-picamera libraspberrypi0 libraspberrypi-dev libraspberrypi-bin
   # Se estiver usando Raspberry Pi OS mais recente com `picamera2`:
   sudo apt install -y python3-picamera2 libcamera-apps
   ```
3. Verifique se a câmera está habilitada e funcionando no sistema.
   - Se aparecer erro `libbcm_host.so: cannot open shared object file`, confirme que você está rodando Raspberry Pi OS ou um sistema compatível com `libraspberrypi`.
4. Instale as dependências do Python:
   ```bash
   python -m pip install -r emitter/requirements.txt
   ```
5. Confirme que o broker MQTT está disponível e que `BROKER_HOST`/`BROKER_PORT` em `emitter/.env` apontam para ele.
   - Se o broker não estiver rodando, o emissor agora tentará reconectar automaticamente em vez de encerrar.
6. Execute o emissor a partir do diretório do projeto:
   ```bash
   python emitter.py
   ```

### Servidor
1. Crie `server/.env` a partir de `server/.env.example`.
2. Instale as dependências:
   ```powershell
   python -m pip install -r server/requirements.txt
   ```
3. Inicie o servidor:
   ```powershell
   python server\server_app.py
   ```
4. (Opcional) Inicie o listener MQTT para registrar mensagens de telemetria:
   ```powershell
   python server\mqtt_listener.py
   ```

## Observações

- `emitter/edge_emitter.py` usa MQTT para metadados, heartbeat e HTTP para upload de imagens.
- `server/server_app.py` armazena imagens em `server/object_store` e metadados em `server/server.db`.
- O emissor mantém um buffer local SQLite em `emitter/queue/queue.db`.
- `emitter.py` no root é um wrapper para rodar o emissor com o comando `python emitter.py`.
