# 🚀 API Test Guide

## Avvio Server

```bash
./etl -web
```

Output atteso:
```
Starting server on :8080
API endpoints available at: http://localhost:8080/api/v1/
WebSocket endpoint: ws://localhost:8080/ws
Health check: http://localhost:8080/api/v1/health
```

## Test Endpoints (aprire un nuovo terminale)

### 1. Health Check
```bash
curl http://localhost:8080/api/v1/health
```
**Output atteso:**
```json
{"success":true,"data":{"service":"go-etl-api","status":"ok"}}
```

### 2. Lista Pipeline (inizialmente vuota)
```bash
curl http://localhost:8080/api/v1/pipelines
```
**Output atteso:**
```json
{"success":true,"data":[],"total":0,"offset":0,"limit":20}
```

### 3. Crea Pipeline
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-pipeline",
    "description": "My first test pipeline",
    "config_yaml": "steps:\n  - name: hello\n    type: stdout\n    config:\n      value: \"Hello World!\""
  }'
```
**Output atteso:**
```json
{"success":true,"data":{"id":1,"name":"test-pipeline","description":"My first test pipeline",...},"message":"Pipeline created successfully"}
```

### 4. Lista Pipeline (ora con 1 pipeline)
```bash
curl http://localhost:8080/api/v1/pipelines
```
**Output atteso:**
```json
{"success":true,"data":[{"id":1,"name":"test-pipeline",...}],"total":1,"offset":0,"limit":20}
```

### 5. Dettagli Pipeline
```bash
curl http://localhost:8080/api/v1/pipelines/1
```

### 6. Stato Pipeline
```bash
curl http://localhost:8080/api/v1/pipelines/1/status
```
**Output atteso:**
```json
{"success":true,"data":{"pipeline_id":1,"status":"CREATED","is_running":false}}
```

### 7. Statistiche Pipeline
```bash
curl http://localhost:8080/api/v1/pipelines/1/stats
```

### 8. Lista Esecuzioni (vuota)
```bash
curl http://localhost:8080/api/v1/executions
```

### 9. Aggiorna Pipeline
```bash
curl -X PUT http://localhost:8080/api/v1/pipelines/1 \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Updated description"
  }'
```

### 10. 🚀 Avvia Pipeline (NUOVO!)
```bash
curl -X POST http://localhost:8080/api/v1/pipelines/1/start
```
**Output atteso:**
```json
{"success":true,"data":{"pipeline_id":1,"execution":{"id":1,...},"message":"Pipeline started successfully"}}
```

### 11. 🏃 Verifica Pipeline in Esecuzione (NUOVO!)
```bash
curl http://localhost:8080/api/v1/pipelines/running
```
**Output atteso:**
```json
{"success":true,"data":[{"pipeline_id":1,"pipeline_name":"test-pipeline","execution_id":1,"status":"RUNNING","started_at":"...","duration_ms":1234}]}
```

### 12. ⏸️ Metti in Pausa Pipeline (NUOVO!)
```bash
curl -X POST http://localhost:8080/api/v1/pipelines/1/pause
```

### 13. ▶️ Riprendi Pipeline (NUOVO!)
```bash
curl -X POST http://localhost:8080/api/v1/pipelines/1/resume
```

### 14. ⏹️ Ferma Pipeline (NUOVO!)
```bash
curl -X POST http://localhost:8080/api/v1/pipelines/1/stop
```

### 15. 📊 Stato Pipeline Aggiornato
```bash
curl http://localhost:8080/api/v1/pipelines/1/status
```
**Output possibile:**
```json
{"success":true,"data":{"pipeline_id":1,"status":"COMPLETED","is_running":false}}
```

### 16. 🗑️ Elimina Pipeline
```bash
curl -X DELETE http://localhost:8080/api/v1/pipelines/1
```

### 17. 🔌 WebSocket Test
Nel browser, aprire la console sviluppatore e testare:
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
ws.onmessage = (event) => {
  console.log('Received:', JSON.parse(event.data));
};
ws.onopen = () => {
  console.log('WebSocket connected');
};
```

### 12. Dashboard Web
Aprire nel browser: http://localhost:8080/

## Test con Filtri e Paginazione

### Lista con filtri:
```bash
# Pipeline abilitate
curl "http://localhost:8080/api/v1/pipelines?enabled=true"

# Pipeline per stato
curl "http://localhost:8080/api/v1/pipelines?state=CREATED"

# Paginazione
curl "http://localhost:8080/api/v1/pipelines?offset=0&limit=5"

# Ordinamento
curl "http://localhost:8080/api/v1/pipelines?order_by=created_at&order_dir=desc"
```

### Esecuzioni con filtri:
```bash
# Esecuzioni per pipeline specifica
curl "http://localhost:8080/api/v1/executions?pipeline_id=1"

# Esecuzioni completate
curl "http://localhost:8080/api/v1/executions?status=COMPLETED"
```

## Test di Errore

### Pipeline non esistente:
```bash
curl http://localhost:8080/api/v1/pipelines/999
```
**Output atteso:**
```json
{"success":false,"error":"Pipeline not found"}
```

### JSON malformato:
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{"name": invalid json}'
```
**Output atteso:**
```json
{"success":false,"error":"Invalid JSON payload"}
```

### Campo obbligatorio mancante:
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{"description": "Missing name"}'
```
**Output atteso:**
```json
{"success":false,"error":"Name is required"}
```

## Note
- Il server si avvia sempre con alcuni template predefiniti nel database
- Il database viene creato automaticamente in `./data/pipelines.db`
- I log del server mostrano tutte le operazioni
- WebSocket invia eventi real-time per operazioni CRUD

## Comandi Utili per Debug
```bash
# Lista file database creato
ls -la data/

# Controllo processi server
ps aux | grep etl

# Kill server se rimane in background
pkill etl
```