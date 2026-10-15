# 🔌 API Reference — AmazonIA 4.0

## Tabla de Contenidos

1. [Introducción](#introducción)
2. [Endpoints](#endpoints)
3. [Modelos de Datos](#modelos-de-datos)
4. [Ejemplos](#ejemplos)

---

## Introducción

La API de AmazonIA 4.0 está construida con **FastAPI** y proporciona endpoints para la ingesta, validación y consulta de telemetría microclimática.

**URL Base:** `http://localhost:8000`

**Documentación Interactiva:** `http://localhost:8000/docs`

---

## Endpoints

### 1. Health Check

**GET** `/health`

Verifica el estado del sistema.

**Respuesta:**

{
  "status": "ok",
  "timestamp": "2026-09-15T10:30:00"
}
2. Ingesta Individual
POST /ingesta

Recibe un registro individual de telemetría.

Body:


{
  "estacion_id": "CARONI-001",
  "timestamp": "2026-09-15T10:30:00",
  "temperatura": 28.5,
  "humedad": 75.0,
  "presion": 1013.25,
  "precipitacion": 0.0,
  "viento": 5.2,
  "voltaje_bateria": 3.8,
  "rssi": -85
}
Respuesta:


{
  "id": 1,
  "estado_calidad": "valid",
  "errores": null,
  "alertas": []
}
3. Ingesta por Lotes
POST /ingesta/batch

Recibe un lote de registros para sincronización.

Body:


{
  "datos": [
    {"estacion_id": "CARONI-001", "timestamp": "...", ...},
    {"estacion_id": "CARONI-002", "timestamp": "...", ...}
  ]
}
Respuesta:


{
  "total": 2,
  "validos": 2,
  "invalidos": 0,
  "incompletos": 0
}
4. Últimas Lecturas
GET /telemetria/ultimas?estacion_id=CARONI-001&limite=10

Obtiene las últimas lecturas para el dashboard.

Parámetros:

estacion_id (opcional): Filtrar por estación.

limite (opcional, default=100): Número de lecturas.

5. Estadísticas de Calidad
GET /telemetria/estadisticas

Obtiene estadísticas de calidad de datos.

Respuesta:


{
  "valid": 150,
  "invalid": 10,
  "incomplete": 5
}
6. Listar Estaciones
GET /telemetria/estaciones

Obtiene la lista de estaciones registradas.

Respuesta:


["CARONI-001", "CARONI-002"]
Modelos de Datos
TelemetriaInput
Campo	Tipo	Requerido	Rango
estacion_id	string	Sí	—
timestamp	string (ISO)	Sí	—
temperatura	float	No	-10 a 45 °C
humedad	float	No	0 a 100 %
presion	float	No	800 a 1100 hPa
precipitacion	float	No	0 a 500 mm
viento	float	No	0 a 150 km/h
voltaje_bateria	float	No	3.0 a 4.2 V
rssi	int	No	-120 a 0 dBm
Ejemplos
Enviar una lectura con cURL
bash
curl -X POST http://localhost:8000/ingesta \
  -H "Content-Type: application/json" \
  -d '{
    "estacion_id": "CARONI-001",
    "timestamp": "2026-09-15T10:30:00",
    "temperatura": 28.5,
    "humedad": 75.0,
    "presion": 1013.25,
    "precipitacion": 0.0,
    "viento": 5.2,
    "voltaje_bateria": 3.8,
    "rssi": -85
  }'
Enviar una lectura con Python
python
import requests

payload = {
    "estacion_id": "CARONI-001",
    "timestamp": "2026-09-15T10:30:00",
    "temperatura": 28.5,
    "humedad": 75.0,
    "presion": 1013.25,
    "precipitacion": 0.0,
    "viento": 5.2,
    "voltaje_bateria": 3.8,
    "rssi": -85,
}

response = requests.post("http://localhost:8000/ingesta", json=payload)
print(response.json())
