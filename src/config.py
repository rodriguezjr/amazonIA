"""
Configuración central del sistema AmazonIA 4.0.
Define rangos de validación, umbrales de alerta y rutas de almacenamiento.
"""
import os
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "telemetria.db"

# Configuración de la API
API_HOST = os.getenv("API_HOST", "localhost")
API_PORT = int(os.getenv("API_PORT", 8000))

# Configuración de sincronización
SYNC_ENDPOINT = os.getenv("SYNC_ENDPOINT", "http://localhost:8000/ingesta/batch")
SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", 100))
SYNC_MAX_RETRIES = int(os.getenv("SYNC_MAX_RETRIES", 3))

# Configuración MQTT (Mosquitto)
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "amazonia/caroni")
MQTT_CLIENT_ID_PUB = os.getenv("MQTT_CLIENT_ID_PUB", "amazonia-publicador")
MQTT_CLIENT_ID_SUB = os.getenv("MQTT_CLIENT_ID_SUB", "amazonia-bridge")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", 60))
MQTT_QOS = int(os.getenv("MQTT_QOS", 1))

# Rangos de validación para la cuenca del río Caroní
RANGOS_VALIDACION = {
    "temperatura": {"min": -10.0, "max": 45.0, "unidad": "°C"},
    "humedad": {"min": 0.0, "max": 100.0, "unidad": "%"},
    "presion": {"min": 800.0, "max": 1100.0, "unidad": "hPa"},
    "precipitacion": {"min": 0.0, "max": 500.0, "unidad": "mm"},
    "viento": {"min": 0.0, "max": 150.0, "unidad": "km/h"},
    "voltaje_bateria": {"min": 3.0, "max": 4.2, "unidad": "V"},
    "rssi": {"min": -120, "max": 0, "unidad": "dBm"},
}

# Umbrales de alerta
UMBRALES_ALERTA = {
    "precipitacion_intensa": 50.0,   # mm
    "calor_extremo": 40.0,           # °C
    "bateria_baja": 3.2,             # V
    "viento_fuerte": 80.0,           # km/h
}

# Estaciones piloto
ESTACIONES_PILOTO = ["CARONI-001", "CARONI-002"]