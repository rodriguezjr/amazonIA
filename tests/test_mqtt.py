"""
Pruebas unitarias para los módulos MQTT (publicador y suscriptor/bridge).
"""
import sys
import json
from pathlib import Path
from unittest.mock import MagicMock

# Agregar la raíz del proyecto al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from src.database import init_database
from src.mqtt_publicador import publicar_telemetria, obtener_cliente_mqtt
from src.mqtt_suscriptor import procesar_mensaje_telemetria


@pytest.fixture(autouse=True)
def setup_database(tmp_path, monkeypatch):
    """Configura una base de datos temporal para cada prueba."""
    db_file = tmp_path / "test_mqtt.db"
    monkeypatch.setattr("src.config.DB_PATH", db_file)
    monkeypatch.setattr("src.database.DB_PATH", db_file)
    init_database()
    yield


class TestMQTTPublicador:
    """Pruebas para el módulo publicador MQTT."""

    def test_publicar_telemetria_exitoso(self):
        """Verifica que se serializa a JSON y se publica en el tópico correcto."""
        mock_cliente = MagicMock()
        mock_info = MagicMock()
        mock_info.is_published.return_value = True
        mock_cliente.publish.return_value = mock_info

        lectura = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-10-20T12:00:00",
            "temperatura": 25.5,
            "humedad": 70.0,
            "presion": 1012.0,
            "precipitacion": 0.0,
            "viento": 10.0,
            "voltaje_bateria": 3.9,
            "rssi": -75,
        }

        resultado = publicar_telemetria(mock_cliente, lectura)
        assert resultado is True

        # Verificar argumentos de publish
        mock_cliente.publish.assert_called_once()
        args, kwargs = mock_cliente.publish.call_args
        topic_llamado = args[0]
        assert topic_llamado == "amazonia/caroni/CARONI-001/telemetria"

        payload_llamado = json.loads(kwargs["payload"])
        assert payload_llamado["estacion_id"] == "CARONI-001"
        assert payload_llamado["temperatura"] == 25.5

    def test_obtener_cliente_mqtt(self):
        """Verifica inicialización del cliente MQTT."""
        cliente = obtener_cliente_mqtt("test-client")
        assert cliente is not None


class TestMQTTSuscriptor:
    """Pruebas para el módulo suscriptor y bridge MQTT."""

    def test_procesar_mensaje_valido(self):
        """Un mensaje válido debe validarse e insertarse en la base de datos."""
        payload = json.dumps({
            "estacion_id": "CARONI-001",
            "timestamp": "2026-10-20T12:00:00",
            "temperatura": 28.0,
            "humedad": 65.0,
            "presion": 1013.0,
            "precipitacion": 0.0,
            "viento": 5.0,
            "voltaje_bateria": 4.0,
            "rssi": -80,
        })

        exito, reg_id, calidad, alertas = procesar_mensaje_telemetria(payload)
        assert exito is True
        assert reg_id is not None
        assert calidad == "valid"
        assert len(alertas) == 0

    def test_procesar_mensaje_invalido_fuera_de_rango(self):
        """Un mensaje con datos fuera de rango debe marcarse como invalid pero persistirse."""
        payload = json.dumps({
            "estacion_id": "CARONI-002",
            "timestamp": "2026-10-20T12:05:00",
            "temperatura": 180.0,  # Físicamente imposible
            "humedad": 65.0,
            "presion": 1013.0,
            "precipitacion": 0.0,
            "viento": 5.0,
            "voltaje_bateria": 4.0,
            "rssi": -80,
        })

        exito, reg_id, calidad, alertas = procesar_mensaje_telemetria(payload)
        assert exito is True
        assert reg_id is not None
        assert calidad == "invalid"

    def test_procesar_mensaje_con_alerta_lluvia(self):
        """Un mensaje con precipitación > 50 mm debe generar alerta."""
        payload = json.dumps({
            "estacion_id": "CARONI-001",
            "timestamp": "2026-10-20T12:10:00",
            "temperatura": 24.0,
            "humedad": 95.0,
            "presion": 1005.0,
            "precipitacion": 65.0,  # Supera umbral de 50 mm
            "viento": 15.0,
            "voltaje_bateria": 3.8,
            "rssi": -85,
        })

        exito, reg_id, calidad, alertas = procesar_mensaje_telemetria(payload)
        assert exito is True
        assert calidad == "valid"
        assert len(alertas) >= 1
        assert any(a["tipo"] == "lluvia_intensa" for a in alertas)

    def test_procesar_mensaje_json_corrupto(self):
        """Un JSON mal formado no debe colapsar el sistema y debe retornar error_json."""
        payload = "{ esto no es json valido }"
        exito, reg_id, calidad, alertas = procesar_mensaje_telemetria(payload)
        assert exito is False
        assert reg_id is None
        assert calidad == "error_json"

    def test_procesar_mensaje_sin_estacion_id(self):
        """Un payload sin estacion_id debe ser rechazado."""
        payload = json.dumps({
            "timestamp": "2026-10-20T12:15:00",
            "temperatura": 25.0,
        })
        exito, reg_id, calidad, alertas = procesar_mensaje_telemetria(payload)
        assert exito is False
        assert reg_id is None
        assert calidad == "formato_invalido"
