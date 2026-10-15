"""
Pruebas unitarias para la API de ingesta con FastAPI.
Utiliza TestClient para simular peticiones HTTP.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi.testclient import TestClient

from src.api import app
from src.database import init_database


@pytest.fixture(autouse=True)
def setup_database():
    """Inicializa la base de datos antes de cada prueba."""
    init_database()
    yield


@pytest.fixture
def client():
    """Cliente de pruebas para la API."""
    return TestClient(app)


class TestAPIHealth:
    """Pruebas para el endpoint de salud."""

    def test_health_check(self, client):
        """El endpoint /health debe responder con status ok."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestAPIIngesta:
    """Pruebas para el endpoint de ingesta individual."""

    def test_ingesta_datos_validos(self, client):
        """Datos válidos deben ser aceptados."""
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
        response = client.post("/ingesta", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["estado_calidad"] == "valid"
        assert "id" in data

    def test_ingesta_datos_invalidos(self, client):
        """Datos fuera de rango deben ser marcados como invalid."""
        payload = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-09-15T10:30:00",
            "temperatura": 150.0,
            "humedad": 75.0,
            "presion": 1013.25,
            "precipitacion": 0.0,
            "viento": 5.2,
            "voltaje_bateria": 3.8,
            "rssi": -85,
        }
        response = client.post("/ingesta", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["estado_calidad"] == "invalid"


class TestAPIBatch:
    """Pruebas para el endpoint de ingesta por lotes."""

    def test_ingesta_batch(self, client):
        """Un lote de datos debe ser procesado correctamente."""
        payload = {
            "datos": [
                {
                    "estacion_id": "CARONI-001",
                    "timestamp": "2026-09-15T10:30:00",
                    "temperatura": 28.5,
                    "humedad": 75.0,
                    "presion": 1013.25,
                    "precipitacion": 0.0,
                    "viento": 5.2,
                    "voltaje_bateria": 3.8,
                    "rssi": -85,
                },
                {
                    "estacion_id": "CARONI-002",
                    "timestamp": "2026-09-15T10:30:00",
                    "temperatura": 29.0,
                    "humedad": 70.0,
                    "presion": 1012.0,
                    "precipitacion": 0.5,
                    "viento": 6.0,
                    "voltaje_bateria": 3.9,
                    "rssi": -80,
                },
            ]
        }
        response = client.post("/ingesta/batch", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert data["validos"] == 2


class TestAPIConsultas:
    """Pruebas para los endpoints de consulta."""

    def test_ultimas_lecturas(self, client):
        """El endpoint de últimas lecturas debe responder."""
        response = client.get("/telemetria/ultimas?limite=10")
        assert response.status_code == 200

    def test_estadisticas_calidad(self, client):
        """El endpoint de estadísticas debe responder."""
        response = client.get("/telemetria/estadisticas")
        assert response.status_code == 200

    def test_listar_estaciones(self, client):
        """El endpoint de estaciones debe responder."""
        response = client.get("/telemetria/estaciones")
        assert response.status_code == 200