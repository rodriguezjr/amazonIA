"""Pruebas unitarias para el módulo de validación."""
import pytest
from src.validacion import validar_telemetria, verificar_alertas


class TestValidacion:
    def test_datos_validos(self):
        datos = {
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
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "valid"
        assert resultado["errores"] is None

    def test_temperatura_fuera_rango(self):
        datos = {
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
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "invalid"
        assert "temperatura" in resultado["errores"]

    def test_campo_ausente(self):
        datos = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-09-15T10:30:00",
            "temperatura": 28.5,
            "presion": 1013.25,
            "precipitacion": 0.0,
            "viento": 5.2,
            "voltaje_bateria": 3.8,
            "rssi": -85,
        }
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "incomplete"
        assert "humedad" in resultado["errores"]

    def test_datos_vacios(self):
        resultado = validar_telemetria({})
        assert resultado["estado"] == "invalid"

    def test_tipo_invalido(self):
        datos = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-09-15T10:30:00",
            "temperatura": "no es un numero",
            "humedad": 75.0,
            "presion": 1013.25,
            "precipitacion": 0.0,
            "viento": 5.2,
            "voltaje_bateria": 3.8,
            "rssi": -85,
        }
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "invalid"


class TestAlertas:
    def test_alerta_lluvia_intensa(self):
        datos = {"precipitacion": 60.0, "temperatura": 25.0, "voltaje_bateria": 3.8, "viento": 10.0}
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "lluvia_intensa" for a in alertas)

    def test_alerta_bateria_baja(self):
        datos = {"precipitacion": 0.0, "temperatura": 25.0, "voltaje_bateria": 2.8, "viento": 10.0}
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "bateria_baja" for a in alertas)

    def test_sin_alertas(self):
        datos = {"precipitacion": 5.0, "temperatura": 28.0, "voltaje_bateria": 3.8, "viento": 10.0}
        alertas = verificar_alertas(datos)
        assert len(alertas) == 0