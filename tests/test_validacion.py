"""
Pruebas unitarias para el módulo de validación de calidad.
Valida el comportamiento del motor de reglas de Buelvas et al. (2023).
"""
import sys
from pathlib import Path

# Agregar la raíz del proyecto al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from src.validacion import validar_telemetria, verificar_alertas


class TestValidacionTelemetria:
    """Pruebas para la función validar_telemetria."""

    def test_datos_validos(self):
        """Datos dentro de rangos deben ser marcados como 'valid'."""
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

    def test_temperatura_fuera_rango_alto(self):
        """Temperatura > 45°C debe ser marcada como 'invalid'."""
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

    def test_temperatura_fuera_rango_bajo(self):
        """Temperatura < -10°C debe ser marcada como 'invalid'."""
        datos = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-09-15T10:30:00",
            "temperatura": -50.0,
            "humedad": 75.0,
            "presion": 1013.25,
            "precipitacion": 0.0,
            "viento": 5.2,
            "voltaje_bateria": 3.8,
            "rssi": -85,
        }
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "invalid"

    def test_campo_ausente(self):
        """Un campo ausente debe ser marcado como 'incomplete'."""
        datos = {
            "estacion_id": "CARONI-001",
            "timestamp": "2026-09-15T10:30:00",
            "temperatura": 28.5,
            "presion": 1013.25,
            "precipitacion": 0.0,
            "viento": 5.2,
            "voltaje_bateria": 3.8,
            "rssi": -85,
            # Falta "humedad"
        }
        resultado = validar_telemetria(datos)
        assert resultado["estado"] == "incomplete"
        assert "humedad" in resultado["errores"]

    def test_datos_vacios(self):
        """Un payload vacío debe ser marcado como 'invalid'."""
        resultado = validar_telemetria({})
        assert resultado["estado"] == "invalid"

    def test_tipo_invalido(self):
        """Un tipo incorrecto debe ser marcado como 'invalid'."""
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
    """Pruebas para la función verificar_alertas."""

    def test_alerta_lluvia_intensa(self):
        """Precipitación > 50mm debe activar alerta."""
        datos = {
            "precipitacion": 60.0,
            "temperatura": 25.0,
            "voltaje_bateria": 3.8,
            "viento": 10.0,
        }
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "lluvia_intensa" for a in alertas)

    def test_alerta_calor_extremo(self):
        """Temperatura > 40°C debe activar alerta."""
        datos = {
            "precipitacion": 0.0,
            "temperatura": 42.0,
            "voltaje_bateria": 3.8,
            "viento": 10.0,
        }
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "calor_extremo" for a in alertas)

    def test_alerta_bateria_baja(self):
        """Voltaje < 3.2V debe activar alerta."""
        datos = {
            "precipitacion": 0.0,
            "temperatura": 25.0,
            "voltaje_bateria": 2.8,
            "viento": 10.0,
        }
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "bateria_baja" for a in alertas)

    def test_alerta_viento_fuerte(self):
        """Viento > 80 km/h debe activar alerta."""
        datos = {
            "precipitacion": 0.0,
            "temperatura": 25.0,
            "voltaje_bateria": 3.8,
            "viento": 95.0,
        }
        alertas = verificar_alertas(datos)
        assert any(a["tipo"] == "viento_fuerte" for a in alertas)

    def test_sin_alertas(self):
        """Datos normales no deben activar alertas."""
        datos = {
            "precipitacion": 5.0,
            "temperatura": 28.0,
            "voltaje_bateria": 3.8,
            "viento": 10.0,
        }
        alertas = verificar_alertas(datos)
        assert len(alertas) == 0