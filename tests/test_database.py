"""
Pruebas unitarias para el módulo de persistencia en SQLite.
Verifica la inserción, consulta y actualización de registros.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from src.database import (
    init_database,
    insertar_telemetria,
    obtener_no_sincronizados,
    marcar_como_sincronizados,
    obtener_ultimas_lecturas,
    obtener_estadisticas_calidad,
    obtener_estaciones,
)


@pytest.fixture(autouse=True)
def setup_database(tmp_path, monkeypatch):
    """Inicializa una base de datos temporal aislada antes de cada prueba."""
    db_file = tmp_path / "test_telemetria.db"
    monkeypatch.setattr("src.config.DB_PATH", db_file)
    monkeypatch.setattr("src.database.DB_PATH", db_file)
    init_database()
    yield


def _registro_ejemplo(estacion_id="CARONI-TEST"):
    """Crea un registro de ejemplo para las pruebas."""
    return {
        "estacion_id": estacion_id,
        "timestamp": "2026-09-15T10:30:00",
        "temperatura": 28.5,
        "humedad": 75.0,
        "presion": 1013.25,
        "precipitacion": 0.0,
        "viento": 5.2,
        "voltaje_bateria": 3.8,
        "rssi": -85,
        "estado_calidad": "valid",
        "errores": None,
    }


class TestInsercion:
    """Pruebas para la inserción de registros."""

    def test_insertar_registro(self):
        """Un registro debe ser insertado correctamente."""
        registro = _registro_ejemplo()
        registro_id = insertar_telemetria(registro)
        assert registro_id is not None
        assert registro_id > 0

    def test_insertar_multiples_registros(self):
        """Múltiples registros deben ser insertados."""
        for i in range(5):
            registro = _registro_ejemplo(f"CARONI-{i:03d}")
            registro_id = insertar_telemetria(registro)
            assert registro_id > 0


class TestConsultas:
    """Pruebas para las consultas a la base de datos."""

    def test_obtener_no_sincronizados(self):
        """Los registros nuevos deben estar pendientes de sincronización."""
        registro = _registro_ejemplo()
        insertar_telemetria(registro)
        pendientes = obtener_no_sincronizados(limite=10)
        assert len(pendientes) >= 1

    def test_marcar_como_sincronizados(self):
        """Los registros deben ser marcados como sincronizados."""
        registro = _registro_ejemplo()
        registro_id = insertar_telemetria(registro)
        marcar_como_sincronizados([registro_id])
        pendientes = obtener_no_sincronizados(limite=10)
        ids_pendientes = [p["id"] for p in pendientes]
        assert registro_id not in ids_pendientes

    def test_obtener_ultimas_lecturas(self):
        """Debe retornar las últimas lecturas."""
        registro = _registro_ejemplo()
        insertar_telemetria(registro)
        lecturas = obtener_ultimas_lecturas(limite=10)
        assert len(lecturas) >= 1

    def test_obtener_estadisticas_calidad(self):
        """Debe retornar las estadísticas de calidad."""
        registro = _registro_ejemplo()
        insertar_telemetria(registro)
        estadisticas = obtener_estadisticas_calidad()
        assert isinstance(estadisticas, dict)
        assert "valid" in estadisticas

    def test_obtener_estaciones(self):
        """Debe retornar la lista de estaciones."""
        registro = _registro_ejemplo("CARONI-TEST-UNICA")
        insertar_telemetria(registro)
        estaciones = obtener_estaciones()
        assert "CARONI-TEST-UNICA" in estaciones

    def test_obtener_lecturas_filtradas(self):
        """Debe retornar lecturas filtradas por fecha_inicio y estación."""
        from src.database import obtener_lecturas_filtradas

        reg1 = _registro_ejemplo("CARONI-TEST-FILTRO")
        reg1["timestamp"] = "2026-10-01T10:00:00"
        insertar_telemetria(reg1)

        reg2 = _registro_ejemplo("CARONI-TEST-FILTRO")
        reg2["timestamp"] = "2026-10-15T10:00:00"
        insertar_telemetria(reg2)

        # Filtrar solo después del 2026-10-10
        filtrados = obtener_lecturas_filtradas(
            estacion_id="CARONI-TEST-FILTRO",
            fecha_inicio="2026-10-10T00:00:00",
        )
        assert len(filtrados) == 1
        assert filtrados[0]["timestamp"] == "2026-10-15T10:00:00"
