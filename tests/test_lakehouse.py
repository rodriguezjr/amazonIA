"""
Pruebas unitarias para el motor analítico DuckDB Lakehouse (AmazonIALakehouse).
Valida las capas Medallion Bronze, Silver y Gold de forma directa.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Agregar raíz al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import pandas as pd
from src.database import init_database, insertar_telemetria
from src.lakehouse.manager import AmazonIALakehouse


@pytest.fixture
def lakehouse_fixture(tmp_path, monkeypatch):
    """Crea una base de datos temporal con 25 registros y un lakehouse limpio."""
    db_file = tmp_path / "lakehouse_test.db"
    lake_dir = tmp_path / "lakehouse_data"

    monkeypatch.setattr("src.config.DB_PATH", db_file)
    monkeypatch.setattr("src.database.DB_PATH", db_file)
    init_database()

    base_time = datetime(2026, 10, 20, 8, 0, 0)
    for i in range(25):
        estacion = "CARONI-001" if i < 15 else "CARONI-002"
        ts = (base_time + timedelta(minutes=i * 10)).isoformat()
        calidad = "valid" if i % 5 != 0 else "invalid"  # 20 válidos, 5 inválidos

        insertar_telemetria({
            "estacion_id": estacion,
            "timestamp": ts,
            "temperatura": 24.0 + (i * 0.4),
            "humedad": 65.0 + (i * 0.2),
            "presion": 1012.0,
            "precipitacion": 1.5 + (i * 0.1),
            "viento": 8.0 + (i * 0.3),
            "voltaje_bateria": 3.7 + (i * 0.01),
            "rssi": -70 - (i % 10),
            "estado_calidad": calidad,
            "errores": None if calidad == "valid" else "Valor no calibrado",
        })

    lh = AmazonIALakehouse(db_path=db_file, lakehouse_dir=lake_dir)
    return {"lakehouse": lh, "db_path": db_file, "lake_dir": lake_dir}


class TestLakehouseBronze:
    """Pruebas de la capa Bronze en AmazonIALakehouse."""

    def test_bronze_ingestion(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        res = lh.procesar_bronze()

        assert res["total_bronze"] == 25
        assert res["estaciones"] == 2
        assert lh.bronze_file.exists()

        # Verificar lectura Parquet con DuckDB
        df = lh.consultar_sql(f"SELECT * FROM read_parquet('{lh.bronze_file.as_posix()}')")
        assert len(df) == 25
        assert "ingesta_lakehouse_ts" in df.columns

    def test_bronze_db_inexistente(self, tmp_path):
        fake_db = tmp_path / "no_existe.db"
        lh = AmazonIALakehouse(db_path=fake_db, lakehouse_dir=tmp_path / "lh")
        res = lh.procesar_bronze()
        assert res["total_bronze"] == 0
        assert "error" in res


class TestLakehouseSilver:
    """Pruebas de la capa Silver en AmazonIALakehouse."""

    def test_silver_limpieza(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        res = lh.procesar_silver()

        # 25 registros en total, 5 inválidos -> 20 válidos
        assert res["total_silver"] == 20
        assert res["estaciones"] == 2
        assert lh.silver_file.exists()

        df = lh.consultar_sql(f"SELECT * FROM read_parquet('{lh.silver_file.as_posix()}')")
        assert len(df) == 20
        assert all(df["estado_calidad"] == "valid")

    def test_silver_normalizacion_termica(self, tmp_path, monkeypatch):
        db_file = tmp_path / "test_temp.db"
        monkeypatch.setattr("src.config.DB_PATH", db_file)
        monkeypatch.setattr("src.database.DB_PATH", db_file)
        init_database()

        insertar_telemetria({
            "estacion_id": "CARONI-001",
            "timestamp": "2026-10-20T10:00:00",
            "temperatura": -30.0,  # Extremadamente frío, fuera de rango
            "humedad": 50.0,
            "presion": 1013.0,
            "precipitacion": 0.0,
            "viento": 5.0,
            "voltaje_bateria": 3.8,
            "rssi": -70,
            "estado_calidad": "valid",
            "errores": None,
        })

        lh = AmazonIALakehouse(db_path=db_file, lakehouse_dir=tmp_path / "lh_temp")
        lh.procesar_silver()

        df = lh.consultar_sql(f"SELECT temperatura FROM read_parquet('{lh.silver_file.as_posix()}')")
        assert pd.isna(df.iloc[0]["temperatura"])

    def test_silver_deduplicacion(self, tmp_path, monkeypatch):
        db_file = tmp_path / "test_dedup.db"
        monkeypatch.setattr("src.config.DB_PATH", db_file)
        monkeypatch.setattr("src.database.DB_PATH", db_file)
        init_database()

        for t in [20.0, 22.0, 25.0]:
            insertar_telemetria({
                "estacion_id": "CARONI-001",
                "timestamp": "2026-10-20T12:00:00",  # Misma estación y timestamp
                "temperatura": t,
                "humedad": 50.0,
                "presion": 1013.0,
                "precipitacion": 0.0,
                "viento": 5.0,
                "voltaje_bateria": 3.8,
                "rssi": -70,
                "estado_calidad": "valid",
                "errores": None,
            })

        lh = AmazonIALakehouse(db_path=db_file, lakehouse_dir=tmp_path / "lh_dedup")
        lh.procesar_silver()

        df = lh.consultar_sql(f"SELECT * FROM read_parquet('{lh.silver_file.as_posix()}')")
        assert len(df) == 1  # Solo debe sobrevivir 1 por ROW_NUMBER()


class TestLakehouseGold:
    """Pruebas de la capa Gold en AmazonIALakehouse."""

    def test_gold_tres_productos(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        res = lh.procesar_gold()

        assert res["agregaciones_calculadas"] > 0
        assert res["dias_procesados"] > 0
        assert res["estaciones_kpi"] == 2

        # Comprobar que los tres archivos Parquet existan
        assert lh.gold_horarios_file.exists()
        assert lh.gold_diarios_file.exists()
        assert lh.gold_kpis_file.exists()

    def test_gold_kpis_calidad(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        lh.procesar_gold()

        kpis = lh.obtener_kpis_estaciones()
        assert len(kpis) == 2
        assert "porcentaje_calidad" in kpis.columns
        assert "total_recibidos" in kpis.columns

        # Suma total recibidos debe ser 25
        assert kpis["total_recibidos"].sum() == 25


class TestLakehousePipelineCompleto:
    """Pruebas de la ejecución extremo a extremo del Lakehouse."""

    def test_ejecutar_pipeline_completo(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        resultado = lh.ejecutar_pipeline_completo()

        assert "duracion_segundos" in resultado
        assert resultado["bronze"]["total_bronze"] == 25
        assert resultado["silver"]["total_silver"] == 20
        assert resultado["gold"]["agregaciones_calculadas"] > 0

        resumen = resultado["resumen"]
        assert resumen["bronze"]["total_archivos"] == 1
        assert resumen["silver"]["total_archivos"] == 1
        assert resumen["gold"]["total_archivos"] == 3

    def test_consultar_sql_personalizada(self, lakehouse_fixture):
        lh = lakehouse_fixture["lakehouse"]
        lh.ejecutar_pipeline_completo()

        # Consulta analítica cruzando datos
        query = f"""
            SELECT estacion_id, AVG(temp_promedio) as media_global
            FROM read_parquet('{lh.gold_horarios_file.as_posix()}')
            GROUP BY estacion_id
            ORDER BY estacion_id
        """
        df = lh.consultar_sql(query)
        assert len(df) == 2
        assert "media_global" in df.columns


class TestLakehouseVacio:
    """Prueba de comportamiento con base de datos vacía."""

    def test_db_vacia(self, tmp_path, monkeypatch):
        db_file = tmp_path / "vacia.db"
        monkeypatch.setattr("src.config.DB_PATH", db_file)
        monkeypatch.setattr("src.database.DB_PATH", db_file)
        init_database()

        lh = AmazonIALakehouse(db_path=db_file, lakehouse_dir=tmp_path / "lh_vacio")
        res = lh.ejecutar_pipeline_completo()

        assert res["bronze"]["total_bronze"] == 0
        assert res["silver"]["total_silver"] == 0
        assert res["gold"]["agregaciones_calculadas"] == 0
