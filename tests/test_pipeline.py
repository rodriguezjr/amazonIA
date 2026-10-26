"""
Pruebas unitarias para el pipeline de datos Medallion (Bronze → Silver → Gold) con DuckDB.
Valida las transformaciones analíticas sin requerir Airflow ni Docker.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Agregar raíz al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import pandas as pd
from src.database import init_database, insertar_telemetria


@pytest.fixture(autouse=True)
def setup_pipeline_test(tmp_path, monkeypatch):
    """
    Configura una base de datos temporal con registros de prueba
    y directorios lakehouse temporales para cada test.
    """
    # Base de datos temporal
    db_file = tmp_path / "test_pipeline.db"
    monkeypatch.setattr("src.config.DB_PATH", db_file)
    monkeypatch.setattr("src.database.DB_PATH", db_file)
    init_database()

    # Insertar registros variados para las pruebas
    base_time = datetime.now() - timedelta(hours=2)
    estaciones = ["CARONI-001", "CARONI-002"]
    for i in range(20):
        estacion = estaciones[i % 2]
        ts = (base_time + timedelta(minutes=i * 5)).isoformat()
        calidad = "valid" if i % 5 != 0 else "invalid"  # 16 válidos, 4 inválidos

        insertar_telemetria({
            "estacion_id": estacion,
            "timestamp": ts,
            "temperatura": 25.0 + (i * 0.5),
            "humedad": 70.0 + (i * 0.3),
            "presion": 1013.0,
            "precipitacion": 2.0 + (i * 0.1),
            "viento": 10.0 + (i * 0.5),
            "voltaje_bateria": 3.8,
            "rssi": -75,
            "estado_calidad": calidad,
            "errores": None if calidad == "valid" else "Temperatura fuera de rango",
        })

    # Directorio temporal para Lakehouse
    lakehouse_dir = tmp_path / "lakehouse"

    # Parchear las rutas en los scripts del pipeline
    monkeypatch.setattr("dags.scripts.bronze_to_silver._DB_PATH", str(db_file))
    monkeypatch.setattr("dags.scripts.bronze_to_silver._LAKEHOUSE_DIR", lakehouse_dir)
    monkeypatch.setattr("dags.scripts.silver_to_gold._DB_PATH", str(db_file))
    monkeypatch.setattr("dags.scripts.silver_to_gold._LAKEHOUSE_DIR", lakehouse_dir)

    yield {
        "db_file": db_file,
        "lakehouse_dir": lakehouse_dir,
        "silver_file": lakehouse_dir / "silver" / "telemetria_limpia.parquet",
        "gold_file": lakehouse_dir / "gold" / "indicadores_horarios.parquet",
    }


class TestBronzeToSilver:
    """Pruebas de la transformación Bronze → Silver con DuckDB."""

    def test_procesar_bronze_silver(self, setup_pipeline_test):
        """Verifica que se filtren datos inválidos y se genere Parquet Silver."""
        from dags.scripts.bronze_to_silver import main

        resultado = main()

        assert resultado["total_bronze"] == 20
        assert resultado["total_silver"] == 16  # 16 válidos
        assert resultado["estaciones"] == 2

        # Verificar que el archivo Parquet fue creado
        silver_file = setup_pipeline_test["silver_file"]
        assert silver_file.exists()

        # Verificar contenido del Parquet
        df = pd.read_parquet(silver_file)
        assert len(df) == 16
        assert "timestamp_procesado" in df.columns
        assert all(df["estado_calidad"] == "valid")

    def test_normaliza_temperaturas_fuera_de_rango(self, tmp_path, monkeypatch):
        """Verifica que temperaturas fuera de rango se normalicen a NULL/NaN."""
        db_file = tmp_path / "test_extreme.db"
        monkeypatch.setattr("src.config.DB_PATH", db_file)
        monkeypatch.setattr("src.database.DB_PATH", db_file)
        init_database()

        insertar_telemetria({
            "estacion_id": "TEST-001",
            "timestamp": "2026-01-01T12:00:00",
            "temperatura": 150.0,  # Fuera de rango (-10 a 60 °C)
            "humedad": 50.0,
            "presion": 1013.0,
            "precipitacion": 0.0,
            "viento": 5.0,
            "voltaje_bateria": 3.8,
            "rssi": -70,
            "estado_calidad": "valid",
            "errores": None,
        })

        lakehouse_dir = tmp_path / "lakehouse_extreme"
        monkeypatch.setattr("dags.scripts.bronze_to_silver._DB_PATH", str(db_file))
        monkeypatch.setattr("dags.scripts.bronze_to_silver._LAKEHOUSE_DIR", lakehouse_dir)

        from dags.scripts.bronze_to_silver import main
        main()

        df = pd.read_parquet(lakehouse_dir / "silver" / "telemetria_limpia.parquet")
        assert pd.isna(df.iloc[0]["temperatura"])  # Debe ser NULL / NaN

    def test_elimina_duplicados(self, tmp_path, monkeypatch):
        """Verifica que se eliminen registros duplicados por (estacion_id, timestamp)."""
        db_file = tmp_path / "test_dup.db"
        monkeypatch.setattr("src.config.DB_PATH", db_file)
        monkeypatch.setattr("src.database.DB_PATH", db_file)
        init_database()

        # Insertar 2 registros con mismo estacion_id + timestamp
        for temp in [25.0, 26.0]:
            insertar_telemetria({
                "estacion_id": "DUP-001",
                "timestamp": "2026-01-01T12:00:00",
                "temperatura": temp,
                "humedad": 50.0,
                "presion": 1013.0,
                "precipitacion": 0.0,
                "viento": 5.0,
                "voltaje_bateria": 3.8,
                "rssi": -70,
                "estado_calidad": "valid",
                "errores": None,
            })

        lakehouse_dir = tmp_path / "lakehouse_dup"
        monkeypatch.setattr("dags.scripts.bronze_to_silver._DB_PATH", str(db_file))
        monkeypatch.setattr("dags.scripts.bronze_to_silver._LAKEHOUSE_DIR", lakehouse_dir)

        from dags.scripts.bronze_to_silver import main
        resultado = main()

        assert resultado["total_bronze"] == 2
        assert resultado["total_silver"] == 1
        df = pd.read_parquet(lakehouse_dir / "silver" / "telemetria_limpia.parquet")
        assert len(df) == 1


class TestSilverToGold:
    """Pruebas de la transformación Silver → Gold con DuckDB."""

    def test_procesar_silver_gold(self, setup_pipeline_test):
        """Verifica que las agregaciones horarias y métricas se calculen correctamente."""
        from dags.scripts.bronze_to_silver import main as bronze_main
        from dags.scripts.silver_to_gold import main as gold_main

        bronze_main()
        resultado = gold_main()

        assert resultado["agregaciones_calculadas"] > 0
        assert resultado["estaciones_procesadas"] == 2  # CARONI-001 y CARONI-002

        # Verificar archivo Gold
        gold_file = setup_pipeline_test["gold_file"]
        assert gold_file.exists()

        df = pd.read_parquet(gold_file)
        assert "temp_promedio" in df.columns
        assert "precipitacion_acumulada" in df.columns
        assert "total_lecturas" in df.columns

    def test_gold_sin_db_retorna_error(self, tmp_path, monkeypatch):
        """Verifica que Gold maneja el caso de BD inexistente."""
        fake_db = tmp_path / "inexistente.db"
        lakehouse_dir = tmp_path / "lakehouse_vacio"
        monkeypatch.setattr("dags.scripts.silver_to_gold._DB_PATH", str(fake_db))
        monkeypatch.setattr("dags.scripts.silver_to_gold._LAKEHOUSE_DIR", lakehouse_dir)

        from dags.scripts.silver_to_gold import main as gold_main
        resultado = gold_main()
        assert resultado["agregaciones_calculadas"] == 0


class TestRAGIndexer:
    """Pruebas del indexador RAG dentro del pipeline."""

    def test_indexar_documentos_pipeline(self):
        """Verifica que el indexador RAG funcione correctamente."""
        from dags.scripts.rag_indexer import main as rag_main

        resultado = rag_main()
        assert resultado["estado"] == "exitoso"
        assert resultado["documentos_indexados"] > 0


class TestPipelineIntegracion:
    """Prueba de integración del pipeline completo Bronze → Silver → Gold."""

    def test_pipeline_completo(self, setup_pipeline_test):
        """Ejecuta el pipeline completo y verifica la consistencia de extremo a extremo."""
        from dags.scripts.bronze_to_silver import main as bronze_main
        from dags.scripts.silver_to_gold import main as gold_main

        # Paso 1: Bronze → Silver
        res_silver = bronze_main()
        assert res_silver["total_silver"] > 0

        # Paso 2: Silver → Gold
        res_gold = gold_main()
        assert res_gold["agregaciones_calculadas"] > 0

        # Verificar consistencia: los registros Gold deben cubrir las estaciones Silver
        silver_file = setup_pipeline_test["silver_file"]
        gold_file = setup_pipeline_test["gold_file"]

        df_silver = pd.read_parquet(silver_file)
        df_gold = pd.read_parquet(gold_file)

        estaciones_silver = set(df_silver["estacion_id"].unique())
        estaciones_gold = set(df_gold["estacion_id"].unique())
        assert estaciones_gold == estaciones_silver

        # Las lecturas totales en Gold deben sumar exactamente la cantidad en Silver
        total_lecturas_gold = df_gold["total_lecturas"].sum()
        assert total_lecturas_gold == len(df_silver)
