"""
Gestor del Lakehouse Medallion (Bronze / Silver / Gold) con DuckDB y Parquet.
AmazonIA 4.0 — Cuenca del Río Caroní.

Implementa una arquitectura columnar embebida y Offline-First:
  - Bronze: Datos brutos inmutables ingeridos desde SQLite (telemetria_raw.parquet).
  - Silver: Datos depurados, deduplicados y validados termodinámicamente (telemetria_limpia.parquet).
  - Gold: Datamarts analíticos con agregaciones horarias, diarias y KPIs de calidad.
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

# Forzar UTF-8 en stdout si es Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import duckdb
import pandas as pd

# Rutas del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
DB_PATH = DATA_DIR / "telemetria.db"

BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"

BRONZE_FILE = BRONZE_DIR / "telemetria_raw.parquet"
SILVER_FILE = SILVER_DIR / "telemetria_limpia.parquet"
GOLD_HORARIOS_FILE = GOLD_DIR / "indicadores_horarios.parquet"
GOLD_DIARIOS_FILE = GOLD_DIR / "indicadores_diarios.parquet"
GOLD_KPIS_FILE = GOLD_DIR / "kpis_calidad_estaciones.parquet"

logger = logging.getLogger("amazonia.lakehouse")


class AmazonIALakehouse:
    """
    Motor analítico OLAP sobre DuckDB y Apache Parquet.
    Ejecuta el ciclo de vida completo de datos Medallion.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        lakehouse_dir: Optional[Path] = None,
        memoria: bool = False,
    ):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.lakehouse_dir = Path(lakehouse_dir) if lakehouse_dir else LAKEHOUSE_DIR
        self.bronze_dir = self.lakehouse_dir / "bronze"
        self.silver_dir = self.lakehouse_dir / "silver"
        self.gold_dir = self.lakehouse_dir / "gold"

        self.bronze_file = self.bronze_dir / "telemetria_raw.parquet"
        self.silver_file = self.silver_dir / "telemetria_limpia.parquet"
        self.gold_horarios_file = self.gold_dir / "indicadores_horarios.parquet"
        self.gold_diarios_file = self.gold_dir / "indicadores_diarios.parquet"
        self.gold_kpis_file = self.gold_dir / "kpis_calidad_estaciones.parquet"

        self._asegurar_directorios()
        self.con = duckdb.connect(":memory:")

    def _asegurar_directorios(self):
        """Crea las carpetas de las capas Medallion si no existen."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    # ============================================================
    # CAPA 1: BRONZE (Ingesta Inmutable de Telemetría Bruta)
    # ============================================================
    def procesar_bronze(self) -> Dict[str, Any]:
        """
        Extrae todos los registros brutos desde la tabla 'telemetria' de SQLite
        y los persiste en formato Parquet comprimido con Snappy/ZSTD.
        Preserva el registro original sin alteraciones para auditoría.
        """
        logger.info("═══ [Lakehouse] Ejecutando Capa Bronze (SQLite → Parquet Raw) ═══")

        if not self.db_path.exists():
            logger.warning("Base de datos SQLite no encontrada en %s", self.db_path)
            return {"total_bronze": 0, "error": "db_no_encontrada"}

        # Leer datos desde SQLite
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        try:
            df_sqlite = pd.read_sql("SELECT * FROM telemetria", conn)
        finally:
            conn.close()

        total = len(df_sqlite)
        if total == 0:
            logger.warning("No hay registros en SQLite para la capa Bronze.")
            return {"total_bronze": 0, "archivo": str(self.bronze_file)}

        # Registrar timestamp de ingesta lakehouse
        df_sqlite["ingesta_lakehouse_ts"] = datetime.now().isoformat()

        # Usar DuckDB para guardar con compresión Parquet óptima
        self.con.register("df_bronze_temp", df_sqlite)
        query = f"""
            COPY (SELECT * FROM df_bronze_temp)
            TO '{self.bronze_file.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        self.con.execute(query)
        self.con.unregister("df_bronze_temp")

        resultado = {
            "total_bronze": total,
            "archivo": str(self.bronze_file),
            "estaciones": df_sqlite["estacion_id"].nunique() if "estacion_id" in df_sqlite.columns else 0,
            "tamano_kb": round(self.bronze_file.stat().st_size / 1024, 2) if self.bronze_file.exists() else 0,
        }
        logger.info("Bronze completado: %s", resultado)
        return resultado

    # ============================================================
    # CAPA 2: SILVER (Limpieza, Deduplicación y Calidad con DuckDB)
    # ============================================================
    def procesar_silver(self) -> Dict[str, Any]:
        """
        Transforma los datos de Bronze a Silver usando DuckDB SQL:
          1. Filtra registros válidos (estado_calidad = 'valid').
          2. Normaliza anomalías térmicas (-10°C a 60°C).
          3. Deduplica registros por (estacion_id, timestamp) usando ROW_NUMBER().
          4. Guarda el resultado en telemetria_limpia.parquet.
        """
        logger.info("═══ [Lakehouse] Ejecutando Capa Silver (DuckDB SQL Depuración) ═══")

        # Si Bronze no existe, ejecutarlo primero
        if not self.bronze_file.exists():
            res_bronze = self.procesar_bronze()
            if res_bronze.get("total_bronze", 0) == 0:
                return {"total_silver": 0, "error": "bronze_vacio"}

        # Consulta analítica de limpieza con DuckDB SQL nativo
        query_silver = f"""
            WITH base AS (
                SELECT
                    id,
                    estacion_id,
                    CAST(timestamp AS VARCHAR) AS timestamp,
                    CASE 
                        WHEN temperatura < -10.0 OR temperatura > 60.0 THEN NULL 
                        ELSE temperatura 
                    END AS temperatura,
                    humedad,
                    presion,
                    precipitacion,
                    viento,
                    voltaje_bateria,
                    rssi,
                    estado_calidad,
                    errores,
                    sincronizado,
                    fecha_recepcion,
                    ROW_NUMBER() OVER (
                        PARTITION BY estacion_id, timestamp 
                        ORDER BY id DESC
                    ) AS fila_orden
                FROM read_parquet('{self.bronze_file.as_posix()}')
                WHERE estado_calidad = 'valid'
            )
            SELECT 
                id,
                estacion_id,
                timestamp,
                temperatura,
                humedad,
                presion,
                precipitacion,
                viento,
                voltaje_bateria,
                rssi,
                estado_calidad,
                errores,
                sincronizado,
                fecha_recepcion,
                CURRENT_TIMESTAMP AS timestamp_procesado
            FROM base
            WHERE fila_orden = 1
        """

        # Ejecutar y exportar a Parquet Silver
        copy_query = f"""
            COPY ({query_silver})
            TO '{self.silver_file.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        self.con.execute(copy_query)

        # Estadísticas de la capa Silver
        stats = self.con.execute(f"""
            SELECT 
                COUNT(*) AS total_silver,
                COUNT(DISTINCT estacion_id) AS estaciones,
                MIN(timestamp) AS fecha_min,
                MAX(timestamp) AS fecha_max
            FROM read_parquet('{self.silver_file.as_posix()}')
        """).fetchone()

        resultado = {
            "total_silver": stats[0],
            "estaciones": stats[1],
            "rango_fechas": f"{stats[2]} -> {stats[3]}" if stats[0] > 0 else "N/A",
            "archivo": str(self.silver_file),
            "tamano_kb": round(self.silver_file.stat().st_size / 1024, 2) if self.silver_file.exists() else 0,
        }
        logger.info("Silver completado: %s", resultado)
        return resultado

    # ============================================================
    # CAPA 3: GOLD (Datamarts y Agregaciones Analíticas con DuckDB)
    # ============================================================
    def procesar_gold(self) -> Dict[str, Any]:
        """
        Genera tres productos analíticos Gold a partir de Silver usando DuckDB:
          1. Indicadores Horarios (promedios, máximos, mínimos, lluvia acumulada).
          2. Indicadores Diarios (resumen bioclimático de 24h).
          3. KPIs de Salud y Disponibilidad por Estación.
        """
        logger.info("═══ [Lakehouse] Ejecutando Capa Gold (DuckDB Agregaciones) ═══")

        if not self.silver_file.exists():
            res_silver = self.procesar_silver()
            if res_silver.get("total_silver", 0) == 0:
                return {"agregaciones_calculadas": 0, "error": "silver_vacio"}

        # ── 1. Indicadores Horarios ─────────────────────────────────
        query_horarios = f"""
            SELECT
                estacion_id,
                date_trunc('hour', CAST(timestamp AS TIMESTAMP)) AS hora,
                ROUND(AVG(temperatura), 2) AS temp_promedio,
                ROUND(MAX(temperatura), 2) AS temp_maxima,
                ROUND(MIN(temperatura), 2) AS temp_minima,
                ROUND(AVG(humedad), 2) AS humedad_promedio,
                ROUND(SUM(COALESCE(precipitacion, 0.0)), 2) AS precipitacion_acumulada,
                ROUND(AVG(viento), 2) AS viento_promedio,
                ROUND(MAX(viento), 2) AS viento_maximo,
                ROUND(AVG(voltaje_bateria), 2) AS bateria_promedio,
                COUNT(*) AS total_lecturas
            FROM read_parquet('{self.silver_file.as_posix()}')
            GROUP BY estacion_id, date_trunc('hour', CAST(timestamp AS TIMESTAMP))
            ORDER BY estacion_id, hora DESC
        """
        self.con.execute(f"""
            COPY ({query_horarios})
            TO '{self.gold_horarios_file.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        # ── 2. Indicadores Diarios ──────────────────────────────────
        query_diarios = f"""
            SELECT
                estacion_id,
                CAST(date_trunc('day', CAST(timestamp AS TIMESTAMP)) AS DATE) AS fecha,
                ROUND(AVG(temperatura), 2) AS temp_media_diaria,
                ROUND(MAX(temperatura), 2) AS temp_max_diaria,
                ROUND(MIN(temperatura), 2) AS temp_min_diaria,
                ROUND(MAX(temperatura) - MIN(temperatura), 2) AS amplitud_termica,
                ROUND(AVG(humedad), 2) AS humedad_media_diaria,
                ROUND(SUM(COALESCE(precipitacion, 0.0)), 2) AS lluvia_total_24h,
                ROUND(MAX(viento), 2) AS rafaga_maxima_viento,
                ROUND(MIN(voltaje_bateria), 2) AS bateria_minima,
                COUNT(*) AS total_lecturas_dia
            FROM read_parquet('{self.silver_file.as_posix()}')
            GROUP BY estacion_id, date_trunc('day', CAST(timestamp AS TIMESTAMP))
            ORDER BY estacion_id, fecha DESC
        """
        self.con.execute(f"""
            COPY ({query_diarios})
            TO '{self.gold_diarios_file.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        # ── 3. KPIs de Salud y Calidad por Estación ─────────────────
        # Se cruza con Bronze para medir porcentaje de datos válidos vs descartados
        query_kpis = f"""
            SELECT
                b.estacion_id,
                COUNT(*) AS total_recibidos,
                SUM(CASE WHEN b.estado_calidad = 'valid' THEN 1 ELSE 0 END) AS lecturas_validas,
                SUM(CASE WHEN b.estado_calidad = 'invalid' THEN 1 ELSE 0 END) AS lecturas_invalidas,
                ROUND(100.0 * SUM(CASE WHEN b.estado_calidad = 'valid' THEN 1 ELSE 0 END) / COUNT(*), 1) AS porcentaje_calidad,
                ROUND(AVG(b.voltaje_bateria), 2) AS bateria_promedio,
                ROUND(AVG(b.rssi), 1) AS rssi_promedio,
                MAX(b.timestamp) AS ultima_telemetria
            FROM read_parquet('{self.bronze_file.as_posix()}') b
            GROUP BY b.estacion_id
            ORDER BY b.estacion_id
        """
        self.con.execute(f"""
            COPY ({query_kpis})
            TO '{self.gold_kpis_file.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        total_horarios = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.gold_horarios_file.as_posix()}')"
        ).fetchone()[0]
        total_diarios = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.gold_diarios_file.as_posix()}')"
        ).fetchone()[0]
        total_estaciones = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.gold_kpis_file.as_posix()}')"
        ).fetchone()[0]

        resultado = {
            "agregaciones_calculadas": total_horarios,
            "dias_procesados": total_diarios,
            "estaciones_kpi": total_estaciones,
            "archivos": {
                "horarios": str(self.gold_horarios_file),
                "diarios": str(self.gold_diarios_file),
                "kpis": str(self.gold_kpis_file),
            },
        }
        logger.info("Gold completado: %s", resultado)
        return resultado

    # ============================================================
    # PIPELINE INTEGRAL (Bronze → Silver → Gold)
    # ============================================================
    def ejecutar_pipeline_completo(self) -> Dict[str, Any]:
        """Ejecuta secuencialmente las 3 capas del Lakehouse Medallion."""
        t_inicio = datetime.now()
        res_bronze = self.procesar_bronze()
        res_silver = self.procesar_silver()
        res_gold = self.procesar_gold()
        duracion = (datetime.now() - t_inicio).total_seconds()

        return {
            "duracion_segundos": round(duracion, 2),
            "bronze": res_bronze,
            "silver": res_silver,
            "gold": res_gold,
            "resumen": self.obtener_resumen_lakehouse(),
        }

    # ============================================================
    # CONSULTAS ANALÍTICAS INTERACTIVAS CON DUCKDB
    # ============================================================
    def consultar_sql(self, sql_query: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL analítica arbitraria con DuckDB.
        Soporta leer directamente los archivos Parquet del Lakehouse.
        """
        return self.con.execute(sql_query).df()

    def obtener_resumen_lakehouse(self) -> Dict[str, Any]:
        """Devuelve el inventario de capas, número de archivos y tamaños en KB."""
        resumen = {}
        for capa, path in [("bronze", self.bronze_dir), ("silver", self.silver_dir), ("gold", self.gold_dir)]:
            if path.exists():
                archivos = list(path.glob("*.parquet"))
                resumen[capa] = {
                    "total_archivos": len(archivos),
                    "archivos": [a.name for a in archivos],
                    "tamano_total_kb": round(sum(a.stat().st_size for a in archivos) / 1024, 2),
                }
            else:
                resumen[capa] = {"total_archivos": 0, "archivos": [], "tamano_total_kb": 0.0}
        return resumen

    def obtener_kpis_estaciones(self) -> pd.DataFrame:
        """Retorna el DataFrame con KPIs analíticos de las estaciones desde Gold."""
        if self.gold_kpis_file.exists():
            return self.consultar_sql(f"SELECT * FROM read_parquet('{self.gold_kpis_file.as_posix()}')")
        return pd.DataFrame()

    def obtener_indicadores_horarios(self, limite: int = 100) -> pd.DataFrame:
        """Retorna los últimos indicadores horarios desde Gold."""
        if self.gold_horarios_file.exists():
            return self.consultar_sql(
                f"SELECT * FROM read_parquet('{self.gold_horarios_file.as_posix()}') LIMIT {limite}"
            )
        return pd.DataFrame()


# Instancia singleton del lakehouse
_lakehouse_singleton: Optional[AmazonIALakehouse] = None


def obtener_lakehouse() -> AmazonIALakehouse:
    """Devuelve la instancia singleton del Lakehouse."""
    global _lakehouse_singleton
    if _lakehouse_singleton is None:
        _lakehouse_singleton = AmazonIALakehouse()
    return _lakehouse_singleton
