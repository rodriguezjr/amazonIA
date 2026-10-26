"""
Utilidades y helpers para los DAGs de AmazonIA 4.0.
Proporciona rutas del lakehouse y funciones auxiliares comunes a todas las tareas.
"""
import os
import logging
from pathlib import Path

logger = logging.getLogger("amazonia.pipeline.helpers")

# Detectar si estamos dentro de un contenedor Airflow o en local
_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "")
_EN_CONTENEDOR = bool(_AIRFLOW_HOME and Path(_AIRFLOW_HOME).exists())

if _EN_CONTENEDOR:
    # Rutas dentro del contenedor Docker Airflow
    PROYECTO_ROOT = Path("/opt/airflow")
    DB_PATH = PROYECTO_ROOT / "data" / "telemetria.db"
else:
    # Rutas locales de desarrollo
    PROYECTO_ROOT = Path(__file__).resolve().parent.parent.parent
    DB_PATH = PROYECTO_ROOT / "data" / "telemetria.db"

LAKEHOUSE_BASE = PROYECTO_ROOT / "data" / "lakehouse"
SILVER_PATH = LAKEHOUSE_BASE / "silver"
GOLD_PATH = LAKEHOUSE_BASE / "gold"
KNOWLEDGE_BASE = PROYECTO_ROOT / "knowledge_base"


def asegurar_directorios():
    """Crea la estructura de directorios del lakehouse si no existe."""
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    GOLD_PATH.mkdir(parents=True, exist_ok=True)
    logger.info("Directorios lakehouse asegurados en %s", LAKEHOUSE_BASE)


def obtener_resumen_lakehouse() -> dict:
    """Retorna un resumen del estado actual del lakehouse (archivos y tamaños)."""
    resumen = {}
    for capa, ruta in [("silver", SILVER_PATH), ("gold", GOLD_PATH)]:
        if ruta.exists():
            archivos = list(ruta.glob("*.parquet"))
            resumen[capa] = {
                "archivos": len(archivos),
                "tamaño_total_kb": sum(f.stat().st_size for f in archivos) / 1024,
            }
        else:
            resumen[capa] = {"archivos": 0, "tamaño_total_kb": 0}
    return resumen
