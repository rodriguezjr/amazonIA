"""
Procesamiento Bronze → Silver con DuckDB y Apache Parquet.
Limpia, deduplica y valida termodinámicamente los datos de telemetría.
Arquitectura Medallion para AmazonIA 4.0.
"""
import sys
import os
import logging
from pathlib import Path

# Agregar la raíz del proyecto al PYTHONPATH
_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))

from src.lakehouse.manager import AmazonIALakehouse

logger = logging.getLogger("amazonia.pipeline.bronze_to_silver")

_DB_PATH = os.getenv("DB_PATH", str(_ROOT / "data" / "telemetria.db"))
_LAKEHOUSE_DIR = Path(os.getenv("LAKEHOUSE_DIR", str(_ROOT / "data" / "lakehouse")))


def main() -> dict:
    """
    Ejecuta la ingesta a Bronze y la depuración a Silver usando DuckDB.
    Retorna un diccionario con el resumen de la operación.
    """
    logger.info("═══ [DuckDB] Iniciando procesamiento Bronze → Silver ═══")

    lakehouse = AmazonIALakehouse(
        db_path=Path(_DB_PATH),
        lakehouse_dir=_LAKEHOUSE_DIR,
    )

    # 1. Asegurar ingesta a Bronze
    res_bronze = lakehouse.procesar_bronze()

    # 2. Procesar Bronze → Silver con DuckDB SQL
    res_silver = lakehouse.procesar_silver()

    resultado = {
        "total_bronze": res_bronze.get("total_bronze", 0),
        "total_silver": res_silver.get("total_silver", 0),
        "estaciones": res_silver.get("estaciones", 0),
        "rango_fechas": res_silver.get("rango_fechas", "N/A"),
        "archivo_destino": res_silver.get("archivo", ""),
    }
    logger.info("═══ [DuckDB] Bronze → Silver completado: %s ═══", resultado)
    return resultado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    res = main()
    print(f"Resultado: {res}")
