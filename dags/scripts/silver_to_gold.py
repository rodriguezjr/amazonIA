"""
Procesamiento Silver → Gold con DuckDB y Apache Parquet.
Calcula agregaciones horarias, diarias y KPIs de calidad sobre la capa Silver.
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

logger = logging.getLogger("amazonia.pipeline.silver_to_gold")

_DB_PATH = os.getenv("DB_PATH", str(_ROOT / "data" / "telemetria.db"))
_LAKEHOUSE_DIR = Path(os.getenv("LAKEHOUSE_DIR", str(_ROOT / "data" / "lakehouse")))


def main() -> dict:
    """
    Ejecuta el cálculo de agregaciones analíticas Gold usando DuckDB.
    Retorna un diccionario con el resumen de la operación.
    """
    logger.info("═══ [DuckDB] Iniciando procesamiento Silver → Gold ═══")

    lakehouse = AmazonIALakehouse(
        db_path=Path(_DB_PATH),
        lakehouse_dir=_LAKEHOUSE_DIR,
    )

    # Procesar Silver → Gold con DuckDB SQL
    res_gold = lakehouse.procesar_gold()

    # Obtener rango temporal de los indicadores horarios si existen
    rango_temporal = "N/A"
    if lakehouse.gold_horarios_file.exists():
        try:
            rango = lakehouse.consultar_sql(f"""
                SELECT MIN(hora) AS min_h, MAX(hora) AS max_h
                FROM read_parquet('{lakehouse.gold_horarios_file.as_posix()}')
            """).iloc[0]
            if rango["min_h"] is not None and rango["max_h"] is not None:
                rango_temporal = f"{rango['min_h']} -> {rango['max_h']}"
        except Exception as e:
            logger.warning("No se pudo calcular el rango temporal: %s", e)

    resultado = {
        "agregaciones_calculadas": res_gold.get("agregaciones_calculadas", 0),
        "estaciones_procesadas": res_gold.get("estaciones_kpi", 0),
        "dias_procesados": res_gold.get("dias_procesados", 0),
        "rango_temporal": rango_temporal,
        "archivo_destino": str(lakehouse.gold_horarios_file),
        "archivos": res_gold.get("archivos", {}),
    }
    logger.info("═══ [DuckDB] Silver → Gold completado: %s ═══", resultado)
    return resultado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    res = main()
    print(f"Resultado: {res}")
