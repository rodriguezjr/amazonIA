"""
Ejecutor local del Pipeline AmazonIA 4.0 (Offline-First).
Permite correr manualmente todas las etapas del pipeline Medallion (DuckDB / Parquet)
y RAG sin depender de Docker ni de la interfaz web de Airflow.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

# Forzar UTF-8 en stdout para terminales Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Configurar path raíz
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("amazonia.pipeline.runner")


def ejecutar_pipeline_completo():
    print("\n" + "=" * 65)
    print("   AMAZONIA 4.0 - PIPELINE MEDALLION DUCKDB & IA (OFFLINE-FIRST)")
    print("=" * 65)
    print(f"Inicio de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. Sincronización y Validación de Buffer
    print("[1/5] Sincronizacion Edge Buffer...")
    try:
        from src.sincronizacion import sincronizar_todo
        res_sync = sincronizar_todo()
        print(f"      [OK] Sincronizacion: {res_sync}")
    except Exception as e:
        print(f"      [INFO] Edge Gateway en reposo / local: {e}")

    # 2. Pipeline Medallion con DuckDB (Bronze → Silver → Gold)
    from src.lakehouse.manager import AmazonIALakehouse
    lakehouse = AmazonIALakehouse()

    print("\n[2/5] Capa Bronze -> Silver (Ingesta & Limpieza DuckDB SQL)...")
    res_bronze = lakehouse.procesar_bronze()
    print(f"      [OK] Total Bronze leidos: {res_bronze.get('total_bronze', 0)}")
    print(f"      [OK] Archivo Bronze Raw: {res_bronze.get('archivo', 'N/A')} ({res_bronze.get('tamaño_kb', 0)} KB)")

    res_silver = lakehouse.procesar_silver()
    print(f"      [OK] Registros validos en Silver: {res_silver.get('total_silver', 0)}")
    print(f"      [OK] Estaciones depuradas: {res_silver.get('estaciones', 0)}")
    print(f"      [OK] Rango temporal: {res_silver.get('rango_fechas', 'N/A')}")
    print(f"      [OK] Archivo Silver: {res_silver.get('archivo', 'N/A')} ({res_silver.get('tamaño_kb', 0)} KB)")

    # 3. Capa Gold (Datamarts y Agregaciones Horarias / Diarias / KPIs)
    print("\n[3/5] Capa Gold (Datamarts Analiticos DuckDB)...")
    res_gold = lakehouse.procesar_gold()
    print(f"      [OK] Agregaciones horarias calculadas: {res_gold.get('agregaciones_calculadas', 0)}")
    print(f"      [OK] Resumenes diarios generados: {res_gold.get('dias_procesados', 0)}")
    print(f"      [OK] KPIs de estaciones evaluados: {res_gold.get('estaciones_kpi', 0)}")
    for tipo, ruta in res_gold.get("archivos", {}).items():
        print(f"           * {tipo.capitalize()}: {ruta}")

    # 4. Actualización de Memoria Semántica RAG (Qdrant)
    print("\n[4/5] Indexacion RAG (Memoria Semantica Qdrant)...")
    from dags.scripts.rag_indexer import main as rag_main
    res_rag = rag_main()
    print(f"      [OK] Documentos tecnicos indexados: {res_rag.get('documentos_indexados', 0)}")
    print(f"      [OK] Coleccion Qdrant: {res_rag.get('coleccion', 'N/A')}")
    print(f"      [OK] Estado: {res_rag.get('estado', 'N/A')}")

    # 5. Verificación de Umbrales Críticos y Alertas Comunitarias
    print("\n[5/5] Evaluacion de Umbrales de Alerta...")
    from src.database import obtener_ultimas_lecturas
    from src.validacion import verificar_alertas

    lecturas = obtener_ultimas_lecturas(limite=50)
    alertas = []
    for l in lecturas:
        for a in verificar_alertas(l):
            alertas.append(f"[{l['estacion_id']}] {a['mensaje']} (Severidad: {a['severidad']})")

    if alertas:
        print(f"      [ALERTA] {len(alertas)} alertas detectadas en las ultimas lecturas:")
        for a in alertas[:5]:
            print(f"         - {a}")
    else:
        print("      [OK] Todas las variables se encuentran dentro de limites normales.")

    # Resumen del Lakehouse
    resumen_lh = lakehouse.obtener_resumen_lakehouse()
    print("\n" + "=" * 65)
    print("   [EXITO] PIPELINE COMPLETADO SATISFACTORIAMENTE")
    print(f"   Almacenamiento Lakehouse Parquet:")
    for capa, info in resumen_lh.items():
        kb = info.get("tamano_total_kb", info.get("tamaño_total_kb", 0))
        print(f"     - Capa {capa.upper()}: {info['total_archivos']} archivo(s), {kb} KB")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    ejecutar_pipeline_completo()
