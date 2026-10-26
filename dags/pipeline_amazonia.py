"""
DAG de Airflow para el pipeline de AmazonIA 4.0.
Orquesta el flujo completo: ingesta → validación → lakehouse (Medallion) → RAG → alertas.

Calibrado para funcionar con:
  - pandas (sin Spark/Delta Lake)
  - Qdrant offline (sin LangChain/OpenAI)
  - SQLite como fuente primaria (Offline-First)

Este DAG implementa el flujo dinámico automatizado descrito en el Capítulo III.
"""
import sys
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Agregar el directorio raíz del proyecto al PYTHONPATH
# Dentro del contenedor: /opt/airflow;  localmente se detecta automáticamente
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger("amazonia.dag.pipeline")


# ============================================================
# CONFIGURACIÓN DEL DAG
# ============================================================

default_args = {
    "owner": "amazonia4.0",
    "depends_on_past": False,
    "email": ["alertas@amazonia4.0.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="amazonia_4_0_pipeline",
    default_args=default_args,
    description="Pipeline Medallion de AmazonIA 4.0: Ingesta → Bronze → Silver → Gold → RAG → Alertas",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["amazonia", "telemetria", "edge", "rag", "medallion"],
    doc_md="""
    ## Pipeline AmazonIA 4.0

    Este DAG orquesta el flujo de datos completo del sistema de monitoreo
    agroclimático para las comunidades de la cuenca del río Caroní.

    ### Arquitectura Medallion (Lakehouse)
    ```
    SQLite (Bronze) → Parquet Silver → Parquet Gold → Qdrant (RAG)
    ```

    ### Flujo de Tareas:
    1. **Verificar Gateway** — Valida conectividad con la API FastAPI
    2. **Sincronizar Datos** — Envía registros pendientes al sistema central
    3. **Evaluar Calidad** — Decide la ruta según % de datos válidos
    4. **Bronze → Silver** — Limpieza, validación y deduplicación (pandas)
    5. **Silver → Gold** — Agregaciones horarias e indicadores (pandas)
    6. **Actualizar Qdrant** — Re-indexa la base vectorial para RAG
    7. **Verificar Alertas** — Evalúa umbrales críticos y notifica
    """,
)


# ============================================================
# TAREA 1: VERIFICAR DISPONIBILIDAD DEL EDGE GATEWAY
# ============================================================

def _verificar_gateway_fn(**context):
    """
    Verifica que la API FastAPI (Edge Gateway) esté respondiendo.
    Usa requests directamente en lugar de HttpSensor para evitar
    configuración de conexión Airflow en el entorno académico.
    """
    import requests

    # Dentro de Docker: http://api:8000; localmente: http://localhost:8000
    api_host = os.getenv("API_HOST", "api")
    api_port = os.getenv("API_PORT", "8000")
    url = f"http://{api_host}:{api_port}/health"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Edge Gateway disponible en %s", url)
            return {"status": "disponible", "url": url}
        else:
            logger.warning("⚠️ Gateway respondió con código %d", response.status_code)
            return {"status": "degradado", "codigo": response.status_code}
    except requests.ConnectionError:
        logger.warning("⚠️ No se pudo conectar al Gateway en %s. Continuando en modo offline.", url)
        return {"status": "offline", "url": url}
    except Exception as e:
        logger.warning("⚠️ Error verificando Gateway: %s. Continuando en modo offline.", e)
        return {"status": "error", "detalle": str(e)}


verificar_gateway = PythonOperator(
    task_id="verificar_gateway",
    python_callable=_verificar_gateway_fn,
    dag=dag,
)


# ============================================================
# TAREA 2: SINCRONIZAR DATOS DESDE EL EDGE GATEWAY
# ============================================================

def _sincronizar_datos_fn(**context):
    """
    Sincroniza los datos del buffer local del Edge Gateway.
    Envía los registros pendientes al sistema central via API.
    """
    try:
        from src.sincronizacion import sincronizar_todo

        logger.info("Iniciando sincronización desde Edge Gateway...")
        resultado = sincronizar_todo()
    except Exception as e:
        logger.warning("No se pudo sincronizar (modo offline): %s", e)
        resultado = {"total_enviados": 0, "total_exitosos": 0, "modo": "offline"}

    # Pasar resultado a la siguiente tarea vía XCom
    context["ti"].xcom_push(key="registros_sincronizados", value=resultado)
    logger.info("Sincronización completada: %s", resultado)
    return resultado


sincronizar = PythonOperator(
    task_id="sincronizar_datos_edge",
    python_callable=_sincronizar_datos_fn,
    dag=dag,
)


# ============================================================
# TAREA 3: EVALUAR CALIDAD (BRANCHING)
# ============================================================

def _evaluar_calidad_fn(**context):
    """
    Evalúa la calidad de los datos disponibles en SQLite.
    Decide la ruta: procesamiento normal o notificación de calidad baja.
    Si no hay datos de sincronización, verifica directamente en la DB.
    """
    ti = context["ti"]
    resultado = ti.xcom_pull(task_ids="sincronizar_datos_edge", key="registros_sincronizados")

    # Si hay resultado de sincronización, evaluar por proporción
    if resultado and resultado.get("total_enviados", 0) > 0:
        total = resultado.get("total_enviados", 0)
        exitosos = resultado.get("total_exitosos", 0)
        porcentaje_calidad = (exitosos / total) * 100 if total > 0 else 0
        logger.info("Calidad por sincronización: %.1f%% (%d/%d)", porcentaje_calidad, exitosos, total)
    else:
        # Evaluar directamente desde SQLite
        try:
            from src.database import obtener_estadisticas_calidad
            stats = obtener_estadisticas_calidad()
            total = sum(stats.values())
            validos = stats.get("valid", 0)
            porcentaje_calidad = (validos / total) * 100 if total > 0 else 0
            logger.info("Calidad por estadísticas DB: %.1f%% (%d/%d válidos)", porcentaje_calidad, validos, total)
        except Exception as e:
            logger.warning("Error evaluando calidad: %s. Continuando con procesamiento.", e)
            porcentaje_calidad = 100  # Asumir OK para no bloquear el pipeline

    # Umbral de calidad: 50% (reducido para entorno académico / demo)
    if porcentaje_calidad >= 50:
        return "procesar_bronze_silver"
    else:
        return "notificar_calidad_baja"


branching_calidad = BranchPythonOperator(
    task_id="evaluar_calidad",
    python_callable=_evaluar_calidad_fn,
    dag=dag,
)


# ============================================================
# TAREA 4: PROCESAR BRONZE → SILVER (pandas)
# ============================================================

def _procesar_bronze_silver_fn(**context):
    """
    Limpia y valida los datos de la capa Bronze usando pandas.
    Escribe los datos depurados como Parquet en la capa Silver.
    """
    logger.info("Procesando Bronze → Silver...")

    from dags.scripts.bronze_to_silver import main as bronze_main
    resultado = bronze_main()

    context["ti"].xcom_push(key="resultado_bronze_silver", value=resultado)
    logger.info("Bronze → Silver completado: %s", resultado)
    return resultado


procesar_bronze_silver = PythonOperator(
    task_id="procesar_bronze_silver",
    python_callable=_procesar_bronze_silver_fn,
    dag=dag,
)


# ============================================================
# TAREA 5: PROCESAR SILVER → GOLD (pandas)
# ============================================================

def _procesar_silver_gold_fn(**context):
    """
    Calcula agregaciones horarias e indicadores climáticos sobre Silver.
    Escribe los indicadores como Parquet en la capa Gold.
    """
    logger.info("Procesando Silver → Gold...")

    from dags.scripts.silver_to_gold import main as gold_main
    resultado = gold_main()

    context["ti"].xcom_push(key="resultado_silver_gold", value=resultado)
    logger.info("Silver → Gold completado: %s", resultado)
    return resultado


procesar_silver_gold = PythonOperator(
    task_id="procesar_silver_gold",
    python_callable=_procesar_silver_gold_fn,
    dag=dag,
)


# ============================================================
# TAREA 6: ACTUALIZAR QDRANT (RAG)
# ============================================================

def _actualizar_qdrant_fn(**context):
    """
    Actualiza la base vectorial Qdrant con los documentos de knowledge_base/.
    Usa AmazonIAVectorStore (embeddings offline deterministas).
    """
    logger.info("Actualizando Qdrant (memoria semántica)...")

    from dags.scripts.rag_indexer import main as rag_main
    resultado = rag_main()

    context["ti"].xcom_push(key="resultado_qdrant", value=resultado)
    logger.info("Qdrant actualizado: %s", resultado)
    return resultado


actualizar_qdrant = PythonOperator(
    task_id="actualizar_qdrant",
    python_callable=_actualizar_qdrant_fn,
    dag=dag,
)


# ============================================================
# TAREA 7: VERIFICAR UMBRALES Y ENVIAR ALERTAS
# ============================================================

def _verificar_y_alertar_fn(**context):
    """
    Verifica si alguna variable supera los umbrales críticos.
    Genera alertas para las comunidades y técnicos.
    """
    logger.info("Verificando umbrales de alerta...")

    try:
        from src.database import obtener_ultimas_lecturas
        from src.validacion import verificar_alertas

        lecturas = obtener_ultimas_lecturas(limite=50)
        alertas_enviadas = []

        for lectura in lecturas:
            alertas = verificar_alertas(lectura)
            for alerta in alertas:
                # En producción, aquí se enviaría la alerta vía Telegram/SMS/MQTT
                logger.warning("ALERTA: %s", alerta["mensaje"])
                alertas_enviadas.append({
                    "estacion_id": lectura["estacion_id"],
                    "tipo": alerta["tipo"],
                    "mensaje": alerta["mensaje"],
                    "severidad": alerta["severidad"],
                })

        resultado = {
            "alertas_enviadas": len(alertas_enviadas),
            "detalle": alertas_enviadas[:10],  # Limitar para XCom
        }
    except Exception as e:
        logger.error("Error verificando alertas: %s", e)
        resultado = {"alertas_enviadas": 0, "error": str(e)}

    context["ti"].xcom_push(key="resultado_alertas", value=resultado)
    logger.info("Alertas procesadas: %s", resultado.get("alertas_enviadas", 0))
    return resultado


enviar_alertas = PythonOperator(
    task_id="enviar_alertas",
    python_callable=_verificar_y_alertar_fn,
    dag=dag,
)


# ============================================================
# TAREA 8: NOTIFICAR CALIDAD BAJA
# ============================================================

def _notificar_calidad_baja_fn(**context):
    """
    Notifica a los técnicos cuando la calidad de los datos es baja.
    """
    logger.warning("⚠️ Calidad de datos baja detectada — Revisar sensores y estaciones")

    resultado = {
        "estado": "notificado",
        "mensaje": "Calidad de datos por debajo del umbral del 50%. Revisar calibración de sensores.",
        "timestamp": datetime.now().isoformat(),
    }

    context["ti"].xcom_push(key="resultado_notificacion", value=resultado)
    return resultado


notificar_calidad_baja = PythonOperator(
    task_id="notificar_calidad_baja",
    python_callable=_notificar_calidad_baja_fn,
    dag=dag,
)


# ============================================================
# TAREA DE CIERRE
# ============================================================

finalizar = EmptyOperator(
    task_id="finalizar",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)


# ============================================================
# DEFINICIÓN DE DEPENDENCIAS
# ============================================================

# Flujo principal
verificar_gateway >> sincronizar >> branching_calidad

# Ruta A: Calidad aceptable → Procesamiento Medallion completo
branching_calidad >> procesar_bronze_silver >> procesar_silver_gold >> actualizar_qdrant >> enviar_alertas >> finalizar

# Ruta B: Calidad baja → Notificación directa
branching_calidad >> notificar_calidad_baja >> finalizar
