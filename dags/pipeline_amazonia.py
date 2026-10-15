"""
DAG de Airflow para el pipeline de AmazonIA 4.0.
Orquesta el flujo completo: ingesta → validación → agregación → alertas.

Este DAG implementa el flujo dinámico automatizado descrito en el Capítulo III.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule
import requests
import logging

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN DEL DAG
# ============================================================

default_args = {
    "owner": "amazonia4.0",
    "depends_on_past": False,
    "email": ["alertas@amazonia4.0.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="amazonia_4_0_pipeline",
    default_args=default_args,
    description="Pipeline completo de AmazonIA 4.0: Ingesta → Lakehouse → IA → Alertas",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["amazonia", "telemetria", "edge", "rag"],
    doc_md="""
    ## Pipeline AmazonIA 4.0
    
    Este DAG orquesta el flujo de datos desde el Edge Gateway hasta la 
    generación de alertas para las comunidades de la cuenca del río Caroní.
    
    ### Flujo:
    1. Verificar disponibilidad del Edge Gateway
    2. Sincronizar datos del buffer local
    3. Evaluar calidad (branching)
    4. Procesar Bronze → Silver
    5. Procesar Silver → Gold
    6. Actualizar Qdrant (RAG)
    7. Verificar umbrales y alertar
    """,
)


# ============================================================
# TAREA 1: VERIFICAR DISPONIBILIDAD DEL EDGE GATEWAY
# ============================================================

verificar_gateway = HttpSensor(
    task_id="verificar_gateway",
    http_conn_id="edge_gateway",
    endpoint="/health",
    poke_interval=60,
    timeout=300,
    mode="reschedule",
    dag=dag,
)


# ============================================================
# TAREA 2: SINCRONIZAR DATOS DESDE EL EDGE GATEWAY
# ============================================================

def sincronizar_datos_edge(**context):
    """
    Sincroniza los datos del buffer local del Edge Gateway.
    Envía los registros pendientes al sistema central.
    """
    from src.sincronizacion import sincronizar_todo
    
    logger.info("Iniciando sincronización desde Edge Gateway...")
    resultado = sincronizar_todo()
    
    # Pasar resultado a la siguiente tarea vía XCom
    context["ti"].xcom_push(key="registros_sincronizados", value=resultado)
    
    logger.info(f"Sincronización completada: {resultado}")
    return resultado


sincronizar = PythonOperator(
    task_id="sincronizar_datos_edge",
    python_callable=sincronizar_datos_edge,
    dag=dag,
)


# ============================================================
# TAREA 3: EVALUAR CALIDAD (BRANCHING)
# ============================================================

def evaluar_calidad(**context):
    """
    Evalúa la calidad de los datos sincronizados.
    Decide la ruta: procesamiento normal o notificación de calidad baja.
    """
    ti = context["ti"]
    resultado = ti.xcom_pull(task_ids="sincronizar_datos_edge", key="registros_sincronizados")
    
    if not resultado:
        logger.warning("No hay datos sincronizados")
        return "notificar_calidad_baja"
    
    total = resultado.get("total_enviados", 0)
    exitosos = resultado.get("total_exitosos", 0)
    
    if total == 0:
        logger.info("No hay registros para procesar")
        return "notificar_calidad_baja"
    
    porcentaje_calidad = (exitosos / total) * 100
    logger.info(f"Calidad de datos: {porcentaje_calidad:.1f}% ({exitosos}/{total})")
    
    # Umbral de calidad: 80%
    if porcentaje_calidad >= 80:
        return "procesar_bronze_silver"
    else:
        return "notificar_calidad_baja"


branching_calidad = BranchPythonOperator(
    task_id="evaluar_calidad",
    python_callable=evaluar_calidad,
    dag=dag,
)


# ============================================================
# TAREA 4: PROCESAR BRONZE → SILVER
# ============================================================

def procesar_bronze_silver(**context):
    """
    Limpia y valida los datos de la capa Bronze.
    Escribe los datos depurados en la capa Silver.
    """
    logger.info("Procesando Bronze → Silver...")
    
    # Simulación del procesamiento Spark
    # En producción, esto ejecutaría un job de Spark
    import sqlite3
    from src.config import DB_PATH
    from src.database import get_connection
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Obtener registros válidos de la última hora
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM telemetria
            WHERE estado_calidad = 'valid'
            AND fecha_recepcion >= datetime('now', '-1 hour')
        """)
        total_validos = cursor.fetchone()["total"]
        
        # Obtener registros inválidos
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM telemetria
            WHERE estado_calidad = 'invalid'
            AND fecha_recepcion >= datetime('now', '-1 hour')
        """)
        total_invalidos = cursor.fetchone()["total"]
    
    resultado = {
        "registros_validos": total_validos,
        "registros_invalidos": total_invalidos,
        "capa_destino": "silver",
    }
    
    context["ti"].xcom_push(key="resultado_bronze_silver", value=resultado)
    logger.info(f"Bronze → Silver completado: {resultado}")
    return resultado


procesar_bronze_silver = PythonOperator(
    task_id="procesar_bronze_silver",
    python_callable=procesar_bronze_silver,
    dag=dag,
)


# ============================================================
# TAREA 5: PROCESAR SILVER → GOLD
# ============================================================

def procesar_silver_gold(**context):
    """
    Calcula agregaciones e indicadores sobre los datos de Silver.
    Escribe los indicadores en la capa Gold.
    """
    logger.info("Procesando Silver → Gold...")
    
    from src.database import get_connection
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Calcular agregaciones horarias
        cursor.execute("""
            SELECT 
                estacion_id,
                strftime('%Y-%m-%d %H:00:00', timestamp) as hora,
                AVG(temperatura) as temp_promedio,
                MAX(temperatura) as temp_maxima,
                MIN(temperatura) as temp_minima,
                AVG(humedad) as humedad_promedio,
                SUM(precipitacion) as precipitacion_acumulada,
                AVG(viento) as viento_promedio,
                COUNT(*) as total_lecturas
            FROM telemetria
            WHERE estado_calidad = 'valid'
            AND timestamp >= datetime('now', '-1 hour')
            GROUP BY estacion_id, strftime('%Y-%m-%d %H:00:00', timestamp)
        """)
        
        agregaciones = [dict(row) for row in cursor.fetchall()]
    
    resultado = {
        "agregaciones_calculadas": len(agregaciones),
        "capa_destino": "gold",
        "datos": agregaciones,
    }
    
    context["ti"].xcom_push(key="resultado_silver_gold", value=resultado)
    logger.info(f"Silver → Gold completado: {len(agregaciones)} agregaciones")
    return resultado


procesar_silver_gold = PythonOperator(
    task_id="procesar_silver_gold",
    python_callable=procesar_silver_gold,
    dag=dag,
)


# ============================================================
# TAREA 6: ACTUALIZAR QDRANT (RAG)
# ============================================================

def actualizar_qdrant(**context):
    """
    Actualiza la base vectorial Qdrant con nuevos documentos y datos.
    Prepara la memoria semántica para consultas RAG.
    """
    logger.info("Actualizando Qdrant (memoria semántica)...")
    
    # Simulación de actualización de Qdrant
    # En producción, esto indexaría embeddings de documentos y datos
    documentos_indexados = 5  # Placeholder
    
    resultado = {
        "documentos_indexados": documentos_indexados,
        "coleccion": "documentos_amazonia",
        "estado": "exitoso",
    }
    
    context["ti"].xcom_push(key="resultado_qdrant", value=resultado)
    logger.info(f"Qdrant actualizado: {resultado}")
    return resultado


actualizar_qdrant = PythonOperator(
    task_id="actualizar_qdrant",
    python_callable=actualizar_qdrant,
    dag=dag,
)


# ============================================================
# TAREA 7: VERIFICAR UMBRALES Y ENVIAR ALERTAS
# ============================================================

def verificar_y_alertar(**context):
    """
    Verifica si alguna variable supera los umbrales críticos.
    Envía alertas a las comunidades y técnicos si es necesario.
    """
    from src.database import obtener_ultimas_lecturas
    from src.validacion import verificar_alertas
    
    logger.info("Verificando umbrales de alerta...")
    
    lecturas = obtener_ultimas_lecturas(limite=50)
    alertas_enviadas = []
    
    for lectura in lecturas:
        alertas = verificar_alertas(lectura)
        for alerta in alertas:
            # En producción, aquí se enviaría la alerta vía Telegram/SMS
            logger.warning(f"ALERTA: {alerta['mensaje']}")
            alertas_enviadas.append({
                "estacion_id": lectura["estacion_id"],
                "tipo": alerta["tipo"],
                "mensaje": alerta["mensaje"],
                "severidad": alerta["severidad"],
            })
    
    resultado = {
        "alertas_enviadas": len(alertas_enviadas),
        "detalle": alertas_enviadas,
    }
    
    context["ti"].xcom_push(key="resultado_alertas", value=resultado)
    logger.info(f"Alertas enviadas: {len(alertas_enviadas)}")
    return resultado


enviar_alertas = PythonOperator(
    task_id="enviar_alertas",
    python_callable=verificar_y_alertar,
    dag=dag,
)


# ============================================================
# TAREA 8: NOTIFICAR CALIDAD BAJA
# ============================================================

def notificar_calidad_baja(**context):
    """
    Notifica a los técnicos cuando la calidad de los datos es baja.
    """
    logger.warning("Calidad de datos baja - Revisar sensores")
    
    # En producción, aquí se enviaría una notificación
    resultado = {
        "estado": "notificado",
        "mensaje": "Calidad de datos por debajo del umbral del 80%",
    }
    
    context["ti"].xcom_push(key="resultado_notificacion", value=resultado)
    return resultado


notificar_calidad_baja = PythonOperator(
    task_id="notificar_calidad_baja",
    python_callable=notificar_calidad_baja,
    dag=dag,
)


# ============================================================
# TAREA DE CIERRE
# ============================================================

finalizar = DummyOperator(
    task_id="finalizar",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)


# ============================================================
# DEFINICIÓN DE DEPENDENCIAS
# ============================================================

# Flujo principal
verificar_gateway >> sincronizar >> branching_calidad

# Ruta A: Calidad alta → Procesamiento completo
branching_calidad >> procesar_bronze_silver >> procesar_silver_gold >> actualizar_qdrant >> enviar_alertas >> finalizar

# Ruta B: Calidad baja → Notificación
branching_calidad >> notificar_calidad_baja >> finalizar