"""
Mecanismo de sincronización por lotes (batching).
Implementa el paradigma Offline-First: los datos se acumulan
localmente y se sincronizan cuando hay conexión.
"""
import time
import requests
import logging
from datetime import datetime

from src.database import (
    obtener_no_sincronizados,
    marcar_como_sincronizados,
    get_connection,
)
from src.config import SYNC_ENDPOINT, SYNC_BATCH_SIZE, SYNC_MAX_RETRIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def registrar_log_sincronizacion(enviados: int, exitosos: int, estado: str, mensaje: str = None):
    """Registra el resultado de una sincronización en la base de datos."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO log_sincronizacion 
            (registros_enviados, registros_exitosos, estado, mensaje)
            VALUES (?, ?, ?, ?)
        """, (enviados, exitosos, estado, mensaje))


def sincronizar_lote() -> dict:
    """
    Sincroniza un lote de registros pendientes.
    
    Returns:
        dict: Resultado de la sincronización
    """
    registros = obtener_no_sincronizados(SYNC_BATCH_SIZE)
    
    if not registros:
        return {"total": 0, "exitosos": 0, "estado": "sin_datos"}
    
    # Preparar payload
    payload = {"datos": [
        {k: v for k, v in r.items() if k not in ("id", "sincronizado", "fecha_recepcion", "estado_calidad", "errores")}
        for r in registros
    ]}
    
    # Intentar enviar con reintentos
    for intento in range(SYNC_MAX_RETRIES):
        try:
            response = requests.post(SYNC_ENDPOINT, json=payload, timeout=10)
            
            if response.status_code == 200:
                ids = [r["id"] for r in registros]
                marcar_como_sincronizados(ids)
                
                logger.info(f"Sincronización exitosa: {len(ids)} registros")
                registrar_log_sincronizacion(
                    len(ids), len(ids), "exitoso",
                    f"Lote de {len(ids)} registros sincronizados"
                )
                
                return {
                    "total": len(ids),
                    "exitosos": len(ids),
                    "estado": "exitoso",
                }
            else:
                logger.warning(f"Error HTTP {response.status_code} en intento {intento + 1}")
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error de conexión en intento {intento + 1}: {e}")
            time.sleep(2 ** intento)  # Backoff exponencial
    
    # Si todos los intentos fallan
    logger.error(f"Falló la sincronización después de {SYNC_MAX_RETRIES} intentos")
    registrar_log_sincronizacion(
        len(registros), 0, "fallido",
        f"Sin conexión después de {SYNC_MAX_RETRIES} intentos"
    )
    
    return {
        "total": len(registros),
        "exitosos": 0,
        "estado": "fallido",
    }


def sincronizar_todo(max_lotes: int = 5) -> dict:
    """
    Sincroniza registros pendientes por lotes.
    Tiene un límite seguro de max_lotes para evitar bucles en entornos locales.
    """
    total_enviados = 0
    total_exitosos = 0
    lotes_procesados = 0
    
    while lotes_procesados < max_lotes:
        resultado = sincronizar_lote()
        total_enviados += resultado["total"]
        total_exitosos += resultado["exitosos"]
        lotes_procesados += 1
        
        if resultado["total"] == 0 or resultado["estado"] == "fallido":
            break
    
    return {
        "total_enviados": total_enviados,
        "total_exitosos": total_exitosos,
        "lotes_procesados": lotes_procesados,
        "estado": "completado" if total_exitosos == total_enviados else "parcial",
    }


if __name__ == "__main__":
    logger.info("Iniciando sincronización...")
    resultado = sincronizar_todo()
    logger.info(f"Resultado: {resultado}")
