"""
Módulo auxiliar para la sincronización desde el Edge Gateway.
"""
import requests
import logging
from src.database import obtener_no_sincronizados, marcar_como_sincronizados
from src.config import SYNC_ENDPOINT, SYNC_BATCH_SIZE

logger = logging.getLogger(__name__)


def sincronizar():
    """
    Sincroniza los datos del buffer local del Edge Gateway.
    Retorna un diccionario con el resultado.
    """
    registros = obtener_no_sincronizados(SYNC_BATCH_SIZE)
    
    if not registros:
        return {"total": 0, "validos": 0}
    
    payload = {"datos": registros}
    
    try:
        response = requests.post(SYNC_ENDPOINT, json=payload, timeout=30)
        response.raise_for_status()
        
        ids = [r["id"] for r in registros]
        marcar_como_sincronizados(ids)
        
        return {
            "total": len(registros),
            "validos": len(registros),
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de sincronización: {e}")
        return {"total": len(registros), "validos": 0}