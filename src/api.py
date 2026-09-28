"""
API de ingesta para el Edge Gateway.
Recibe telemetría, valida calidad y almacena en SQLite.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

from src.database import (
    init_database, insertar_telemetria, obtener_no_sincronizados,
    marcar_como_sincronizados, obtener_ultimas_lecturas,
    obtener_estadisticas_calidad, obtener_estaciones,
)
from src.validacion import validar_telemetria, verificar_alertas

app = FastAPI(
    title="AmazonIA 4.0 - API de Ingesta",
    description="API para la ingesta de telemetría microclimática en el borde",
    version="1.0.0",
)


class TelemetriaInput(BaseModel):
    estacion_id: str = Field(..., example="CARONI-001")
    timestamp: str = Field(..., example="2026-09-15T10:30:00")
    temperatura: Optional[float] = Field(None, example=28.5)
    humedad: Optional[float] = Field(None, example=75.0)
    presion: Optional[float] = Field(None, example=1013.25)
    precipitacion: Optional[float] = Field(None, example=0.0)
    viento: Optional[float] = Field(None, example=5.2)
    voltaje_bateria: Optional[float] = Field(None, example=3.8)
    rssi: Optional[int] = Field(None, example=-85)


class BatchInput(BaseModel):
    datos: List[TelemetriaInput]


@app.get("/")
async def root():
    return {
        "nombre": "AmazonIA 4.0 - API de Ingesta",
        "version": "1.0.0",
        "documentacion": "/docs",
        "health": "/health",
    }


@app.on_event("startup")
async def startup_event():
    """Inicializa la base de datos al arrancar la API."""
    init_database()


@app.get("/health")
async def health_check():
    """Endpoint de verificación de salud del sistema."""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.post("/ingesta")
async def recibir_telemetria(data: TelemetriaInput):
    """
    Recibe un registro individual de telemetría.
    Valida calidad y almacena en SQLite.
    """
    datos = data.model_dump()
    
    # Validar
    resultado = validar_telemetria(datos)
    
    # Preparar registro para inserción
    registro = {
        "estacion_id": datos["estacion_id"],
        "timestamp": datos["timestamp"],
        "temperatura": datos.get("temperatura"),
        "humedad": datos.get("humedad"),
        "presion": datos.get("presion"),
        "precipitacion": datos.get("precipitacion"),
        "viento": datos.get("viento"),
        "voltaje_bateria": datos.get("voltaje_bateria"),
        "rssi": datos.get("rssi"),
        "estado_calidad": resultado["estado"],
        "errores": resultado["errores"],
    }
    
    registro_id = insertar_telemetria(registro)
    
    # Verificar alertas
    alertas = verificar_alertas(datos)
    
    return {
        "id": registro_id,
        "estado_calidad": resultado["estado"],
        "errores": resultado["errores"],
        "alertas": alertas,
    }


@app.post("/ingesta/batch")
async def recibir_telemetria_batch(data: BatchInput):
    """
    Recibe un lote de registros de telemetría.
    Útil para la sincronización desde el Edge Gateway.
    """
    resultados = []
    
    for item in data.datos:
        datos = item.model_dump()
        resultado = validar_telemetria(datos)
        
        registro = {
            "estacion_id": datos["estacion_id"],
            "timestamp": datos["timestamp"],
            "temperatura": datos.get("temperatura"),
            "humedad": datos.get("humedad"),
            "presion": datos.get("presion"),
            "precipitacion": datos.get("precipitacion"),
            "viento": datos.get("viento"),
            "voltaje_bateria": datos.get("voltaje_bateria"),
            "rssi": datos.get("rssi"),
            "estado_calidad": resultado["estado"],
            "errores": resultado["errores"],
        }
        
        registro_id = insertar_telemetria(registro)
        resultados.append({
            "id": registro_id,
            "estado_calidad": resultado["estado"],
        })
    
    return {
        "total": len(resultados),
        "validos": sum(1 for r in resultados if r["estado_calidad"] == "valid"),
        "invalidos": sum(1 for r in resultados if r["estado_calidad"] == "invalid"),
        "incompletos": sum(1 for r in resultados if r["estado_calidad"] == "incomplete"),
    }


@app.get("/telemetria/ultimas")
async def ultimas_lecturas(estacion_id: Optional[str] = None, limite: int = 100):
    """Obtiene las últimas lecturas para el dashboard."""
    return obtener_ultimas_lecturas(estacion_id, limite)


@app.get("/telemetria/estadisticas")
async def estadisticas_calidad():
    """Obtiene estadísticas de calidad de datos."""
    return obtener_estadisticas_calidad()


@app.get("/telemetria/estaciones")
async def listar_estaciones():
    """Obtiene la lista de estaciones registradas."""
    return obtener_estaciones()


@app.get("/sincronizacion/pendientes")
async def pendientes_sincronizacion(limite: int = 100):
    """Obtiene registros pendientes de sincronización."""
    return obtener_no_sincronizados(limite)