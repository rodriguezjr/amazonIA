"""
Motor de validación de calidad de datos.
Implementa el marco de Data Quality de Buelvas et al. (2023).
Asigna etiquetas: valid, invalid, incomplete.
"""
from datetime import datetime
from src.config import RANGOS_VALIDACION, UMBRALES_ALERTA


def validar_telemetria(datos: dict) -> dict:
    """
    Valida un registro de telemetría y retorna el resultado.
    
    Returns:
        dict: {
            "estado": "valid" | "invalid" | "incomplete",
            "errores": str | None,
            "datos_limpios": dict
        }
    """
    errores = []
    datos_limpios = {}
    
    # 1. Verificar campos obligatorios
    campos_obligatorios = ["estacion_id", "timestamp"]
    for campo in campos_obligatorios:
        if not datos.get(campo):
            errores.append(f"Campo obligatorio faltante: {campo}")
    
    if errores:
        return {
            "estado": "invalid",
            "errores": "; ".join(errores),
            "datos_limpios": datos,
        }
    
    # 2. Validar cada variable contra su rango físico
    variables_numericas = [
        "temperatura", "humedad", "presion",
        "precipitacion", "viento", "voltaje_bateria", "rssi"
    ]
    
    for variable in variables_numericas:
        valor = datos.get(variable)
        
        if valor is None:
            # Campo ausente: marcar como incomplete
            errores.append(f"Campo ausente: {variable}")
            continue
        
        # Verificar tipo numérico
        try:
            valor_num = float(valor)
        except (TypeError, ValueError):
            errores.append(f"Tipo inválido en {variable}: {valor}")
            continue
        
        # Verificar rango
        rango = RANGOS_VALIDACION.get(variable)
        if rango:
            if not (rango["min"] <= valor_num <= rango["max"]):
                errores.append(
                    f"{variable} fuera de rango: {valor_num} "
                    f"(esperado {rango['min']}-{rango['max']} {rango['unidad']})"
                )
                continue
        
        datos_limpios[variable] = valor_num
    
    # 3. Determinar estado de calidad
    if not errores:
        estado = "valid"
        errores_str = None
    elif any("fuera de rango" in e or "Tipo inválido" in e for e in errores):
        estado = "invalid"
        errores_str = "; ".join(errores)
    else:
        estado = "incomplete"
        errores_str = "; ".join(errores)
    
    return {
        "estado": estado,
        "errores": errores_str,
        "datos_limpios": datos_limpios,
    }


def verificar_alertas(registro: dict) -> list:
    """
    Verifica si un registro supera los umbrales de alerta.
    Retorna una lista de alertas activadas.
    """
    alertas = []
    
    if registro.get("precipitacion", 0) > UMBRALES_ALERTA["precipitacion_intensa"]:
        alertas.append({
            "tipo": "lluvia_intensa",
            "mensaje": f"Precipitación intensa: {registro['precipitacion']} mm",
            "severidad": "alta",
        })
    
    if registro.get("temperatura", 0) > UMBRALES_ALERTA["calor_extremo"]:
        alertas.append({
            "tipo": "calor_extremo",
            "mensaje": f"Calor extremo: {registro['temperatura']} °C",
            "severidad": "alta",
        })
    
    if registro.get("voltaje_bateria", 5) < UMBRALES_ALERTA["bateria_baja"]:
        alertas.append({
            "tipo": "bateria_baja",
            "mensaje": f"Batería baja: {registro['voltaje_bateria']} V",
            "severidad": "media",
        })
    
    if registro.get("viento", 0) > UMBRALES_ALERTA["viento_fuerte"]:
        alertas.append({
            "tipo": "viento_fuerte",
            "mensaje": f"Viento fuerte: {registro['viento']} km/h",
            "severidad": "media",
        })
    
    return alertas