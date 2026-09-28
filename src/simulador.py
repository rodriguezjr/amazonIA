"""
Simulador de telemetría para estaciones meteorológicas comunitarias.
Genera datos sintéticos realistas para CARONI-001 y CARONI-002.
"""
import random
import time
import requests
from datetime import datetime, timedelta
from src.config import API_HOST, API_PORT, ESTACIONES_PILOTO

API_URL = f"http://{API_HOST}:{API_PORT}/ingesta"


def generar_lectura_valida(estacion_id: str) -> dict:
    """Genera una lectura válida con valores realistas."""
    hora = datetime.now().hour
    
    # Temperatura: más cálida de día, más fresca de noche
    temp_base = 26 + 6 * (1 if 6 <= hora <= 18 else -1)
    temperatura = round(temp_base + random.uniform(-3, 3), 1)
    
    # Humedad: inversamente proporcional a la temperatura
    humedad = round(random.uniform(60, 95), 1)
    
    # Presión: rango típico de la cuenca
    presion = round(random.uniform(1005, 1020), 2)
    
    # Precipitación: probabilidad de lluvia
    precipitacion = round(random.uniform(0, 10), 1) if random.random() < 0.3 else 0.0
    
    # Viento
    viento = round(random.uniform(0, 25), 1)
    
    # Batería: simulando descarga gradual
    voltaje_bateria = round(random.uniform(3.5, 4.1), 2)
    
    # RSSI: calidad de señal
    rssi = random.randint(-100, -60)
    
    return {
        "estacion_id": estacion_id,
        "timestamp": datetime.now().isoformat(),
        "temperatura": temperatura,
        "humedad": humedad,
        "presion": presion,
        "precipitacion": precipitacion,
        "viento": viento,
        "voltaje_bateria": voltaje_bateria,
        "rssi": rssi,
    }


def generar_lectura_invalida(estacion_id: str) -> dict:
    """Genera una lectura con valores fuera de rango."""
    lectura = generar_lectura_valida(estacion_id)
    
    # Introducir un error aleatorio
    error = random.choice(["temperatura", "humedad", "presion"])
    
    if error == "temperatura":
        lectura["temperatura"] = random.choice([150.0, -50.0, 99.9])
    elif error == "humedad":
        lectura["humedad"] = random.choice([-10.0, 150.0])
    elif error == "presion":
        lectura["presion"] = random.choice([500.0, 2000.0])
    
    return lectura


def generar_lectura_incompleta(estacion_id: str) -> dict:
    """Genera una lectura con campos faltantes."""
    lectura = generar_lectura_valida(estacion_id)
    
    # Eliminar un campo aleatorio
    campo = random.choice(["humedad", "presion", "viento"])
    del lectura[campo]
    
    return lectura


def enviar_lectura(lectura: dict) -> dict:
    """Envía una lectura a la API de ingesta."""
    try:
        response = requests.post(API_URL, json=lectura, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"No se pudo conectar a la API en {API_URL}. ¿Está corriendo uvicorn?"}
    except requests.exceptions.Timeout:
        return {"error": "Timeout al conectar con la API"}
    except requests.exceptions.HTTPError as e:
        return {"error": f"HTTP {e.response.status_code}: {e.response.text}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def simular_estacion(estacion_id: str, intervalo_segundos: int = 10, max_iteraciones: int = None):
    """
    Simula el envío continuo de datos desde una estación.
    """
    print(f"[SIMULADOR] Iniciando estación {estacion_id}")
    print(f"[SIMULADOR] Enviando datos a: {API_URL}")
    iteracion = 0
    
    while max_iteraciones is None or iteracion < max_iteraciones:
        # Distribución de escenarios: 80% válidos, 10% inválidos, 10% incompletos
        r = random.random()
        if r < 0.8:
            lectura = generar_lectura_valida(estacion_id)
        elif r < 0.9:
            lectura = generar_lectura_invalida(estacion_id)
        else:
            lectura = generar_lectura_incompleta(estacion_id)
        
        resultado = enviar_lectura(lectura)
        
        if "error" in resultado:
            print(f"[{estacion_id}] {lectura['timestamp']} → ERROR: {resultado['error']}")
        else:
            estado = resultado.get("estado_calidad", "desconocido")
            alertas = resultado.get("alertas", [])
            alertas_str = f" | {len(alertas)} alerta(s)" if alertas else ""
            print(f"[{estacion_id}] {lectura['timestamp']} → {estado}{alertas_str}")
        
        iteracion += 1
        time.sleep(intervalo_segundos)


if __name__ == "__main__":
    import threading
    
    # Simular ambas estaciones en paralelo
    hilos = []
    for estacion in ESTACIONES_PILOTO:
        hilo = threading.Thread(
            target=simular_estacion,
            args=(estacion, 10, 100),
        )
        hilos.append(hilo)
        hilo.start()
    
    for hilo in hilos:
        hilo.join()