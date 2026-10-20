"""
Publicador MQTT para estaciones meteorológicas de AmazonIA 4.0.
Publica telemetría microclimática hacia el broker Eclipse Mosquitto.
"""
import json
import logging
import time
import argparse
from typing import Optional, Dict, Any
from datetime import datetime

import paho.mqtt.client as mqtt

from src.config import (
    MQTT_BROKER,
    MQTT_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MQTT_TOPIC_PREFIX,
    MQTT_CLIENT_ID_PUB,
    MQTT_KEEPALIVE,
    MQTT_QOS,
    ESTACIONES_PILOTO,
)
from src.simulador import (
    generar_lectura_valida,
    generar_lectura_invalida,
    generar_lectura_incompleta,
)

logger = logging.getLogger("amazonia.mqtt_publicador")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def obtener_cliente_mqtt(client_id: Optional[str] = None) -> mqtt.Client:
    """
    Crea e inicializa un cliente MQTT compatible con paho-mqtt v1 y v2.
    """
    cid = client_id or f"{MQTT_CLIENT_ID_PUB}-{int(time.time())}"
    
    # Compatibilidad paho-mqtt v2 vs v1
    if hasattr(mqtt, "CallbackAPIVersion"):
        cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
    else:
        cliente = mqtt.Client(client_id=cid)

    if MQTT_USERNAME:
        cliente.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(client, userdata, flags, rc, properties=None):
        codigo = getattr(rc, "value", rc)
        if codigo == 0:
            logger.info("Conectado exitosamente al broker MQTT (%s:%s)", MQTT_BROKER, MQTT_PORT)
        else:
            logger.error("Fallo al conectar al broker MQTT. Código: %s", codigo)

    def on_disconnect(client, userdata, disconnect_flags_or_rc, reason_code=None, properties=None):
        logger.warning("Desconectado del broker MQTT")

    cliente.on_connect = on_connect
    cliente.on_disconnect = on_disconnect
    return cliente


def publicar_telemetria(
    cliente: mqtt.Client,
    lectura: Dict[str, Any],
    topico: Optional[str] = None,
    qos: int = MQTT_QOS,
) -> bool:
    """
    Publica una lectura meteorológica en el tópico correspondiente.
    Tópico por defecto: {MQTT_TOPIC_PREFIX}/{estacion_id}/telemetria
    """
    estacion_id = lectura.get("estacion_id", "DESCONOCIDA")
    target_topic = topico or f"{MQTT_TOPIC_PREFIX}/{estacion_id}/telemetria"
    payload = json.dumps(lectura)

    info = cliente.publish(target_topic, payload=payload, qos=qos)
    info.wait_for_publish(timeout=5.0)

    if info.is_published():
        logger.info("[%s] Publicado en %s (QoS %d)", estacion_id, target_topic, qos)
        return True
    else:
        logger.error("[%s] Error publicando mensaje en %s", estacion_id, target_topic)
        return False


def simular_envio_estacion(
    estacion_id: str,
    broker: str = MQTT_BROKER,
    port: int = MQTT_PORT,
    intervalo_segundos: int = 10,
    max_iteraciones: Optional[int] = None,
):
    """
    Bucle de simulación para publicar lecturas continuas vía MQTT.
    """
    cliente = obtener_cliente_mqtt(f"{MQTT_CLIENT_ID_PUB}-{estacion_id}")
    logger.info("Iniciando simulador MQTT para estación %s hacia %s:%d", estacion_id, broker, port)
    
    try:
        cliente.connect(broker, port, keepalive=MQTT_KEEPALIVE)
        cliente.loop_start()
    except Exception as e:
        logger.error("No se pudo conectar a Mosquitto en %s:%d: %s", broker, port, e)
        return

    iteracion = 0
    try:
        while max_iteraciones is None or iteracion < max_iteraciones:
            # Distribución: 80% válidas, 10% inválidas, 10% incompletas
            import random
            r = random.random()
            if r < 0.8:
                lectura = generar_lectura_valida(estacion_id)
            elif r < 0.9:
                lectura = generar_lectura_invalida(estacion_id)
            else:
                lectura = generar_lectura_incompleta(estacion_id)

            publicar_telemetria(cliente, lectura)
            iteracion += 1
            time.sleep(intervalo_segundos)
    except KeyboardInterrupt:
        logger.info("Deteniendo simulación MQTT para %s...", estacion_id)
    finally:
        cliente.loop_stop()
        cliente.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publicador MQTT de Telemetría AmazonIA 4.0")
    parser.add_argument("--estacion", type=str, default=ESTACIONES_PILOTO[0], help="ID de estación")
    parser.add_argument("--broker", type=str, default=MQTT_BROKER, help="Host broker MQTT")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="Puerto broker MQTT")
    parser.add_argument("--intervalo", type=int, default=5, help="Intervalo entre publicaciones en segundos")
    parser.add_argument("--max", type=int, default=None, help="Número máximo de publicaciones")
    args = parser.parse_args()

    simular_envio_estacion(
        estacion_id=args.estacion,
        broker=args.broker,
        port=args.port,
        intervalo_segundos=args.intervalo,
        max_iteraciones=args.max,
    )
