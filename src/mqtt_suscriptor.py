"""
Suscriptor y Bridge MQTT para AmazonIA 4.0.
Escucha mensajes de telemetría desde Mosquitto, ejecuta validación de calidad de datos,
persiste en SQLite (Offline-First) y republica alertas en tiempo real.
"""
import json
import logging
import argparse
from typing import Optional, Dict, Any, Tuple

import paho.mqtt.client as mqtt

from src.config import (
    MQTT_BROKER,
    MQTT_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MQTT_TOPIC_PREFIX,
    MQTT_CLIENT_ID_SUB,
    MQTT_KEEPALIVE,
    MQTT_QOS,
)
from src.validacion import validar_telemetria, verificar_alertas
from src.database import insertar_telemetria

logger = logging.getLogger("amazonia.mqtt_suscriptor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def procesar_mensaje_telemetria(
    payload_str: str,
    topico: Optional[str] = None
) -> Tuple[bool, Optional[int], str, list]:
    """
    Procesa un mensaje JSON crudo:
    1. Deserializa JSON
    2. Ejecuta motor de validación de calidad
    3. Persiste en SQLite
    4. Evalúa umbrales de alerta
    Retorna: (exito, id_registro, estado_calidad, alertas)
    """
    try:
        datos = json.loads(payload_str)
    except json.JSONDecodeError as e:
        logger.error("Error al decodificar JSON recibido: %s", e)
        return False, None, "error_json", []

    # Validar campos obligatorios básicos
    if not isinstance(datos, dict) or "estacion_id" not in datos or "timestamp" not in datos:
        logger.warning("Mensaje ignorado: faltan campos obligatorios (estacion_id, timestamp)")
        return False, None, "formato_invalido", []

    resultado_validacion = validar_telemetria(datos)
    estado_calidad = resultado_validacion.get("estado", "invalid")
    errores = resultado_validacion.get("errores")

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
        "estado_calidad": estado_calidad,
        "errores": errores,
    }

    alertas = verificar_alertas(datos)

    try:
        registro_id = insertar_telemetria(registro)
        logger.info(
            "[%s] Guardado en DB (ID: %d | Calidad: %s | Alertas: %d)",
            datos["estacion_id"],
            registro_id,
            estado_calidad,
            len(alertas),
        )
        return True, registro_id, estado_calidad, alertas
    except Exception as e:
        logger.error("Error al persistir en SQLite: %s", e)
        return False, None, estado_calidad, alertas


def iniciar_bridge_mqtt(
    broker: str = MQTT_BROKER,
    port: int = MQTT_PORT,
    topic_prefix: str = MQTT_TOPIC_PREFIX,
):
    """
    Inicia el bucle de escucha del suscriptor MQTT conectándose a Mosquitto.
    """
    if hasattr(mqtt, "CallbackAPIVersion"):
        cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID_SUB)
    else:
        cliente = mqtt.Client(client_id=MQTT_CLIENT_ID_SUB)

    if MQTT_USERNAME:
        cliente.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    topic_telemetria = f"{topic_prefix}/+/telemetria"

    def on_connect(client, userdata, flags, rc, properties=None):
        codigo = getattr(rc, "value", rc)
        if codigo == 0:
            logger.info("Bridge conectado a Mosquitto (%s:%s)", broker, port)
            client.subscribe(topic_telemetria, qos=MQTT_QOS)
            logger.info("Suscrito al tópico: %s", topic_telemetria)
        else:
            logger.error("Fallo al conectar al broker Mosquitto. Código: %s", codigo)

    def on_message(client, userdata, msg):
        payload_str = msg.payload.decode("utf-8")
        exito, registro_id, estado_calidad, alertas = procesar_mensaje_telemetria(
            payload_str, topico=msg.topic
        )

        # Si se detectaron alertas críticas, republicar al tópico de alertas
        if exito and alertas:
            try:
                datos = json.loads(payload_str)
                estacion_id = datos.get("estacion_id", "DESCONOCIDA")
                alerta_payload = json.dumps({
                    "estacion_id": estacion_id,
                    "registro_id": registro_id,
                    "timestamp": datos.get("timestamp"),
                    "alertas": alertas,
                })
                topic_alertas = f"{topic_prefix}/{estacion_id}/alertas"
                client.publish(topic_alertas, alerta_payload, qos=MQTT_QOS)
                logger.warning("[%s] ¡Alerta republicada en %s!: %s", estacion_id, topic_alertas, alertas)
            except Exception as e:
                logger.error("Error republicando alertas: %s", e)

    cliente.on_connect = on_connect
    cliente.on_message = on_message

    logger.info("Iniciando servicio Bridge MQTT en %s:%d...", broker, port)
    try:
        cliente.connect(broker, port, keepalive=MQTT_KEEPALIVE)
        cliente.loop_forever()
    except KeyboardInterrupt:
        logger.info("Deteniendo Bridge MQTT...")
    finally:
        cliente.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bridge MQTT a SQLite para AmazonIA 4.0")
    parser.add_argument("--broker", type=str, default=MQTT_BROKER, help="Host broker MQTT")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="Puerto broker MQTT")
    parser.add_argument("--prefix", type=str, default=MQTT_TOPIC_PREFIX, help="Prefijo de tópicos")
    args = parser.parse_args()

    iniciar_bridge_mqtt(broker=args.broker, port=args.port, topic_prefix=args.prefix)
