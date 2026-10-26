"""
Motor RAG (Retrieval-Augmented Generation) para AmazonIA 4.0.
Combina la telemetría en tiempo real desde SQLite con el conocimiento vectorial de Qdrant
para generar diagnósticos y recomendaciones agroclimáticas inteligentes.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from src.database import obtener_lecturas_filtradas, obtener_ultimas_lecturas
from src.rag.vector_store import AmazonIAVectorStore
from src.config import UMBRALES_ALERTA

logger = logging.getLogger("amazonia.rag.engine")

# Instancia singleton del vector store
_vector_store_instance: Optional[AmazonIAVectorStore] = None


def obtener_vector_store() -> AmazonIAVectorStore:
    global _vector_store_instance
    if _vector_store_instance is None:
        _vector_store_instance = AmazonIAVectorStore()
        # Asegurar que los documentos estén indexados
        _vector_store_instance.indexar_documentos()
    return _vector_store_instance


def extraer_contexto_telemetria(estacion_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Extrae un resumen cuantitativo del estado actual de las estaciones en SQLite:
    - Última lectura
    - Acumulados y promedios en las últimas 24 horas
    - Alertas vigentes
    """
    fecha_hace_24h = (datetime.now() - timedelta(hours=24)).isoformat()
    lecturas_24h = obtener_lecturas_filtradas(
        estacion_id=estacion_id if estacion_id and estacion_id != "Todas" else None,
        fecha_inicio=fecha_hace_24h,
        limite=500,
    )

    if not lecturas_24h:
        # Fallback a las últimas disponibles
        lecturas_24h = obtener_ultimas_lecturas(estacion_id=estacion_id, limite=20)

    if not lecturas_24h:
        return {
            "estacion": estacion_id or "General",
            "registros_analizados": 0,
            "ultima_lectura": None,
            "resumen_24h": None,
            "alertas_activas": [],
        }

    ultima = lecturas_24h[0]
    temps = [r["temperatura"] for r in lecturas_24h if r.get("temperatura") is not None]
    humedades = [r["humedad"] for r in lecturas_24h if r.get("humedad") is not None]
    lluvias = [r["precipitacion"] for r in lecturas_24h if r.get("precipitacion") is not None]
    vientos = [r["viento"] for r in lecturas_24h if r.get("viento") is not None]
    baterias = [r["voltaje_bateria"] for r in lecturas_24h if r.get("voltaje_bateria") is not None]

    alertas = []
    lluvia_total = sum(lluvias) if lluvias else 0.0
    if lluvia_total > UMBRALES_ALERTA["precipitacion_intensa"]:
        alertas.append(f"Precipitación intensa acumulada ({lluvia_total:.1f} mm)")

    if temps and max(temps) > UMBRALES_ALERTA["calor_extremo"]:
        alertas.append(f"Picos de calor extremo ({max(temps):.1f} °C)")

    if baterias and baterias[0] < UMBRALES_ALERTA["bateria_baja"]:
        alertas.append(f"Batería crítica en estación ({baterias[0]:.2f} V)")

    if vientos and max(vientos) > UMBRALES_ALERTA["viento_fuerte"]:
        alertas.append(f"Ráfagas fuertes de viento ({max(vientos):.1f} km/h)")

    return {
        "estacion": ultima.get("estacion_id", "Todas"),
        "timestamp_ultimo": ultima.get("timestamp"),
        "registros_analizados": len(lecturas_24h),
        "ultima_lectura": {
            "temperatura": ultima.get("temperatura"),
            "humedad": ultima.get("humedad"),
            "presion": ultima.get("presion"),
            "precipitacion": ultima.get("precipitacion"),
            "viento": ultima.get("viento"),
            "bateria": ultima.get("voltaje_bateria"),
            "rssi": ultima.get("rssi"),
            "calidad": ultima.get("estado_calidad"),
        },
        "resumen_24h": {
            "temp_media": round(sum(temps) / len(temps), 1) if temps else None,
            "temp_max": max(temps) if temps else None,
            "temp_min": min(temps) if temps else None,
            "humedad_media": round(sum(humedades) / len(humedades), 1) if humedades else None,
            "lluvia_acumulada_mm": round(lluvia_total, 1),
            "viento_max_kmh": max(vientos) if vientos else None,
            "bateria_actual_v": baterias[0] if baterias else None,
        },
        "alertas_activas": alertas,
    }


def _generar_respuesta_experta_offline(
    pregunta: str,
    contexto_telemetria: Dict[str, Any],
    documentos: List[Dict[str, Any]],
) -> str:
    """
    Motor determinista offline de razonamiento agroclimático.
    Genera respuestas estructuradas correlacionando sensores reales con las guías de Qdrant.
    """
    p_lower = pregunta.lower()
    resumen = contexto_telemetria.get("resumen_24h") or {}
    ultima = contexto_telemetria.get("ultima_lectura") or {}
    estacion = contexto_telemetria.get("estacion", "Caroní")
    alertas = contexto_telemetria.get("alertas_activas", [])

    lluvia_24h = resumen.get("lluvia_acumulada_mm", 0.0)
    temp_media = resumen.get("temp_media", 28.0)
    temp_max = resumen.get("temp_max", 32.0)
    viento_max = resumen.get("viento_max_kmh", 10.0)
    bat_val = resumen.get("bateria_actual_v", 3.8)

    doc_titulos = ", ".join([f"*{d['titulo']}*" for d in documentos]) if documentos else "Manuales locales"

    # Caso 1: Preguntas sobre siembra / agricultura / cultivos (yuca, maíz, plátano)
    if any(k in p_lower for k in ["sembr", "yuca", "maiz", "maíz", "cultiv", "platano", "plátano", "cosech", "agrícol", "agricol"]):
        lineas = [
            f"### 🌾 Diagnóstico Agroecológico para {estacion}",
            f"Basado en los datos en vivo de los sensores y la guía de *{doc_titulos}*:\n",
            f"**Condiciones observadas en las últimas 24 horas:**",
            f"- **Precipitación acumulada:** `{lluvia_24h} mm`",
            f"- **Temperatura media:** `{temp_media} °C` (Máx: `{temp_max} °C`)",
            f"- **Ráfagas de viento:** `{viento_max} km/h`\n",
        ]

        if lluvia_24h > 50.0:
            lineas.append(
                "⚠️ **Alerta para Yuca y Raíces:** Las precipitaciones superan los 50 mm en 24h. "
                "Según el manual, suelos saturados causan asfixia radicular y pudrición bacteriana (*Erwinia*). "
                "**Recomendación:** No sembrar en terrenos bajos y habilitar zanjas de sangría para drenar el agua."
            )
        elif lluvia_24h < 5.0 and temp_max > 36.0:
            lineas.append(
                "☀️ **Déficit hídrico y calor:** Las temperaturas elevadas y la baja precipitación comprometen la floración del maíz. "
                "**Recomendación:** Aplicar riego de auxilio en horas tempranas de la mañana."
            )
        else:
            lineas.append(
                "✅ **Condición Favorable de Siembra:** El balance de humedad y temperatura actual se encuentra dentro "
                "del rango óptimo de germinación para maíz comunitario y establecimiento de estacas de yuca en la cuenca del Caroní."
            )

        if viento_max > 40.0:
            lineas.append(
                f"\n💨 **Precaución en Platanales:** Se registraron vientos de `{viento_max} km/h`. "
                "El follaje de plátano y topocho es susceptible a desgarro a partir de 40 km/h."
            )

        return "\n".join(lineas)

    # Caso 2: Inundación / Lluvias / Alertas / Crecidas
    elif any(k in p_lower for k in ["inunda", "lluvi", "crecid", "alerta", "riesgo", "rio", "río", "evacua"]):
        nivel = "🟢 Verde (Normal)"
        if lluvia_24h > 50.0:
            nivel = "🔴 Rojo (Alerta Máxima / Riesgo de Desborde)"
        elif lluvia_24h >= 30.0:
            nivel = "🟡 Amarillo (Prevención y Vigilancia)"

        lineas = [
            f"### 🚨 Evaluación Hidrometeorológica — Protocolo Comunitario",
            f"**Nivel de Alerta Actual:** **{nivel}**\n",
            f"- **Precipitación registrada en 24h:** `{lluvia_24h} mm` (Estación: {estacion})",
            f"- **Umbral de Alerta Temprana:** > 50.0 mm en 24h activa comité de contingencia.",
        ]

        if nivel.startswith("🔴"):
            lineas.extend([
                "\n**Acciones Inmediatas Requeridas:**",
                "1. Activar brigada de emergencia comunitaria y evacuar conucos en planicies de inundación.",
                "2. Retirar motobombas y enseres de orillas de ríos y caños secundarios.",
                "3. Suspender el tránsito fluvial en embarcaciones ligeras por incremento de palizadas.",
            ])
        elif nivel.startswith("🟡"):
            lineas.extend([
                "\n**Acciones Preventivas:**",
                "1. Limpieza de zanjas y cañadas comunitarias para evitar encharcamientos.",
                "2. Resguardo de semillas y herramientas en trojas elevadas.",
            ])
        else:
            lineas.append(
                "\n✅ **Situación Estable:** No hay indicios de anegamiento ni crecidas en las microcuencas tributarias del Caroní."
            )

        return "\n".join(lineas)

    # Caso 3: Estado de salud de estaciones, batería y señal
    elif any(k in p_lower for k in ["bater", "batería", "salud", "sensor", "manten", "rssi", "señal", "calidad"]):
        lineas = [
            f"### 🔧 Diagnóstico Técnico de Estación {estacion}",
            f"- **Voltaje actual de batería:** `{bat_val} V`",
            f"- **Calidad de señal (RSSI):** `{ultima.get('rssi', 'N/A')} dBm`",
            f"- **Estado de validación de datos:** `{ultima.get('calidad', 'N/A')}`\n",
        ]

        if bat_val < 3.2:
            lineas.append(
                "🚨 **Batería Crítica (< 3.2 V):** La celda de litio está en umbral de desconexión. "
                "Revisar de inmediato suciedad en el panel solar o fallas en el circuito de carga TP4056."
            )
        elif bat_val < 3.6:
            lineas.append(
                "⚠️ **Batería Baja (3.2 V - 3.5 V):** Estado operativo reducido por días nublados o sombreado matutino."
            )
        else:
            lineas.append("✅ **Alimentación Óptima:** El sistema fotovoltaico mantiene niveles saludables (> 3.7 V).")

        return "\n".join(lineas)

    # Caso 4: Consulta general informada por RAG
    else:
        lineas = [
            f"### 🌿 Asistente Comunitario AmazonIA 4.0",
            f"Para tu consulta: *\"{pregunta}\"*:\n",
            f"**Telemetría actual en {estacion}:** Temperatura `{ultima.get('temperatura', 'N/A')} °C`, Humedad `{ultima.get('humedad', 'N/A')} %`, Lluvia 24h `{lluvia_24h} mm`.",
            f"\n**Información recuperada de la base de conocimiento ({doc_titulos}):**\n",
        ]
        for d in documentos[:2]:
            lineas.append(f"> **{d['titulo']}:**\n> {d['texto'][:300]}...\n")

        lineas.append("¿Deseas profundizar sobre un cultivo en particular, el protocolo de inundación o el estado técnico?")
        return "\n".join(lineas)


def responder_consulta_rag(
    pregunta: str,
    estacion_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Función principal del pipeline RAG:
    1. Recupera la telemetría viva de SQLite.
    2. Busca documentos semánticamente relevantes en Qdrant.
    3. Construye el prompt contextualizado y genera la respuesta.
    """
    contexto_telemetria = extraer_contexto_telemetria(estacion_id)
    vs = obtener_vector_store()
    documentos_recuperados = vs.buscar_similares(pregunta, top_k=3)

    api_key_openai = os.getenv("OPENAI_API_KEY", "").strip()
    motor_usado = "Motor Agroclimático Offline (Reglas + Qdrant)"
    respuesta_final = ""

    # Intentar OpenAI si hay API Key válida
    if api_key_openai and not api_key_openai.startswith("your_"):
        try:
            import openai
            client_ai = openai.OpenAI(api_key=api_key_openai)

            contexto_prompt = f"""
Eres AmazonIA 4.0, el asistente agroclimático inteligente de la cuenca del río Caroní (Venezuela).
Responde con tono técnico, claro y orientado a la comunidad agrícola e indígena.

TELEMETRÍA EN TIEMPO REAL (SENSORES EDGE):
- Estación: {contexto_telemetria.get('estacion')}
- Última lectura: {json.dumps(contexto_telemetria.get('ultima_lectura'))}
- Resumen 24 horas: {json.dumps(contexto_telemetria.get('resumen_24h'))}
- Alertas activas: {contexto_telemetria.get('alertas_activas')}

CONOCIMIENTO TÉCNICO Y MANUALES DE LA CUENCA (RECUPERADOS DE QDRANT):
"""
            for i, doc in enumerate(documentos_recuperados, 1):
                contexto_prompt += f"\n[Documento {i} - {doc['titulo']}]:\n{doc['texto']}\n"

            chat_res = client_ai.chat.completions.create(
                model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": contexto_prompt},
                    {"role": "user", "content": pregunta},
                ],
                temperature=0.3,
                max_tokens=600,
            )
            respuesta_final = chat_res.choices[0].message.content
            motor_usado = f"OpenAI ({os.getenv('OPENAI_MODEL', 'gpt-4o-mini')}) + Qdrant"
        except Exception as e:
            logger.warning("Fallo al consultar OpenAI (%s). Usando motor offline determinista.", e)
            respuesta_final = _generar_respuesta_experta_offline(
                pregunta, contexto_telemetria, documentos_recuperados
            )
    else:
        respuesta_final = _generar_respuesta_experta_offline(
            pregunta, contexto_telemetria, documentos_recuperados
        )

    return {
        "pregunta": pregunta,
        "respuesta": respuesta_final,
        "estacion": contexto_telemetria.get("estacion"),
        "telemetria": contexto_telemetria,
        "documentos": documentos_recuperados,
        "motor": motor_usado,
    }
