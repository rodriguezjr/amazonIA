"""
Dashboard interactivo para AmazonIA 4.0.
Visualización de telemetría microclimática en tiempo real con pestañas especializadas
y filtros temporales (hace 1 hora, 1 día, 1 semana, 1 mes, 1 año).
"""
import sys
from pathlib import Path

# Agregar la raíz del proyecto al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

# ⚠️ IMPORTANTE: set_page_config DEBE ser el primer comando de Streamlit
st.set_page_config(
    page_title="AmazonIA 4.0 - Monitoreo Microclimático",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from src.database import (
    obtener_lecturas_filtradas,
    obtener_estadisticas_calidad,
    obtener_estaciones,
)
from src.config import RANGOS_VALIDACION, UMBRALES_ALERTA
from src.rag.engine import responder_consulta_rag

# Encabezado principal
st.title("🌿 AmazonIA 4.0 — Monitoreo Microclimático en el Borde")
st.caption("Red Comunitaria de Estaciones Meteorológicas | Cuenca del Río Caroní, Edo. Bolívar")

# ==============================================================================
# SIDEBAR: FILTROS Y CONTROL TEMPORAL
# ==============================================================================
with st.sidebar:
    st.header("⚙️ Configuración y Filtros")

    # Selector de estación
    estaciones = obtener_estaciones()
    if not estaciones:
        estaciones = ["CARONI-001", "CARONI-002"]

    estacion_sel = st.selectbox(
        "📍 Estación Meteorológica",
        ["Todas"] + estaciones,
        help="Filtrar telemetría por una estación específica o ver todas combinadas.",
    )

    # Selector de ventana temporal solicitado
    opciones_tiempo = {
        "Última hora (1h)": timedelta(hours=1),
        "Últimas 24 horas (1 día)": timedelta(days=1),
        "Última semana (7 días)": timedelta(days=7),
        "Último mes (30 días)": timedelta(days=30),
        "Último año (365 días)": timedelta(days=365),
        "Todo el historial": None,
    }

    ventana_sel = st.selectbox(
        "⏱️ Ventana Temporal",
        list(opciones_tiempo.keys()),
        index=1,
        help="Selecciona desde cuándo consultar las lecturas registradas.",
    )

    delta_elegido = opciones_tiempo[ventana_sel]
    fecha_inicio_iso = (
        (datetime.now() - delta_elegido).isoformat() if delta_elegido else None
    )

    limite_datos = st.slider(
        "🔢 Límite máximo de registros",
        min_value=50,
        max_value=5000,
        value=1000,
        step=50,
        help="Máximo de lecturas a traer de la base de datos local SQLite.",
    )

    filtrar_anomalias = st.checkbox(
        "🛡️ Filtrar anomalías en gráficas (Recomendado)",
        value=True,
        help="Excluye lecturas con estado 'invalid' de las curvas y estadísticas analíticas (Mín, Máx, Media, Desv. Estándar) para evitar distorsión de escala.",
    )

    st.markdown("---")
    st.markdown("### 📡 Estado del Sistema")
    st.success("🟢 Edge Gateway (SQLite Offline-First)")
    st.info("🔄 Sincronización y MQTT activos")
    st.markdown("---")
    st.caption("AmazonIA 4.0 • UNEG")


# ==============================================================================
# CARGA Y PROCESAMIENTO DE DATOS
# ==============================================================================
@st.cache_data(ttl=10)
def cargar_telemetria(estacion_id: str, fecha_inicio: str, limite: int):
    registros = obtener_lecturas_filtradas(
        estacion_id=estacion_id,
        fecha_inicio=fecha_inicio,
        limite=limite,
    )
    if not registros:
        return pd.DataFrame()
    df_res = pd.DataFrame(registros)
    df_res["timestamp_dt"] = pd.to_datetime(df_res["timestamp"], errors="coerce")
    return df_res


df = cargar_telemetria(estacion_sel, fecha_inicio_iso, limite_datos)

if df.empty:
    st.warning(
        f"⚠️ No se encontraron lecturas registradas para la estación **'{estacion_sel}'** en la ventana **'{ventana_sel}'**."
    )
    st.info(
        "💡 Si el sistema es nuevo, ejecuta el orquestador (`python start.py`) o el simulador (`python -m src.simulador`) para generar datos."
    )
    st.stop()

# Ordenar cronológicamente para trazado coherente
df_sorted = df.sort_values("timestamp")

# Conjunto depurado para métricas estadísticas y trazado de curvas
if filtrar_anomalias:
    df_graficos = df_sorted[df_sorted["estado_calidad"] == "valid"].copy()
    if df_graficos.empty:
        df_graficos = df_sorted.copy()
else:
    df_graficos = df_sorted.copy()

# Última lectura (prioriza la última lectura válida si el filtro está activo)
ultimo = df_graficos.iloc[-1] if not df_graficos.empty else df_sorted.iloc[-1]

# ==============================================================================
# SECCIÓN 1: INDICADORES Y KPIS ACTUALES
# ==============================================================================
st.subheader("📊 Indicadores de la Última Lectura")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    val = ultimo.get("temperatura")
    st.metric(
        "🌡️ Temperatura",
        f"{val:.1f} °C" if pd.notna(val) else "N/A",
        help="Rango físico: -10 °C a 45 °C",
    )

with col2:
    val = ultimo.get("humedad")
    st.metric(
        "💧 Humedad",
        f"{val:.1f} %" if pd.notna(val) else "N/A",
        help="Humedad relativa en el ambiente",
    )

with col3:
    val = ultimo.get("precipitacion")
    st.metric(
        "🌧️ Precipitación",
        f"{val:.1f} mm" if pd.notna(val) else "N/A",
        help="Precipitación acumulada",
    )

with col4:
    val = ultimo.get("viento")
    st.metric(
        "💨 Viento",
        f"{val:.1f} km/h" if pd.notna(val) else "N/A",
        help="Velocidad del viento",
    )

with col5:
    bat = ultimo.get("voltaje_bateria")
    if pd.notna(bat):
        estado_bat = "Normal" if bat >= 3.5 else "Baja"
        st.metric("🔋 Batería", f"{bat:.2f} V", delta=estado_bat)
    else:
        st.metric("🔋 Batería", "N/A")


# ==============================================================================
# SECCIÓN 2: PESTAÑAS DEDICADAS POR SENSOR Y ASISTENTE IA
# ==============================================================================
st.markdown("---")
st.subheader("📈 Monitoreo de Variables y Asistencia Inteligente")
filtro_badge = " • 🛡️ **Filtro de anomalías activado** (Curvas y estadísticas depuradas)" if filtrar_anomalias else " • ⚠️ **Modo Crudo** (Mostrando todas las lecturas)"
st.markdown(f"*Total registros: **{len(df_sorted)}** | Válidos en gráficas: **{len(df_graficos)}** ({ventana_sel}){filtro_badge}*")

(
    tab_rag,
    tab_temp,
    tab_hum,
    tab_prec,
    tab_pres,
    tab_viento,
    tab_bat,
    tab_rssi,
    tab_comparativa,
) = st.tabs([
    "🤖 Asistente Comunitario IA (RAG)",
    "🌡️ Temperatura",
    "💧 Humedad",
    "🌧️ Precipitación",
    "⏱️ Presión",
    "💨 Viento",
    "🔋 Batería",
    "📶 Señal (RSSI)",
    "🌐 Comparativa Multivariable",
])

# ------------------------------------------------------------------------------
# TAB 0: ASISTENTE COMUNITARIO IA (RAG)
# ------------------------------------------------------------------------------
with tab_rag:
    st.markdown("### 🤖 Asistente Agroclimático y de Riesgo — RAG AmazonIA 4.0")
    st.caption(
        "Combina la telemetría viva de SQLite con la memoria semántica de la cuenca del Río Caroní indexada en Qdrant."
    )

    # Inicializar historial de chat en session_state
    if "rag_messages" not in st.session_state:
        st.session_state.rag_messages = [
            {
                "role": "assistant",
                "content": (
                    "¡Hola! Soy el asistente inteligente de **AmazonIA 4.0**. "
                    "Estoy conectado a los sensores de la cuenca del río Caroní y a los manuales agroclimáticos y protocolos comunitarios. "
                    "¿En qué puedo orientarte hoy sobre siembras, alertas hidrometeorológicas o estado de las estaciones?"
                ),
            }
        ]

    # Botones de preguntas predefinidas rápidas
    st.markdown("**Consultas rápidas sugeridas:**")
    cp1, cp2, cp3, cp4 = st.columns(4)
    pregunta_rapida = None

    if cp1.button("🌱 ¿Es seguro sembrar yuca o maíz hoy?", use_container_width=True):
        pregunta_rapida = "¿Es seguro sembrar yuca o maíz hoy dadas las condiciones actuales?"
    if cp2.button("🚨 ¿Cuál es el nivel de riesgo de inundación?", use_container_width=True):
        pregunta_rapida = "¿Cuál es el nivel de riesgo de crecida o inundación según las lluvias de las últimas 24 horas?"
    if cp3.button("🔋 ¿Cuál es el estado de las baterías?", use_container_width=True):
        pregunta_rapida = "¿Cuál es el estado de salud de la batería y paneles de las estaciones meteorológicas?"
    if cp4.button("💨 ¿El viento amenaza los platanales?", use_container_width=True):
        pregunta_rapida = "¿Las velocidades de viento registradas representan peligro para el plátano o maíz?"

    # Renderizar historial de mensajes
    for msg in st.session_state.rag_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "contexto" in msg:
                with st.expander("🔍 Ver fuentes Qdrant y telemetría analizada"):
                    st.json(msg["contexto"]["telemetria"])
                    for d in msg["contexto"]["documentos"]:
                        st.markdown(f"**Documento:** `{d.get('documento')}` (Similitud: `{d.get('score')}`)")
                        st.markdown(f"> {d.get('texto')[:280]}...")

    # Captura de input del usuario (por botón rápido o campo de texto)
    prompt_usuario = st.chat_input("Escribe tu pregunta sobre cultivos, clima, alertas o telemetría...")
    prompt_activo = pregunta_rapida or prompt_usuario

    if prompt_activo:
        st.session_state.rag_messages.append({"role": "user", "content": prompt_activo})
        with st.chat_message("user"):
            st.markdown(prompt_activo)

        with st.chat_message("assistant"):
            with st.spinner("Analizando telemetría de sensores y consultando base vectorial Qdrant..."):
                resultado_rag = responder_consulta_rag(
                    pregunta=prompt_activo,
                    estacion_id=estacion_sel if estacion_sel != "Todas" else None,
                )
                respuesta_texto = resultado_rag["respuesta"]
                st.markdown(respuesta_texto)
                st.caption(f"⚙️ Procesado mediante: `{resultado_rag['motor']}`")

                with st.expander("🔍 Ver fuentes Qdrant y telemetría analizada"):
                    st.write("**Telemetría viva utilizada:**")
                    st.json(resultado_rag["telemetria"])
                    st.write("**Fragmentos recuperados de Qdrant:**")
                    for d in resultado_rag["documentos"]:
                        st.markdown(f"**[{d.get('titulo')}]** - `{d.get('documento')}` (Score: `{d.get('score')}`)")
                        st.markdown(f"> {d.get('texto')[:300]}...")

        st.session_state.rag_messages.append({
            "role": "assistant",
            "content": respuesta_texto,
            "contexto": {
                "telemetria": resultado_rag["telemetria"],
                "documentos": resultado_rag["documentos"],
            },
        })

# ------------------------------------------------------------------------------
# TAB 1: TEMPERATURA
# ------------------------------------------------------------------------------
with tab_temp:
    st.markdown("#### Dinámica de Temperatura (°C)")
    c1, c2, c3, c4 = st.columns(4)
    t_series = df_graficos["temperatura"].dropna()
    if not t_series.empty:
        c1.metric("Mínima", f"{t_series.min():.1f} °C")
        c2.metric("Media", f"{t_series.mean():.1f} °C")
        c3.metric("Máxima", f"{t_series.max():.1f} °C")
        c4.metric("Desv. Estándar", f"{t_series.std():.2f}")

    fig = px.line(
        df_graficos,
        x="timestamp",
        y="temperatura",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Historial de Temperatura",
        labels={"temperatura": "Temperatura (°C)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#e67e22", "#d35400", "#e74c3c"],
    )
    # Línea de umbral de alerta (40 °C)
    fig.add_hline(
        y=UMBRALES_ALERTA["calor_extremo"],
        line_dash="dot",
        line_color="red",
        annotation_text="Alerta Calor Extremo (40°C)",
        annotation_position="top left",
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 2: HUMEDAD
# ------------------------------------------------------------------------------
with tab_hum:
    st.markdown("#### Humedad Relativa (%)")
    c1, c2, c3 = st.columns(3)
    h_series = df_graficos["humedad"].dropna()
    if not h_series.empty:
        c1.metric("Mínima", f"{h_series.min():.1f} %")
        c2.metric("Promedio", f"{h_series.mean():.1f} %")
        c3.metric("Máxima", f"{h_series.max():.1f} %")

    fig = px.line(
        df_graficos,
        x="timestamp",
        y="humedad",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Historial de Humedad Relativa",
        labels={"humedad": "Humedad (%)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#3498db", "#2980b9", "#1abc9c"],
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 3: PRECIPITACIÓN
# ------------------------------------------------------------------------------
with tab_prec:
    st.markdown("#### Registro de Precipitación Pluvial (mm)")
    c1, c2, c3 = st.columns(3)
    p_series = df_graficos["precipitacion"].dropna()
    if not p_series.empty:
        c1.metric("Acumulada en ventana", f"{p_series.sum():.1f} mm")
        c2.metric("Pico Máximo", f"{p_series.max():.1f} mm")
        c3.metric("Lecturas con lluvia", f"{(p_series > 0).sum()}")

    fig = px.bar(
        df_graficos,
        x="timestamp",
        y="precipitacion",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Precipitación Registrada",
        labels={"precipitacion": "Precipitación (mm)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#1f77b4", "#17becf"],
    )
    fig.add_hline(
        y=UMBRALES_ALERTA["precipitacion_intensa"],
        line_dash="dot",
        line_color="crimson",
        annotation_text="Alerta Lluvia Intensa (50mm)",
        annotation_position="top left",
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 4: PRESIÓN ATMOSFÉRICA
# ------------------------------------------------------------------------------
with tab_pres:
    st.markdown("#### Presión Barométrica (hPa)")
    c1, c2, c3 = st.columns(3)
    pr_series = df_graficos["presion"].dropna()
    if not pr_series.empty:
        c1.metric("Mínima", f"{pr_series.min():.2f} hPa")
        c2.metric("Media", f"{pr_series.mean():.2f} hPa")
        c3.metric("Máxima", f"{pr_series.max():.2f} hPa")

    fig = px.line(
        df_graficos,
        x="timestamp",
        y="presion",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Presión Barométrica",
        labels={"presion": "Presión (hPa)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#9b59b6", "#8e44ad"],
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 5: VIENTO
# ------------------------------------------------------------------------------
with tab_viento:
    st.markdown("#### Velocidad del Viento (km/h)")
    c1, c2, c3 = st.columns(3)
    v_series = df_graficos["viento"].dropna()
    if not v_series.empty:
        c1.metric("Velocidad Media", f"{v_series.mean():.1f} km/h")
        c2.metric("Ráfaga Máxima", f"{v_series.max():.1f} km/h")
        c3.metric("Calmas (0 km/h)", f"{(v_series == 0).sum()}")

    fig = px.area(
        df_graficos,
        x="timestamp",
        y="viento",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Velocidad del Viento",
        labels={"viento": "Viento (km/h)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#2ecc71", "#27ae60"],
    )
    fig.add_hline(
        y=UMBRALES_ALERTA["viento_fuerte"],
        line_dash="dot",
        line_color="darkred",
        annotation_text="Alerta Viento Fuerte (80 km/h)",
        annotation_position="top left",
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 6: BATERÍA
# ------------------------------------------------------------------------------
with tab_bat:
    st.markdown("#### Estado de Energía y Batería (V)")
    c1, c2, c3 = st.columns(3)
    b_series = df_graficos["voltaje_bateria"].dropna()
    if not b_series.empty:
        c1.metric("Voltaje Actual", f"{b_series.iloc[-1]:.2f} V")
        c2.metric("Mínimo Registrado", f"{b_series.min():.2f} V")
        c3.metric("Nivel Crítico (< 3.2V)", f"{(b_series < 3.2).sum()} lecturas")

    fig = px.line(
        df_graficos,
        x="timestamp",
        y="voltaje_bateria",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Voltaje de Alimentación de la Estación",
        labels={"voltaje_bateria": "Batería (Voltios)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#f39c12", "#e67e22"],
    )
    fig.add_hline(
        y=UMBRALES_ALERTA["bateria_baja"],
        line_dash="dash",
        line_color="red",
        annotation_text="Nivel Crítico (3.2 V)",
        annotation_position="bottom left",
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 7: RSSI (SEÑAL)
# ------------------------------------------------------------------------------
with tab_rssi:
    st.markdown("#### Calidad de Enlace y Señal RSSI (dBm)")
    c1, c2, c3 = st.columns(3)
    r_series = df_graficos["rssi"].dropna()
    if not r_series.empty:
        c1.metric("RSSI Actual", f"{r_series.iloc[-1]:.0f} dBm")
        c2.metric("Mejor Señal", f"{r_series.max():.0f} dBm")
        c3.metric("Peor Señal", f"{r_series.min():.0f} dBm")

    fig = px.line(
        df_graficos,
        x="timestamp",
        y="rssi",
        color="estacion_id" if estacion_sel == "Todas" else None,
        title="Potencia de Señal Recibida (RSSI)",
        labels={"rssi": "RSSI (dBm)", "timestamp": "Fecha / Hora"},
        color_discrete_sequence=["#16a085", "#1abc9c"],
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 8: COMPARATIVA MULTIVARIABLE
# ------------------------------------------------------------------------------
with tab_comparativa:
    st.markdown("#### Correlación Temperatura vs Humedad")
    fig_scatter = px.scatter(
        df_graficos,
        x="temperatura",
        y="humedad",
        color="estacion_id" if estacion_sel == "Todas" else None,
        size="precipitacion",
        hover_data=["timestamp", "viento"],
        title="Dispersión Microclimática: Temperatura vs Humedad (Tamaño = Precipitación)",
        labels={"temperatura": "Temperatura (°C)", "humedad": "Humedad (%)"},
    )
    st.plotly_chart(fig_scatter, use_container_width=True)


# ==============================================================================
# SECCIÓN 3: CALIDAD DE DATOS Y ALERTAS ACTIVAS
# ==============================================================================
st.markdown("---")
col_izq, col_der = st.columns([1, 1])

with col_izq:
    st.subheader("✅ Distribución de Calidad de Datos")
    calidad = obtener_estadisticas_calidad()
    if calidad:
        df_calidad = pd.DataFrame([{"Estado": k, "Total": v} for k, v in calidad.items()])
        fig_calidad = px.pie(
            df_calidad,
            values="Total",
            names="Estado",
            title="Calidad de Datos (Marco Buelvas et al.)",
            color="Estado",
            color_discrete_map={
                "valid": "#2ecc71",
                "invalid": "#e74c3c",
                "incomplete": "#f39c12",
            },
        )
        st.plotly_chart(fig_calidad, use_container_width=True)
    else:
        st.info("Sin estadísticas de calidad aún.")

with col_der:
    st.subheader("🚨 Detección de Alertas en Ventana")
    st.caption(f"Supervisión continua sobre {len(df_sorted)} registros de la ventana activa.")
    
    alertas_detectadas = []
    
    # Evaluar todas las filas de la ventana para alertas climáticas y fallas de calidad
    for _, row in df_sorted.iterrows():
        est_id = row.get("estacion_id", "ESTACION")
        ts_str = str(row.get("timestamp", ""))
        p_val = row.get("precipitacion")
        t_val = row.get("temperatura")
        b_val = row.get("voltaje_bateria")
        v_val = row.get("viento")
        calidad_val = str(row.get("estado_calidad", "")).lower()
        err_msg = row.get("errores")

        # 1. Alertas de Fallas de Calidad de Datos (Buelvas et al.)
        if calidad_val == "invalid":
            detalle_err = f": *{err_msg}*" if err_msg and pd.notna(err_msg) else " (Valores fuera de rango físico)"
            alertas_detectadas.append(f"❌ **Falla de Calidad / Sensor Inválido** en `{est_id}`{detalle_err} a las `{ts_str}`")
        elif calidad_val == "incomplete":
            alertas_detectadas.append(f"⚠️ **Telemetría Incompleta** en `{est_id}` a las `{ts_str}`")

        # 2. Alertas Meteorológicas Críticas
        if pd.notna(p_val) and p_val > UMBRALES_ALERTA["precipitacion_intensa"]:
            alertas_detectadas.append(f"🌧️ **Lluvia Intensa** en `{est_id}` ({p_val:.1f} mm) a las `{ts_str}`")
        if pd.notna(t_val) and t_val > UMBRALES_ALERTA["calor_extremo"] and calidad_val == "valid":
            alertas_detectadas.append(f"🔥 **Calor Extremo** en `{est_id}` ({t_val:.1f} °C) a las `{ts_str}`")
        if pd.notna(b_val) and b_val < UMBRALES_ALERTA["bateria_baja"]:
            alertas_detectadas.append(f"🔋 **Batería Baja** en `{est_id}` ({b_val:.2f} V) a las `{ts_str}`")
        if pd.notna(v_val) and v_val > UMBRALES_ALERTA["viento_fuerte"]:
            alertas_detectadas.append(f"💨 **Viento Fuerte** en `{est_id}` ({v_val:.1f} km/h) a las `{ts_str}`")

    if alertas_detectadas:
        st.warning(f"⚠️ Se detectaron **{len(alertas_detectadas)} eventos / anomalías** en esta ventana temporal.")
        # Mostrar las más recientes (últimas 8)
        for alerta in alertas_detectadas[-8:]:
            st.markdown(alerta)
        
        if len(alertas_detectadas) > 8:
            with st.expander(f"📜 Ver todas las {len(alertas_detectadas)} alertas de la ventana"):
                for a in reversed(alertas_detectadas[:-8]):
                    st.markdown(f"- {a}")
    else:
        st.success("✅ No se detectaron condiciones anómalas ni fallas de calidad en este rango.")


# ==============================================================================
# SECCIÓN 4: TABLA DE REGISTROS RECIENTES CON RESALTADO EN ROJO
# ==============================================================================
st.markdown("---")
st.subheader("📋 Tabla de Registros Recientes")
st.caption("Las variables y tramas con estado `invalid` se resaltan automáticamente en **rojo** para facilitar el diagnóstico de campo.")

columnas_tabla = [
    "estacion_id",
    "timestamp",
    "temperatura",
    "humedad",
    "presion",
    "precipitacion",
    "viento",
    "voltaje_bateria",
    "rssi",
    "estado_calidad",
    "errores",
]
columnas_existentes = [c for c in columnas_tabla if c in df_sorted.columns]
df_tabla = df_sorted[columnas_existentes].tail(40).copy()

def resaltar_anomalias_celdas(row):
    """
    Aplica estilo visual a las celdas:
    Si la fila es 'invalid', resalta en rojo la variable específica fuera de rango
    y la etiqueta de calidad.
    """
    estilos = [""] * len(row)
    columnas = list(row.index)
    es_invalida = str(row.get("estado_calidad", "")).lower() == "invalid"
    
    # CSS para resaltar celda en rojo
    estilo_rojo_critico = "background-color: #ffcccc; color: #990000; font-weight: bold; border-radius: 3px;"
    estilo_texto_error = "color: #b30000; font-weight: bold; font-style: italic;"

    for idx, col in enumerate(columnas):
        val = row[col]
        
        # Resaltar la celda del estado de calidad
        if col == "estado_calidad" and es_invalida:
            estilos[idx] = estilo_rojo_critico
        elif col == "errores" and pd.notna(val) and es_invalida:
            estilos[idx] = estilo_texto_error
        
        # Resaltar la variable numérica específica que causó la anomalía
        elif col == "temperatura" and pd.notna(val) and (val < -10.0 or val > 45.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "humedad" and pd.notna(val) and (val < 0.0 or val > 100.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "presion" and pd.notna(val) and (val < 800.0 or val > 1100.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "precipitacion" and pd.notna(val) and (val < 0.0 or val > 300.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "viento" and pd.notna(val) and (val < 0.0 or val > 150.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "voltaje_bateria" and pd.notna(val) and (val < 2.5 or val > 5.0):
            estilos[idx] = estilo_rojo_critico
        elif col == "rssi" and pd.notna(val) and (val < -120 or val > 0):
            estilos[idx] = estilo_rojo_critico

    return estilos

# Aplicar estilo condicional
df_estilizado = df_tabla.style.apply(resaltar_anomalias_celdas, axis=1)

st.dataframe(
    df_estilizado,
    use_container_width=True,
    height=420,
)

# Footer
st.markdown("---")
st.caption(
    "AmazonIA 4.0 | Maestría en Tecnologías de la Información | Universidad Nacional Experimental de Guayana (UNEG)"
)