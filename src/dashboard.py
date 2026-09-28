"""
Dashboard interactivo para AmazonIA 4.0.
Visualización de telemetría microclimática en tiempo real.
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
)

# Ahora sí, el resto de imports
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

from src.database import (
    obtener_ultimas_lecturas,
    obtener_estadisticas_calidad,
    obtener_estaciones,
)


st.title("🌿 AmazonIA 4.0 - Monitoreo Microclimático")
st.markdown("### Cuenca del Río Caroní")


# Sidebar con filtros
with st.sidebar:
    st.header("🔧 Filtros")
    
    estaciones = obtener_estaciones()
    if not estaciones:
        estaciones = ["CARONI-001", "CARONI-002"]
    
    estacion = st.selectbox("Estación", ["Todas"] + estaciones)
    limite = st.slider("Número de lecturas", 10, 500, 100)
    
    st.markdown("---")
    st.markdown("**Estado del sistema**")
    st.success("✅ Edge Gateway operativo")
    st.info("📡 Sincronización por lotes activa")


# Cargar datos
@st.cache_data(ttl=30)
def cargar_datos(estacion_id, limite):
    if estacion_id == "Todas":
        datos = obtener_ultimas_lecturas(None, limite)
    else:
        datos = obtener_ultimas_lecturas(estacion_id, limite)
    return pd.DataFrame(datos)


df = cargar_datos(estacion, limite)

if df.empty:
    st.warning("No hay datos disponibles. Ejecute el simulador para generar telemetría.")
    st.stop()


# Métricas principales
st.subheader("📊 Indicadores Actuales")

col1, col2, col3, col4 = st.columns(4)

ultimo = df.iloc[0]

with col1:
    st.metric("🌡️ Temperatura", f"{ultimo['temperatura']:.1f} °C" if pd.notna(ultimo['temperatura']) else "N/A")

with col2:
    st.metric("💧 Humedad", f"{ultimo['humedad']:.1f} %" if pd.notna(ultimo['humedad']) else "N/A")

with col3:
    st.metric("🌧️ Precipitación", f"{ultimo['precipitacion']:.1f} mm" if pd.notna(ultimo['precipitacion']) else "N/A")

with col4:
    bateria = ultimo['voltaje_bateria']
    if pd.notna(bateria):
        delta = "OK" if bateria > 3.5 else "Crítico"
        st.metric("🔋 Batería", f"{bateria:.2f} V", delta=delta)
    else:
        st.metric("🔋 Batería", "N/A")


# Series temporales
st.subheader("📈 Series Temporales")

df_sorted = df.sort_values("timestamp")

fig_temp = px.line(
    df_sorted, x="timestamp", y="temperatura",
    title="Temperatura en el Tiempo",
    labels={"temperatura": "Temperatura (°C)", "timestamp": "Fecha"},
    color="estacion_id" if estacion == "Todas" else None,
)
st.plotly_chart(fig_temp, use_container_width=True)

fig_hum = px.line(
    df_sorted, x="timestamp", y="humedad",
    title="Humedad Relativa",
    labels={"humedad": "Humedad (%)", "timestamp": "Fecha"},
    color="estacion_id" if estacion == "Todas" else None,
)
st.plotly_chart(fig_hum, use_container_width=True)

fig_prec = px.bar(
    df_sorted, x="timestamp", y="precipitacion",
    title="Precipitación Acumulada",
    labels={"precipitacion": "Precipitación (mm)", "timestamp": "Fecha"},
)
st.plotly_chart(fig_prec, use_container_width=True)


# Estado de calidad
st.subheader("✅ Estado de Calidad de Datos")

calidad = obtener_estadisticas_calidad()

if calidad:
    df_calidad = pd.DataFrame([
        {"Estado": k, "Total": v} for k, v in calidad.items()
    ])
    
    fig_calidad = px.pie(
        df_calidad, values="Total", names="Estado",
        title="Distribución de Calidad",
        color="Estado",
        color_discrete_map={
            "valid": "#2ecc71",
            "invalid": "#e74c3c",
            "incomplete": "#f39c12",
        },
    )
    st.plotly_chart(fig_calidad, use_container_width=True)
else:
    st.info("No hay datos de calidad disponibles.")


# Tabla de datos recientes
st.subheader("📋 Datos Recientes")
st.dataframe(
    df[["estacion_id", "timestamp", "temperatura", "humedad", "presion",
        "precipitacion", "viento", "voltaje_bateria", "estado_calidad"]].head(20),
    use_container_width=True,
)


# Alertas activas
st.subheader("🚨 Alertas Activas")

alertas = []
for _, row in df.head(10).iterrows():
    if pd.notna(row['precipitacion']) and row['precipitacion'] > 50:
        alertas.append(f"⚠️ Lluvia intensa en {row['estacion_id']}: {row['precipitacion']} mm")
    if pd.notna(row['temperatura']) and row['temperatura'] > 40:
        alertas.append(f"🔥 Calor extremo en {row['estacion_id']}: {row['temperatura']} °C")
    if pd.notna(row['voltaje_bateria']) and row['voltaje_bateria'] < 3.2:
        alertas.append(f"🔋 Batería baja en {row['estacion_id']}: {row['voltaje_bateria']} V")

if alertas:
    for alerta in alertas:
        st.warning(alerta)
else:
    st.success("✅ No hay alertas activas")


# Footer
st.markdown("---")
st.caption("AmazonIA 4.0 | Maestría en Tecnologías de la Información | UNEG")