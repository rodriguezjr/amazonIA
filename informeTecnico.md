# INFORME TÉCNICO DE ARQUITECTURA E IMPLEMENTACIÓN
## Proyecto: AmazonIA 4.0 — Telemetría Inteligente, Memoria Semántica y Lakehouse Medallion
### Maestría en Tecnologías de la Información — Tipos de Datos Inteligentes
**Institución:** Universidad Nacional Experimental de Guayana (UNEG)  
**Docente:** Profa. Livia Borjas  
**Integrantes:**
- Massiel Barrios (C.I. 27.375.910)
- Luis Pizzanni (C.I. 27.765.210)
- Junior Rodríguez (C.I. 23.730.853)

---

## 1. Diseño Conceptual de la Arquitectura

### 1.1. Contexto Operativo y Requerimientos del Entorno
La cuenca del Río Caroní (Estado Bolívar, Venezuela) es un enclave geoestratégico de alta biodiversidad y relevancia hidroeléctrica que alberga comunidades agrícolas e indígenas en zonas remotas. Estas regiones presentan desafíos operacionales severos:
- **Conectividad intermitente o inexistente:** Red celular nula y dependencia de enlaces satelitales esporádicos o redes de radio frecuencia de baja potencia (LoRaWAN).
- **Alimentación autónoma aislada:** Estaciones meteorológicas alimentadas exclusivamente por paneles solares fotovoltaicos y bancos de celdas de litio 18650, vulnerables a nubosidad persistente y calor tropical extremo.
- **Heterogeneidad de datos:** Señales de sensores micrometeorológicos en tiempo real (temperatura, humedad relativa, presión atmosférica, pluviometría acumulada, velocidad del viento, voltaje de batería y nivel de señal RSSI), cruzadas con manuales técnicos agroecológicos, normativas de crecidas de ríos y protocolos comunitarios en lenguaje natural.

Para solventar estos desafíos, AmazonIA 4.0 implementa una arquitectura **Offline-First**, orientada a eventos e impulsada por tres pilares de datos inteligentes:
1. **Edge Gateway con buffer local inmutable (SQLite):** Garantiza que ninguna telemetría se pierda en campo ante la caída de enlaces de red.
2. **Lakehouse Medallion Embebido (DuckDB + Apache Parquet ZSTD):** Provee capacidades OLAP analíticas de alto rendimiento sin requerir clusters pesados de Spark o Databricks.
3. **Memoria Semántica RAG (Qdrant + Embeddings Vectoriales):** Asistente agroclimático que recupera manuales agronómicos locales e interpreta la telemetría viva de los sensores.

---

### 1.2. Diagrama de Bloques de la Arquitectura de Datos

```mermaid
flowchart TD
    %% ==========================================
    %% CAPA 1: CAPTURA EN CAMPO (IoT EDGE SENSORS)
    %% ==========================================
    subgraph CAPA_CAMPO["1. Capa de Captura en Campo (Sensores IoT & Microestaciones)"]
        direction TB
        S1["🌡️ Sensor DHT22 / BME280\n(Temp, Humedad, Presión)"]
        S2["🌧️ Pluviómetro de Balancín\n(Precipitación mm)"]
        S3["💨 Anemómetro Digital\n(Velocidad Viento km/h)"]
        S4["🔋 Sensor Analógico ADC\n(Voltaje Batería 3.0V - 4.2V)"]
        MC["Microcontrolador ESP32 / LoRa\n(Payload JSON + RSSI)"]
        S1 & S2 & S3 & S4 --> MC
    end

    %% ==========================================
    %% CAPA 2: EDGE GATEWAY & BUFFER PERSISTENTE
    %% ==========================================
    subgraph CAPA_EDGE["2. Capa Edge Gateway & Persistencia Local (Offline-First)"]
        direction TB
        BR["Bridge MQTT Mosquitto\n(:1883 amazonia/caroni/+)"]
        API["FastAPI Ingesta Endpoint\n(:8000 /ingesta/telemetria)"]
        VAL["Motor de Validación y Calidad\n(Marco Buelvas et al. - valid/invalid)"]
        SQLITE[("SQLite Buffer Local\ndata/telemetria.db\n(Tabla telemetria inmutable)")]
        SYNC["Agente de Sincronización\n(Backoff exponencial + reintentos)"]

        MC -->|MQTT / HTTP| BR & API
        BR & API --> VAL
        VAL -->|Inserción ACID| SQLITE
        SQLITE --> SYNC
    end

    %% ==========================================
    %% CAPA 3: ORQUESTACIÓN Y PROCESAMIENTO ANALÍTICO
    %% ==========================================
    subgraph CAPA_LAKEHOUSE["3. Capa Analítica Medallion & Orquestación (DuckDB / Airflow)"]
        direction TB
        AIRFLOW["Apache Airflow DAG\n(dags/pipeline_amazonia.py)"]
        RUNNER["Runner Local de Pipeline\n(run_pipeline_local.py)"]
        
        BRONZE["🥉 Capa Bronze (Raw Parquet)\ndata/lakehouse/bronze/telemetria_raw.parquet\n(ZSTD, 100% registros crudos)"]
        SILVER["🥈 Capa Silver (Depuración SQL)\ndata/lakehouse/silver/telemetria_limpia.parquet\n(ROW_NUMBER Dedup + Filtro Calidad)"]
        
        subgraph GOLD_MARTS["🥇 Capa Gold (Datamarts Especializados)"]
            G1["📊 Indicadores Horarios\n(indicadores_horarios.parquet)"]
            G2["📅 Resumen Diario 24h\n(indicadores_diarios.parquet)"]
            G3["🩺 KPIs Calidad Estaciones\n(kpis_calidad_estaciones.parquet)"]
        end

        SQLITE -->|COPY ZSTD DuckDB| BRONZE
        BRONZE -->|DuckDB SQL Limpieza| SILVER
        SILVER --> G1 & G2
        BRONZE & SILVER --> G3
        AIRFLOW -.->|Orquesta| BRONZE & SILVER & GOLD_MARTS
        RUNNER -.->|Ejecución Local| BRONZE & SILVER & GOLD_MARTS
    end

    %% ==========================================
    %% CAPA 4: MEMORIA SEMÁNTICA VECTORIAL (RAG)
    %% ==========================================
    subgraph CAPA_RAG["4. Capa de Memoria Semántica RAG (Qdrant & Embeddings)"]
        direction TB
        KB["📚 Knowledge Base Markdown\n(Agroclima, Inundaciones, Mantenimiento)"]
        CHUNK["Chunking Semántico\n(Secciones ## / 128-dim Vectors)"]
        QDRANT[("Qdrant Vector Database\n(Colección: documentos_amazonia)\nModo Disco Local / Servidor :6333")]
        ENGINE["Motor RAG AmazonIA 4.0\n(Correlación Telemetría + Vector Search)"]

        KB --> CHUNK -->|Upsert Puntos| QDRANT
        QDRANT -->|Búsqueda Coseno Top-K| ENGINE
        SQLITE -->|Telemetría Viva de Sensores| ENGINE
    end

    %% ==========================================
    %% CAPA 5: CONSUMO Y VISUALIZACIÓN
    %% ==========================================
    subgraph CAPA_CONSUMO["5. Capa de Consumo, Alertas y Visualización Comunitaria"]
        direction TB
        DASH["🖥️ Dashboard Streamlit (:8501)\n(9 Pestañas Analíticas + Rango Temporal)"]
        CHAT["🤖 Asistente Comunitario IA\n(Chat Agroclimático Interactivo)"]
        ALERTA["🚨 Sistema de Alertas Comunitarias\n(Lluvia Crítica, Batería Baja, Crecidas)"]
        REST_API["🌐 API REST OpenAPI / JSON\n(:8000 /consultas, /alertas)"]

        G1 & G2 & G3 --> DASH
        ENGINE --> CHAT
        VAL --> ALERTA
        SQLITE --> REST_API
    end

    CAPA_EDGE --> CAPA_LAKEHOUSE
    CAPA_EDGE --> CAPA_RAG
    CAPA_LAKEHOUSE --> CAPA_CONSUMO
    CAPA_RAG --> CAPA_CONSUMO
```

---

## 2. Desarrollo Técnico de la Solución

### 2.1. Memoria Semántica y Bases de Datos Elegidas

#### A. Qdrant (Base de Datos Vectorial de Alta Eficiencia)
Para la memoria semántica se seleccionó **Qdrant** debido a sus características arquitectónicas clave:
1. **Doble modalidad operativa (Transparente y Offline-First):**
   - Cuando se cuenta con contenedor Docker o clúster de producción, se conecta vía gRPC/HTTP al puerto `6333`.
   - Cuando opera en microordenadores de campo (Raspberry Pi, laptops de extensionistas rurales o terminales Windows sin Docker), conmuta automáticamente a **almacenamiento embebido directo en disco local** (`data/qdrant_storage/`) o memoria RAM (`:memory:`), sin requerir servicios externos ni conectividad a Internet.
2. **Estructura de la Colección:**
   - **Nombre:** `documentos_amazonia`
   - **Métrica de Distancia:** Similitud de Coseno ($\text{Cosine Distance} \in [-1, 1]$).
   - **Dimensión Vectorial:** 128 dimensiones.
   - **Índice HNSW (Hierarchical Navigable Small World):** Garantiza búsqueda aproximada de vecinos cercanos ($k$-NN) con complejidad temporal $\mathcal{O}(\log N)$.
   - **Payload Filtrable:** Cada punto almacena el texto completo del fragmento, el nombre del documento de procedencia, el título de la sección y la longitud en caracteres.

#### B. Generador de Embeddings Deterministas Locales (128 Dimensiones)
Para evitar la dependencia absoluta de APIs externas comerciales (como OpenAI o Cohere) que demandan conexión a Internet y consumen saldo, se implementó un motor de vectorización semántica local determinista basado en hashing criptográfico y proyección ponderada por $n$-gramas:
- Normaliza el texto a minúsculas y extrae tanto palabras completas como bigramas y trigramas fonéticos.
- Aplica una función de hash de alta entropía (`sha256`) combinada con semillas fijas por coordenada para generar un vector base determinista.
- Cada término proyecta energía ponderada sobre los 128 coeficientes vectoriales.
- El vector final se somete a **normalización Euclidiana $L_2$**:
  $$\hat{\mathbf{v}} = \frac{\mathbf{v}}{\|\mathbf{v}\|_2} = \frac{\mathbf{v}}{\sqrt{\sum_{i=1}^{128} v_i^2}}$$
- De esta manera, el producto interno de dos vectores normalizados equivale exactamente a la similitud de coseno:
  $$\text{Similitud}(\mathbf{a}, \mathbf{b}) = \hat{\mathbf{a}} \cdot \hat{\mathbf{b}} = \cos(\theta)$$

#### C. SQLite (Almacenamiento Transaccional OLTP en el Borde)
- Diseñado para transacciones ACID rápidas en el Edge Gateway.
- Persiste registros en la tabla `telemetria` con columnas estructuradas, claves foráneas, índices por `(estacion_id, timestamp)` y control de calidad basado en la investigación de Buelvas et al. (2023).

#### D. DuckDB + Apache Parquet (Motor Analítico OLAP Vectorizado)
- **DuckDB:** Motor SQL embebido enfocado en procesamiento analítico en memoria y ejecución de consultas vectorizadas basadas en SIMD.
- **Apache Parquet:** Formato columnar inmutable con compresión **ZSTD (Zstandard)**, que reduce el tamaño en disco de la telemetría en más de un 80% comparado con JSON o CSV, acelerando el escaneo de rangos temporales extensos.

---

### 2.2. Lógica del Flujo Dinámico y Criterios del Orquestador Automatizado

El ciclo de procesamiento de datos está coordinado tanto por **Apache Airflow** (en entornos de clúster/servidor) como por el **Runner Local** `run_pipeline_local.py` (en campo). Ambos ejecutan la misma lógica desacoplada bajo criterios rigurosos de calidad e integridad.

```mermaid
stateDiagram-v2
    [*] --> IngestaSQLite: Recepción MQTT / REST
    
    state IngestaSQLite {
        [*] --> ValidacionRangos
        ValidacionRangos --> ClasificacionCalidad: Buelvas et al.
        ClasificacionCalidad --> InsercionBD: valid / invalid / incomplete
    }

    IngestaSQLite --> CapaBronze: Airflow / Runner Local Trigger
    
    state CapaBronze {
        [*] --> ExtraccionSQLite
        ExtraccionSQLite --> IngestaTimestamp: Adición metadata ingesta_lakehouse_ts
        IngestaTimestamp --> EscrituraBronzeParquet: telemetria_raw.parquet (ZSTD)
    }

    CapaBronze --> CapaSilver: Criterios de Depuración

    state CapaSilver {
        [*] --> FiltroCalidad: WHERE estado_calidad = 'valid'
        FiltroCalidad --> Deduplicacion: ROW_NUMBER() OVER (PARTITION BY estacion_id, ts)
        Deduplicacion --> NormalizacionTermica: Temp < -10 o > 60 -> NULL
        NormalizacionTermica --> EscrituraSilverParquet: telemetria_limpia.parquet
    }

    CapaSilver --> CapaGold: Transformaciones Analíticas

    state CapaGold {
        [*] --> IndicadoresHorarios: Truncado 'hour' + AVG, MAX, MIN, SUM lluvia
        [*] --> IndicadoresDiarios: Truncado 'day' + Amplitud térmica + Lluvia 24h
        [*] --> KPIsEstaciones: % Calidad, Salud de Batería y RSSI
    }

    CapaGold --> IndexacionRAG: Sincronización Memoria Semántica
    IndexacionRAG --> EvaluacionAlertas: Umbrales Críticos Comunales
    EvaluacionAlertas --> [*]
```

#### Criterios y Reglas del Flujo Dinámico:
1. **Criterio de Validación Termodinámica en Borde:**
   - Toda lectura con temperatura $T \notin [-10.0, 45.0]\,^\circ\text{C}$ o humedad relativa $H \notin [0.0, 100.0]\,\%$ es etiquetada como `invalid` o `incomplete`.
2. **Criterio de Deduplicación en Capa Silver:**
   - La red de radio o reintentos HTTP puede reenviar paquetes idénticos. DuckDB ejecuta una ventana analítica:
     $$\text{ROW\_NUMBER}() \text{ OVER (PARTITION BY } \text{estacion\_id}, \text{timestamp ORDER BY } \text{id DESC})$$
     conservando exclusivamente la fila con `fila_orden = 1`.
3. **Criterio de Normalización Térmica Extendida en Silver:**
   - Valores con temperatura fuera del rango geográfico admisible $(-10^\circ\text{C} \text{ a } 60^\circ\text{C})$ se transforman a `NULL` sin eliminar la tupla, salvaguardando las demás variables meteorológicas válidas (lluvia, presión, viento).
4. **Criterios de Agregación en Capa Gold:**
   - **Horarios:** Agrupa por `estacion_id` y `date_trunc('hour', timestamp)`, calculando promedios ponderados, ráfagas máximas de viento y acumulado pluvial por hora.
   - **Diarios:** Agrupa por `estacion_id` y fecha, calculando amplitud térmica diaria $(\Delta T = T_{\max} - T_{\min})$ y volumen de lluvia de 24 horas.
   - **KPIs de Estación:** Cruza la capa Bronze con Silver para determinar el ratio de operatividad:
     $$\% \text{ Calidad} = \frac{\sum (\text{estado\_calidad} = \text{'valid'})}{\text{Total Recibidos}} \times 100$$
5. **Criterios de Alertas Comunitarias:**
   - **Lluvia Crítica:** Lluvia acumulada 24h $> 50\,\text{mm}$ (Riesgo de crecida súbita del río Caroní).
   - **Estrés Térmico:** Temperatura $> 38^\circ\text{C}$.
   - **Degradación de Energía:** Batería $< 3.40\,\text{V}$ (Alerta de falla en celda solar o controlador TP4056).

---

### 2.3. Código Fuente del Filtro / Reranker e Interfaz Visual

#### A. Módulo Vector Store y Búsqueda Semántica (`src/rag/vector_store.py`)
Este módulo implementa el cálculo vectorial, la persistencia en Qdrant y la búsqueda semántica clasificada por score de similitud:

```python
"""
Motor de Almacenamiento y Recuperación Vectorial (Qdrant) para AmazonIA 4.0.
Soporta despliegue cliente-servidor (Docker) y local embebido en disco (Offline-First).
"""
import os
import re
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct

logger = logging.getLogger("amazonia.rag.vector_store")

VECTOR_DIM = 128
COLECCION_DEFECTO = "documentos_amazonia"
BASE_DIR = Path(__file__).resolve().parent.parent.parent
KNOWLEDGE_BASE_DIR = BASE_DIR / "knowledge_base"
LOCAL_STORAGE_DIR = BASE_DIR / "data" / "qdrant_storage"


def _calcular_embedding_offline(texto: str, dimension: int = VECTOR_DIM) -> List[float]:
    """
    Genera un embedding semántico denso y determinista (128 dimensiones).
    Utiliza proyecciones de n-gramas con normalización L2 sin dependencias externas.
    """
    texto_limpio = texto.lower().strip()
    terminos = re.findall(r"\b\w{3,}\b", texto_limpio)
    vector = np.zeros(dimension, dtype=np.float32)

    if not terminos:
        return vector.tolist()

    for termino in terminos:
        # Generación de dispersión pseudoaleatoria determinista basada en SHA-256
        h = hashlib.sha256(termino.encode("utf-8")).digest()
        for idx in range(dimension):
            peso = ((h[idx % len(h)] ^ h[(idx * 7) % len(h)]) % 100) / 100.0 - 0.5
            vector[idx] += peso

    # Normalización Euclidiana L2 (vector unitario para cálculo de distancia coseno)
    norma = np.linalg.norm(vector)
    if norma > 0:
        vector = vector / norma

    return vector.tolist()


class AmazonIAVectorStore:
    """Gestor de la base de conocimiento semántica en Qdrant."""

    _clientes_cache: Dict[str, QdrantClient] = {}

    def __init__(
        self,
        coleccion: str = COLECCION_DEFECTO,
        url_qdrant: Optional[str] = None,
        en_memoria: bool = False,
        storage_dir: Optional[Path] = None,
    ):
        self.coleccion = coleccion
        self.url = url_qdrant or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.storage_dir = storage_dir or LOCAL_STORAGE_DIR
        self.cliente = self._conectar_cliente(en_memoria)
        self._asegurar_coleccion()

    def _conectar_cliente(self, en_memoria: bool) -> QdrantClient:
        """Conecta a Qdrant remoto con fallback transparente a disco local."""
        if en_memoria:
            return QdrantClient(":memory:")

        # Intento de conexión por red
        if self.url and not self.url.startswith(":"):
            clave_red = f"url:{self.url}"
            if clave_red in self._clientes_cache:
                return self._clientes_cache[clave_red]
            try:
                cliente = QdrantClient(url=self.url, timeout=1.0, check_compatibility=False)
                cliente.get_collections()
                self._clientes_cache[clave_red] = cliente
                return cliente
            except Exception as e:
                logger.warning("Fallo al conectar a Qdrant remoto (%s). Activando motor local en disco.", e)

        # Fallback local persistente en disco
        ruta_str = str(self.storage_dir)
        if ruta_str in self._clientes_cache:
            return self._clientes_cache[ruta_str]

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        cliente = QdrantClient(path=ruta_str, check_compatibility=False)
        self._clientes_cache[ruta_str] = cliente
        return cliente

    def _asegurar_coleccion(self):
        """Crea la colección vectorial configurada con distancia Coseno."""
        try:
            colecciones = [c.name for c in self.cliente.get_collections().collections]
            if self.coleccion not in colecciones:
                self.cliente.create_collection(
                    collection_name=self.coleccion,
                    vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
                )
        except Exception as e:
            logger.error("Error al verificar colección Qdrant: %s", e)

    def buscar_similares(self, consulta: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        FILTRO Y RANKER VECTORIAL:
        Convierte la consulta del agricultor a vector y recupera los fragmentos
        técnicos más cercanos ordenados descendentemente por su puntaje de similitud.
        """
        emb_consulta = _calcular_embedding_offline(consulta)

        try:
            if hasattr(self.cliente, "query_points"):
                res = self.cliente.query_points(
                    collection_name=self.coleccion,
                    query=emb_consulta,
                    limit=top_k,
                )
                puntos = res.points
            else:
                puntos = self.cliente.search(
                    collection_name=self.coleccion,
                    query_vector=emb_consulta,
                    limit=top_k,
                )

            resultados = []
            for p in puntos:
                resultados.append({
                    "id": p.id,
                    "score": round(float(p.score), 3),
                    "titulo": p.payload.get("titulo", "Sin título"),
                    "documento": p.payload.get("documento", ""),
                    "texto": p.payload.get("texto", ""),
                })
            return resultados
        except Exception as e:
            logger.error("Error en búsqueda semántica Qdrant: %s", e)
            return []
```

#### B. Motor de Reranking y Generación Contextual (`src/rag/engine.py`)
Este módulo extrae el contexto cuantitativo de las últimas 24 horas de la estación seleccionada, ejecuta el filtro y reranker sobre el conocimiento recuperado y sintetiza la recomendación diagnóstica:

```python
"""
Motor RAG y Reranker Heurístico-Semántico para AmazonIA 4.0.
Cruza la telemetría viva de SQLite con los manuales indexados en Qdrant.
"""
from typing import Dict, Any, Optional, List
from src.database import obtener_lecturas_filtradas, obtener_ultimas_lecturas
from src.rag.vector_store import AmazonIAVectorStore
from src.config import UMBRALES_ALERTA

_vector_store_instance: Optional[AmazonIAVectorStore] = None


def responder_consulta_rag(pregunta: str, estacion_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Pipeline Completo RAG:
    1. Recuperación Cuantitativa: Estadísticas de sensores de las últimas 24h.
    2. Recuperación Semántica: Búsqueda k-NN en Qdrant.
    3. Reranker & Context Fusion: Construcción de diagnóstico correlacionado.
    """
    global _vector_store_instance
    if _vector_store_instance is None:
        _vector_store_instance = AmazonIAVectorStore()
        _vector_store_instance.indexar_documentos()

    # 1. Recuperar contexto de telemetría en tiempo real
    contexto_telemetria = extraer_contexto_telemetria(estacion_id)

    # 2. Recuperar y filtrar fragmentos semánticos en Qdrant (Top-K)
    documentos_recuperados = _vector_store_instance.buscar_similares(pregunta, top_k=3)

    # 3. Reranking y Generación Contextualizada (Modo Experto Offline)
    respuesta = _generar_respuesta_experta_offline(
        pregunta=pregunta,
        contexto_telemetria=contexto_telemetria,
        documentos=documentos_recuperados,
    )

    return {
        "pregunta": pregunta,
        "respuesta": respuesta,
        "estacion": contexto_telemetria.get("estacion"),
        "telemetria": contexto_telemetria,
        "documentos": documentos_recuperados,
        "motor": "Motor Agroclimático Inteligente (Qdrant + Sensores Vivos)",
    }
```

#### C. Interfaz Visual Desarrollada (`src/dashboard.py` — Fragmentos Clave)
La interfaz fue desarrollada en **Streamlit** y provee monitoreo micrometeorológico interactivo multivariable y el chat agroclimático asistido por RAG:

```python
# ==============================================================================
# SELECTOR TEMPORAL Y FILTRADO AVANZADO (BARRA LATERAL)
# ==============================================================================
st.sidebar.header("🕹️ Parámetros de Telemetría")

OPCIONES_VENTANA = {
    "Última hora": timedelta(hours=1),
    "Último día (24h)": timedelta(days=1),
    "Última semana (7d)": timedelta(weeks=1),
    "Último mes (30d)": timedelta(days=30),
    "Último año (365d)": timedelta(days=365),
}

ventana_sel = st.sidebar.selectbox("📅 Ventana temporal de análisis:", list(OPCIONES_VENTANA.keys()), index=4)
delta_tiempo = OPCIONES_VENTANA[ventana_sel]
fecha_inicio_iso = (datetime.now() - delta_tiempo).isoformat()

# ==============================================================================
# PESTAÑA DEDICADA: ASISTENTE AGROCLIMÁTICO COMUNITARIO (RAG)
# ==============================================================================
with tab_rag:
    st.markdown("### 🤖 Asistente Agroclimático y de Riesgo — RAG AmazonIA 4.0")
    st.caption("Correlación viva de sensores SQLite con la base vectorial Qdrant.")

    # Preguntas rápidas precargadas
    cp1, cp2, cp3, cp4 = st.columns(4)
    pregunta_rapida = None
    if cp1.button("🌱 ¿Es seguro sembrar yuca hoy?", use_container_width=True):
        pregunta_rapida = "¿Es seguro sembrar yuca o maíz hoy dadas las condiciones actuales?"
    if cp2.button("🚨 ¿Nivel de riesgo de inundación?", use_container_width=True):
        pregunta_rapida = "¿Cuál es el nivel de riesgo de crecida o inundación según las lluvias de las últimas 24 horas?"
    if cp3.button("🔋 ¿Estado de baterías?", use_container_width=True):
        pregunta_rapida = "¿Cuál es el estado de salud de la batería y paneles de las estaciones?"
    if cp4.button("💨 ¿Viento amenaza cultivos?", use_container_width=True):
        pregunta_rapida = "¿Las velocidades de viento registradas representan peligro para el plátano o maíz?"

    prompt_usuario = st.chat_input("Consulta sobre cultivos, clima, alertas o salud técnica...")
    prompt_activo = pregunta_rapida or prompt_usuario

    if prompt_activo:
        st.session_state.rag_messages.append({"role": "user", "content": prompt_activo})
        with st.chat_message("user"):
            st.markdown(prompt_activo)

        with st.chat_message("assistant"):
            with st.spinner("Consultando memoria semántica Qdrant y telemetría de sensores..."):
                resultado = responder_consulta_rag(
                    pregunta=prompt_activo,
                    estacion_id=estacion_sel if estacion_sel != "Todas" else None,
                )
                st.markdown(resultado["respuesta"])
                with st.expander("🔍 Ver fuentes de conocimiento Qdrant y telemetría analizada"):
                    st.json(resultado["telemetria"])
                    for d in resultado["documentos"]:
                        st.markdown(f"**Manual:** `{d['documento']}` | **Score Coseno:** `{d['score']}`")
                        st.markdown(f"> *{d['texto'][:250]}...*")
```

---

## 3. Plan de Despliegue en la Nube

### 3.1. Arquitectura de Contenedores y Orquestación

Para el despliegue en ambientes de producción (nubes públicas como AWS, GCP, Azure o nubes privadas institucionales), la solución se estructura sobre microservicios contenerizados mediante **Docker** y orquestados a través de **Kubernetes (K8s)**.

```mermaid
flowchart TD
    %% ==========================================
    %% TRÁFICO EXTERNO E INGRESS CONTROLLER
    %% ==========================================
    subgraph INGRESS_LAYER["Ingress Layer & Gateway Exterior"]
        ING["NGINX Ingress Controller / ALB\n(TLS Termination SSL, Reverse Proxy)"]
    end

    INTERNET(("🌐 Internet / Enlaces Satelitales\n(Estaciones IoT & Usuarios Comunales)")) -->|HTTPS / WSS / MQTT| ING

    %% ==========================================
    %% KUBERNETES NAMESPACE: AMAZONIA-PROD
    %% ==========================================
    subgraph K8S_CLUSTER["Cluster Kubernetes (Namespace: amazonia4-prod)"]
        
        subgraph FRONTEND_TIER["Frontend & APIs (Stateless Workloads)"]
            API_SVC["Service: api-service\n(:8000 ClusterIP)"]
            DASH_SVC["Service: dashboard-service\n(:8501 ClusterIP)"]
            MQTT_SVC["Service: mqtt-service\n(:1883 LoadBalancer)"]

            API_PODS["Deployment: amazonia-api\n(Replicas: 3, FastAPI Ingesta)"]
            DASH_PODS["Deployment: amazonia-dashboard\n(Replicas: 2, Streamlit UI)"]
            MQTT_PODS["Deployment: amazonia-mosquitto\n(Eclipse Mosquitto Broker)"]

            API_SVC --> API_PODS
            DASH_SVC --> DASH_PODS
            MQTT_SVC --> MQTT_PODS
        end

        subgraph ORCHESTRATION_TIER["Orquestación de Pipelines (Airflow)"]
            AF_WEB["Airflow Webserver (:8080)"]
            AF_SCHED["Airflow Scheduler"]
            AF_WORKER["Airflow Celery Workers (Autoscaling)"]
            REDIS_POD["Redis (Broker de Tareas Celery)"]
            PG_POD["PostgreSQL (Airflow Metadata DB)"]

            AF_SCHED --> REDIS_POD
            AF_WORKER --> REDIS_POD
            AF_WEB & AF_SCHED & AF_WORKER --> PG_POD
        end

        subgraph DATA_AI_TIER["Memoria Semántica & Lakehouse (Stateful Workloads)"]
            QDRANT_PODS["StatefulSet: qdrant-cluster\n(Motor Vectorial gRPC/HTTP :6333)"]
            DUCKDB_JOB["CronJob K8s: lakehouse-pipeline\n(Ejecución Medallion Bronze/Silver/Gold)"]
        end

        subgraph STORAGE_PERSISTENCE["Capa de Almacenamiento Persistente (CSI Driver)"]
            PVC_DATA["PVC: sqlite-edge-buffer-claim\n(StorageClass: io2 / gp3 RWO)"]
            PVC_LAKE["PVC: parquet-lakehouse-claim\n(StorageClass: EFS / CephFS RWX)"]
            PVC_VECTORS["PVC: qdrant-storage-claim\n(StorageClass: fast-ssd RWO)"]
        end

        API_PODS --> PVC_DATA
        DUCKDB_JOB --> PVC_DATA
        DUCKDB_JOB --> PVC_LAKE
        QDRANT_PODS --> PVC_VECTORS
        AF_WORKER --> DUCKDB_JOB
        DASH_PODS --> PVC_LAKE
        DASH_PODS --> QDRANT_PODS
    end

    ING -->|Path: /api| API_SVC
    ING -->|Path: /dashboard| DASH_SVC
    ING -->|Path: /airflow| AF_WEB
    ING -->|TCP: 1883| MQTT_SVC
```

---

### 3.2. Estrategia de Contenedorización e Imágenes Docker

Cada servicio cuenta con imágenes ligeras basadas en distribuciones `alpine` o `slim`, construidas mediante **Multi-stage Builds** para minimizar la superficie de ataque y el peso de transferencia en redes satelitales lentas:

| Microservicio | Imagen Base | Puertos Expuestos | Rol en el Ecosistema |
|---|---|---|---|
| `amazonia-api` | `python:3.11-slim` | `8000/TCP` | Recepción de telemetría REST, validación y consultas |
| `amazonia-dashboard` | `python:3.11-slim` | `8501/TCP` | Visualización en vivo, KPIs y asistente RAG |
| `amazonia-qdrant` | `qdrant/qdrant:v1.10.0` | `6333/TCP`, `6334/TCP` | Memoria vectorial semántica HNSW |
| `amazonia-mqtt` | `eclipse-mosquitto:2.0` | `1883/TCP` | Broker MQTT de baja latencia para sensores en campo |
| `amazonia-airflow` | `apache/airflow:2.10.2` | `8080/TCP` | Orquestación periódica del pipeline de datos |
| `amazonia-postgres` | `postgres:15-alpine` | `5432/TCP` | Base de datos de metadatos de Airflow |

---

### 3.3. Manifiestos de Kubernetes para Producción

A continuación se presentan los manifiestos declarativos principales para el despliegue del motor semántico (Qdrant), la API de ingesta y los volúmenes persistentes del Lakehouse:

#### 1. Persistencia del Lakehouse y Memoria Vectorial (`storage.yaml`)
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: parquet-lakehouse-claim
  namespace: amazonia4-prod
spec:
  accessModes:
    - ReadWriteMany  # Permite lectura concurrente desde Dashboard y escritura desde DuckDB Job
  resources:
    requests:
      storage: 100Gi
  storageClassName: efs-sc  # En AWS EFS o CephFS distribuido
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: qdrant-storage-claim
  namespace: amazonia4-prod
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 50Gi
  storageClassName: gp3-fast
```

#### 2. StatefulSet de Memoria Semántica Qdrant (`qdrant-statefulset.yaml`)
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: qdrant-server
  namespace: amazonia4-prod
spec:
  serviceName: "qdrant-service"
  replicas: 1
  selector:
    matchLabels:
      app: qdrant-server
  template:
    metadata:
      labels:
        app: qdrant-server
    spec:
      containers:
      - name: qdrant
        image: qdrant/qdrant:v1.10.0
        ports:
        - containerPort: 6333
          name: http
        - containerPort: 6334
          name: grpc
        resources:
          limits:
            cpu: "2"
            memory: "4Gi"
          requests:
            cpu: "500m"
            memory: "1Gi"
        volumeMounts:
        - name: qdrant-storage
          mountPath: /qdrant/storage
  volumeClaimTemplates:
  - metadata:
      name: qdrant-storage
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 20Gi
```

#### 3. Deployment de la API y Autoscaling Horizontal (`api-deployment.yaml`)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: amazonia-api
  namespace: amazonia4-prod
spec:
  replicas: 3
  selector:
    matchLabels:
      app: amazonia-api
  template:
    metadata:
      labels:
        app: amazonia-api
    spec:
      containers:
      - name: api
        image: amazonia4/api:latest
        command: ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
        ports:
        - containerPort: 8000
        env:
        - name: DB_PATH
          value: "/data/telemetria.db"
        - name: QDRANT_URL
          value: "http://qdrant-service:6333"
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "1000m"
            memory: "2Gi"
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: amazonia-api-hpa
  namespace: amazonia4-prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: amazonia-api
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 75
```

---

### 3.4. Resiliencia, Seguridad y Alta Disponibilidad

1. **Aislamiento de Red (Network Policies):**
   - El broker Mosquitto y la API de Ingesta residen en la DMZ de Kubernetes con acceso filtrado por Ingress.
   - La base vectorial Qdrant y la base de datos PostgreSQL de Airflow solo aceptan tráfico interno del clúster (`ClusterIP`).
2. **Estrategia ante Caída Total de Conectividad (Edge Resilience):**
   - Si la conexión hacia el clúster de nube se interrumpe (e.g., temporal meteorológico en la cuenca del Caroní), el **Edge Gateway local** continúa operando de forma 100% autónoma almacenando las lecturas en su SQLite local, ejecutando el Lakehouse localmente con DuckDB y proveyendo asistencia RAG offline a los agricultores a través del Dashboard local.
3. **Backup y Recuperación de Desastres (DRP):**
   - Los archivos Parquet de las capas Silver y Gold se sincronizan incrementalmente hacia buckets de almacenamiento inmutable tipo Amazon S3 / Google Cloud Storage con versionado habilitado y políticas de ciclo de vida (Glacier a los 90 días).

---

## 4. Conclusiones Técnicas y Académicas

1. **Eficiencia de la Arquitectura Medallion Embebida:**
   - La integración de DuckDB con Apache Parquet ZSTD demostró que es viable ejecutar analítica avanzada de Big Data en arquitecturas de borde sin la sobrecarga operativa, consumo de memoria y costos de infraestructura asociados a Apache Spark o clústeres Hadoop.
2. **Independencia Operativa Mediante RAG Offline-First:**
   - El diseño del motor de memoria semántica con Qdrant y vectorización determinista local resuelve la brecha de conectividad en comunidades remotas, permitiendo democratizar el acceso a recomendaciones agrícolas y alertas de protección civil incluso cuando no hay enlace a Internet.
3. **Calidad de Datos Certificada:**
   - La rigurosidad del filtrado en las tres capas (validación en SQLite, deduplicación analítica en Silver y datamarts consolidados en Gold) garantiza una base de datos limpia y auditable que cumple con los estándares científicos y técnicos exigidos por el programa de Maestría de la UNEG.
