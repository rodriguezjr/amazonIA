# 🌿 AmazonIA 4.0 — Arquitectura de Datos Inteligente en el Borde

**Diseño de Arquitectura de Datos Inteligente en el Borde para el Procesamiento de Telemetría Microclimática en Estaciones Meteorológicas Comunitarias en la Cuenca del Río Caroní**

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.40-red.svg)](https://streamlit.io/)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-lightblue.svg)](https://www.sqlite.org/)
[![Pytest](https://img.shields.io/badge/Pytest-8.3-orange.svg)](https://pytest.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Tabla de Contenidos

- [Descripción General](#-descripción-general)
- [Problemática](#-problemática)
- [Arquitectura del MVP](#️-arquitectura-del-mvp)
- [Tecnologías Utilizadas](#️-tecnologías-utilizadas)
- [Estructura del Repositorio](#-estructura-del-repositorio)
- [Requisitos Previos](#-requisitos-previos)
- [Instalación](#-instalación)
- [Uso del Sistema](#-uso-del-sistema)
- [API Reference](#-api-reference)
- [Pruebas Unitarias](#-pruebas-unitarias)
- [Variables de Entorno](#-variables-de-entorno)
- [Estructura de la Base de Datos](#-estructura-de-la-base-de-datos)
- [Autores](#-autores)
- [Referencias](#-referencias)

---

## 🌎 Descripción General

**AmazonIA 4.0** es una arquitectura de datos inteligente en el borde *(Edge Computing)* diseñada para el procesamiento de telemetría microclimática en estaciones meteorológicas comunitarias de la cuenca del río Caroní, estado Bolívar, Venezuela.

El sistema aborda la **asimetría de información** en la región mediante:

- **Edge Computing** — procesamiento local de datos en el borde de la red
- **Offline-First** — buffer SQLite que garantiza continuidad operativa ante cortes de red
- **Validación de calidad** en tiempo real, clasificando datos como `valid`, `invalid` o `incomplete`
- **Sincronización por lotes** — reenvío automático con reintentos y backoff exponencial
- **Visualización interactiva** — dashboard en tiempo real con alertas configurables

Este repositorio contiene el **Producto Mínimo Viable (MVP)** que implementa los componentes esenciales de la arquitectura.

---

## 🔍 Problemática

La cuenca del río Caroní enfrenta:

- **Escasez de estaciones de monitoreo** microclimático en zonas rurales
- **Inestabilidad eléctrica** que provoca pérdida de telemetría
- **Conectividad intermitente** que impide transmisión continua a la nube
- **Dependencia de arquitecturas centralizadas** sin resiliencia local
- **Datos sin interpretación contextual** para las comunidades locales

Las consecuencias incluyen series temporales incompletas, imposibilidad de generar alertas tempranas y decisiones agrícolas basadas en percepciones subjetivas.

---

## 🏗️ Arquitectura del MVP

El MVP implementa las siguientes capas:

```
┌─────────────────────────────────────────────────────────────┐
│  FUENTES DE DATOS                                           │
│  Sensores IoT (simulados: CARONI-001, CARONI-002)           │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTP POST / MQTT (paho-mqtt)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  CAPA DE INGESTA — Edge Gateway                             │
│  FastAPI  ·  Motor de Validación  ·  Mosquitto MQTT         │
│  ─────────────── Offline-First ─────────────────            │
└─────────────────────────┬───────────────────────────────────┘
                          │ SQLite (buffer local)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  PERSISTENCIA LOCAL                                         │
│  SQLite  ·  Sincronización por Lotes (HTTP batch)           │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  CAPA DE CONSUMO                                            │
│  Streamlit Dashboard  ·  Alertas en tiempo real             │
└─────────────────────────────────────────────────────────────┘
```

### Flujo de calidad de datos

Cada lectura se clasifica según el marco de Buelvas et al. (2023):

| Estado | Condición |
|---|---|
| `valid` | Todos los campos presentes y dentro de rangos físicos |
| `invalid` | Valor fuera de rango o tipo de dato incorrecto |
| `incomplete` | Campo numérico ausente (pero campos obligatorios presentes) |

---

## 🛠️ Tecnologías Utilizadas

| Componente | Tecnología | Versión | Función |
|:---|:---|:---|:---|
| **API de Ingesta** | FastAPI | 0.115 | Recepción y validación de telemetría |
| **Servidor ASGI** | Uvicorn | 0.32 | Servidor de producción para FastAPI |
| **Modelos de datos** | Pydantic | 2.9 | Validación y serialización de esquemas |
| **Persistencia local** | SQLite | 3.x | Buffer local Offline-First |
| **Dashboard** | Streamlit | 1.40 | Visualización interactiva en tiempo real |
| **Gráficos** | Plotly | 5.24 | Gráficos y series temporales interactivos |
| **Análisis de datos** | Pandas | 2.x | Manipulación tabular en el dashboard |
| **Mensajería IoT** | Mosquitto (MQTT) | 2.x | Broker para publicación de sensores |
| **Cliente MQTT** | paho-mqtt | 2.x | Publicador y suscriptor en Python |
| **Sincronización** | requests / httpx | 2.32 / 0.28 | Sincronización por lotes vía HTTP |
| **Contenedores** | Docker + Compose | Latest | Despliegue portable de servicios |
| **Pruebas** | Pytest | 8.3 | Suite de pruebas unitarias (25 tests) |
| **Lenguaje** | Python | 3.11+ | Desarrollo principal |

---

## 📁 Estructura del Repositorio

```
amazonia4.0-mvp/
│
├── README.md                    # Este archivo
├── requirements.txt             # Dependencias de Python
├── proyect.toml                 # Configuración de herramientas (pytest, ruff, black)
├── docker-compose.yml           # Despliegue completo con Docker
├── dockerfile                   # Imagen base del proyecto
├── makefile                     # Comandos de desarrollo
├── .env.example                 # Variables de entorno de ejemplo
├── .gitignore
│
├── src/                         # Código fuente del MVP
│   ├── __init__.py
│   ├── config.py                # Configuración central y rangos de validación
│   ├── database.py              # Persistencia SQLite (Offline-First)
│   ├── validacion.py            # Motor de calidad de datos
│   ├── api.py                   # API de ingesta (FastAPI)
│   ├── simulador.py             # Simulador de estaciones (HTTP)
│   ├── mqtt_publicador.py       # Publicador MQTT de sensores hacia Mosquitto
│   ├── mqtt_suscriptor.py       # Suscriptor y Bridge MQTT hacia SQLite
│   ├── sincronizacion.py        # Sincronización por lotes con reintentos
│   └── dashboard.py             # Dashboard interactivo (Streamlit)
│
├── tests/                       # Pruebas unitarias (32 tests)
│   ├── __init__.py
│   ├── test_api.py              # Pruebas de endpoints FastAPI (7 tests)
│   ├── test_database.py         # Pruebas de persistencia SQLite (7 tests)
│   ├── test_validacion.py       # Pruebas del motor de calidad (11 tests)
│   └── test_mqtt.py             # Pruebas de publicación y bridge MQTT (7 tests)
│
├── dags/                        # DAGs de Apache Airflow
│   ├── pipeline_amazonia.py     # DAG principal del pipeline
│   ├── utils/
│   ├── scripts/
│   └── config/
│
├── mosquitto/                   # Configuración del broker MQTT
│   └── config/
│       └── mosquitto.conf
│
└── data/                        # Generado en tiempo de ejecución
    └── telemetria.db            # Base de datos SQLite
```

---

## ✅ Requisitos Previos

- **Python 3.11 o superior** — [Descargar](https://www.python.org/downloads/)
- **Git** — [Descargar](https://git-scm.com/)
- **Docker** *(opcional, para despliegue con contenedores)* — [Descargar](https://www.docker.com/)

```bash
python --version    # Python 3.11+
git --version       # git 2.x
docker --version    # Opcional
```

---

## 🚀 Instalación

### Opción 1: Instalación Local

```bash
# 1. Clonar el repositorio
git clone https://github.com/rodriguezjr/amazonIA.git
cd amazonia4.0-mvp

# 2. Crear entorno virtual
python -m venv .venv

# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

# 3. Instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configurar variables de entorno
cp .env.example .env

# 5. Inicializar la base de datos
python -c "from src.database import init_database; init_database()"

# 6. Verificar instalación (deben pasar 32 tests)
pytest tests/ -v
```

---

## 🎯 Despliegue Rápido Local (Un Solo Comando)

Para evitar abrir múltiples terminales, el proyecto cuenta con un **orquestador unificado**:

```bash
# Iniciar todo (API + Dashboard + Simulador) en una sola ventana:
python start.py
```
> En Windows también puedes simplemente hacer **doble clic en `start.bat`**.

Esto automáticamente:
1. Verifica e inicializa la base de datos SQLite si es necesario.
2. Inicia la **API FastAPI** en segundo plano (`:8000`).
3. Inicia el **Dashboard Streamlit** (`:8501`).
4. Inicia el **Simulador de telemetría**.
5. Abre tu navegador automáticamente en el Dashboard.
6. Al presionar **Ctrl+C**, apaga y limpia todos los procesos de forma segura.

**Opciones de ejecución rápida:**
```bash
python start.py --modo http           # Modo REST API por defecto
python start.py --modo mqtt           # Inicia Bridge MQTT y Publicador Mosquitto
python start.py --sin-simulador       # Inicia solo API y Dashboard (sin datos simulados)
```

---

## 🛠️ Uso Manual del Sistema (Módulo por Módulo)

Si prefieres ejecutar cada componente por separado en terminales independientes:

### 1. Iniciar la API de Ingesta

```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
```

Verificar que está funcionando:

```bash
curl http://localhost:8000/health
# {"status": "ok", "timestamp": "2026-10-15T10:30:00"}
```

La documentación interactiva (Swagger UI) está disponible en: **http://localhost:8000/docs**

### 2. Ejecutar el Simulador de Telemetría

El simulador genera datos sintéticos realistas para las estaciones `CARONI-001` y `CARONI-002`, enviando un 80% de lecturas válidas, 10% inválidas y 10% incompletas.

```bash
python -m src.simulador
```

Salida esperada:

```
[SIMULADOR] Iniciando estación CARONI-001
[CARONI-001] 2026-10-15T10:30:00 → valid
[CARONI-001] 2026-10-15T10:30:10 → valid
[CARONI-001] 2026-10-15T10:30:20 → invalid
[CARONI-001] 2026-10-15T10:30:30 → incomplete | 1 alerta(s)
```

### 3. Iniciar el Dashboard

```bash
streamlit run src/dashboard.py
# Abre http://localhost:8501
```

El dashboard incluye:
- 📊 **Indicadores actuales** — temperatura, humedad, precipitación y voltaje de batería
- 📈 **Series temporales** — temperatura, humedad y precipitación
- ✅ **Distribución de calidad** — gráfico de torta `valid / invalid / incomplete`
- 📋 **Tabla de datos recientes** — últimas 20 lecturas
- 🚨 **Alertas activas** — lluvia intensa, calor extremo, batería baja, viento fuerte

### 4. Sincronización por Lotes

La sincronización envía los datos acumulados en SQLite al endpoint batch de la API:

```bash
python -c "
from src.sincronizacion import sincronizar_todo
resultado = sincronizar_todo()
print(resultado)
"
```

Salida esperada:

```json
{
  "total_enviados": 150,
  "total_exitosos": 150,
  "estado": "completado"
}
```

### 5. Ingesta y Publicación mediante MQTT (Mosquitto)

El proyecto incluye soporte nativo para mensajería IoT con Eclipse Mosquitto:

```
Sensor (mqtt_publicador) ──[MQTT pub]──> Mosquitto (:1883) ──[MQTT sub]──> Bridge (mqtt_suscriptor) ──> SQLite
                                                                                  │
                                                                           [alerta pub]
                                                                                  ↓
                                                                     amazonia/caroni/{id}/alertas
```

**Paso 1: Levantar el broker Mosquitto (Docker)**
```bash
docker-compose up -d mosquitto
```

**Paso 2: Iniciar el Bridge MQTT a SQLite (en una terminal)**
```bash
python -m src.mqtt_suscriptor
```

**Paso 3: Iniciar el Publicador de Sensores MQTT (en otra terminal)**
```bash
# Publicar estación CARONI-001 cada 5 segundos
python -m src.mqtt_publicador --estacion CARONI-001 --intervalo 5
```

**Tópicos MQTT utilizados:**
| Tópico | Dirección | Descripción |
|---|---|---|
| `amazonia/caroni/{id}/telemetria` | Sensor → Broker | Lecturas crudas del sensor en JSON |
| `amazonia/caroni/{id}/alertas` | Bridge → Broker | Alertas críticas republicadas en tiempo real |
| `amazonia/caroni/{id}/status` | Sensor → Broker | Estado operativo de la estación |

---

## 📡 API Reference

### Endpoints disponibles

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/` | Información de la API |
| `GET` | `/health` | Estado del sistema |
| `POST` | `/ingesta` | Ingestar un registro individual |
| `POST` | `/ingesta/batch` | Ingestar un lote de registros |
| `GET` | `/telemetria/ultimas` | Últimas lecturas (paginado) |
| `GET` | `/telemetria/estadisticas` | Estadísticas de calidad |
| `GET` | `/telemetria/estaciones` | Lista de estaciones registradas |
| `GET` | `/sincronizacion/pendientes` | Registros pendientes de sincronizar |

### Ejemplo: Ingestar telemetría

```bash
curl -X POST http://localhost:8000/ingesta \
  -H "Content-Type: application/json" \
  -d '{
    "estacion_id": "CARONI-001",
    "timestamp": "2026-10-15T10:30:00",
    "temperatura": 28.5,
    "humedad": 75.0,
    "presion": 1013.25,
    "precipitacion": 0.0,
    "viento": 5.2,
    "voltaje_bateria": 3.8,
    "rssi": -85
  }'
```

Respuesta:

```json
{
  "id": 42,
  "estado_calidad": "valid",
  "errores": null,
  "alertas": []
}
```

### Rangos de validación

| Variable | Mínimo | Máximo | Unidad |
|---|---|---|---|
| `temperatura` | -10.0 | 45.0 | °C |
| `humedad` | 0.0 | 100.0 | % |
| `presion` | 800.0 | 1100.0 | hPa |
| `precipitacion` | 0.0 | 500.0 | mm |
| `viento` | 0.0 | 150.0 | km/h |
| `voltaje_bateria` | 3.0 | 4.2 | V |
| `rssi` | -120 | 0 | dBm |

### Umbrales de alerta

| Alerta | Umbral | Severidad |
|---|---|---|
| Lluvia intensa | precipitación > 50 mm | Alta |
| Calor extremo | temperatura > 40 °C | Alta |
| Batería baja | voltaje < 3.2 V | Media |
| Viento fuerte | viento > 80 km/h | Media |

---

## 🧪 Pruebas Unitarias

El proyecto cuenta con **32 tests** distribuidos en 4 módulos:

```bash
# Ejecutar todos los tests
pytest tests/ -v

# Con reporte de cobertura
pytest tests/ -v --cov=src --cov-report=html
```

| Archivo | Tests | Cobertura |
|---|---|---|
| `test_api.py` | 7 | Endpoints FastAPI, ingesta individual y batch |
| `test_database.py` | 7 | Inserción, consulta y sincronización SQLite |
| `test_validacion.py` | 11 | Motor de calidad y sistema de alertas |
| `test_mqtt.py` | 7 | Publicación, bridge y persistencia MQTT |

Resultado esperado: **`32 passed`**

---

## ⚙️ Variables de Entorno

Copia `.env.example` a `.env` y ajusta según tu entorno:

```bash
cp .env.example .env
```

| Variable | Por defecto | Descripción |
|---|---|---|
| `API_HOST` | `localhost` | Host de la API |
| `API_PORT` | `8000` | Puerto de la API |
| `DB_PATH` | `data/telemetria.db` | Ruta de la base de datos SQLite |
| `SYNC_ENDPOINT` | `http://localhost:8000/ingesta/batch` | Endpoint de sincronización |
| `SYNC_BATCH_SIZE` | `100` | Registros por lote de sincronización |
| `SYNC_MAX_RETRIES` | `3` | Reintentos ante fallo de conexión |
| `MQTT_BROKER` | `localhost` | Host del broker Mosquitto |
| `MQTT_PORT` | `1883` | Puerto MQTT |
| `MQTT_TOPIC_PREFIX` | `amazonia/caroni` | Prefijo de tópicos MQTT |

---

## 🗄️ Estructura de la Base de Datos

### Tabla `telemetria`

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INTEGER PK | Identificador autoincremental |
| `estacion_id` | TEXT | Identificador de la estación |
| `timestamp` | TEXT | Marca de tiempo ISO 8601 |
| `temperatura` | REAL | Temperatura en °C |
| `humedad` | REAL | Humedad relativa en % |
| `presion` | REAL | Presión atmosférica en hPa |
| `precipitacion` | REAL | Precipitación en mm |
| `viento` | REAL | Velocidad del viento en km/h |
| `voltaje_bateria` | REAL | Voltaje de batería en V |
| `rssi` | INTEGER | Intensidad de señal en dBm |
| `estado_calidad` | TEXT | `valid` / `invalid` / `incomplete` |
| `errores` | TEXT | Descripción de errores (nullable) |
| `sincronizado` | INTEGER | `0` pendiente / `1` sincronizado |
| `fecha_recepcion` | TEXT | Timestamp de recepción en el gateway |

### Tabla `log_sincronizacion`

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INTEGER PK | Identificador |
| `timestamp` | TEXT | Momento de la sincronización |
| `registros_enviados` | INTEGER | Total de registros enviados |
| `registros_exitosos` | INTEGER | Registros confirmados por el servidor |
| `estado` | TEXT | `exitoso` / `fallido` |
| `mensaje` | TEXT | Detalle del resultado |

---

## 👥 Autores

| Nombre | C.I. |
|---|---|
| Barrios, Massiel | 27.375.910 | 
| Pizzanni, Luis | 27.765.210 |
| Rodríguez, Junior | 23.730.853 |
**Profesora:** Profa. Borjas, Livia  
**Institución:** Universidad Nacional Experimental de Guayana (UNEG)  
**Programa:** Maestría en Tecnologías de la Información

---

## 📚 Referencias

- Buelvas et al. (2023). *Marco de calidad de datos para telemetría IoT en entornos rurales.*
- FastAPI. (2024). *FastAPI Documentation.* https://fastapi.tiangolo.com/
- Eclipse Foundation. (2024). *Eclipse Mosquitto MQTT Broker.* https://mosquitto.org/
- Streamlit Inc. (2024). *Streamlit Documentation.* https://docs.streamlit.io/
- Python Software Foundation. (2024). *Python 3.11 Documentation.* https://docs.python.org/3.11/
