# 🌿 AmazonIA 4.0 — Arquitectura de Datos Inteligente en el Borde

**Diseño de Arquitectura de Datos Inteligente en el Borde para el Procesamiento de Telemetría Microclimática en Estaciones Meteorológicas Comunitarias en la Cuenca del Río Caroní**

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.40-red.svg)](https://streamlit.io/)
[![Airflow](https://img.shields.io/badge/Airflow-2.10-blue.svg)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Tabla de Contenidos

- [Descripción General](#-descripción-general)
- [Problemática](#-problemática)
- [Arquitectura](#-arquitectura)
- [Tecnologías Utilizadas](#-tecnologías-utilizadas)
- [Estructura del Repositorio](#-estructura-del-repositorio)
- [Requisitos Previos](#-requisitos-previos)
- [Instalación](#-instalación)
  - [Opción 1: Instalación Local](#opción-1-instalación-local)
  - [Opción 2: Docker Compose](#opción-2-docker-compose)
  - [Opción 3: Docker + Airflow](#opción-3-docker--airflow)
- [Uso del Sistema](#-uso-del-sistema)
  - [1. Iniciar la API de Ingesta](#1-iniciar-la-api-de-ingesta)
  - [2. Ejecutar el Simulador de Telemetría](#2-ejecutar-el-simulador-de-telemetría)
  - [3. Iniciar el Dashboard](#3-iniciar-el-dashboard)
  - [4. Sincronización por Lotes](#4-sincronización-por-lotes)
  - [5. Orquestación con Airflow](#5-orquestación-con-airflow)
- [Pruebas Unitarias](#-pruebas-unitarias)
- [API Reference](#-api-reference)
- [Configuración](#-configuración)
- [Estructura de la Base de Datos](#-estructura-de-la-base-de-datos)
- [Solución de Problemas](#-solución-de-problemas)
- [Contribuciones](#-contribuciones)
- [Autores](#-autores)
- [Licencia](#-licencia)
- [Referencias](#-referencias)

---

## 🌎 Descripción General

**AmazonIA 4.0** es una arquitectura de datos inteligente en el borde (Edge Computing) diseñada para el procesamiento de telemetría microclimática en estaciones meteorológicas comunitarias de la cuenca del río Caroní, estado Bolívar, Venezuela.

El sistema aborda la **asimetría de información** en la región mediante la integración de:

- **Edge Computing** para el procesamiento local de datos
- **Offline-First** para garantizar la continuidad operativa ante cortes de red
- **Validación de calidad** en tiempo real (marco Data Quality)
- **Arquitectura Lakehouse** (Medallion: Bronze, Silver, Gold)
- **Orquestación con Apache Airflow**
- **Memoria semántica (RAG)** con bases vectoriales
- **Gobernanza y privacidad** (RBAC + anonimización)

Este repositorio contiene el **Producto Mínimo Viable (MVP)** que implementa los componentes esenciales de la arquitectura descrita en el informe técnico.

---

## 🔍 Problemática

La cuenca del río Caroní enfrenta:

- **Escasez de estaciones de monitoreo** microclimático
- **Inestabilidad eléctrica** que provoca pérdida de telemetría
- **Conectividad intermitente** que impide transmisión continua
- **Dependencia de arquitecturas centralizadas** en la nube
- **Datos aislados sin interpretación contextual** para las comunidades

Las consecuencias incluyen series temporales incompletas, imposibilidad de generar alertas tempranas y decisiones agrícolas basadas en percepciones subjetivas.

---

## 🏗️ Arquitectura

La arquitectura se estructura en **cinco capas funcionales**:
┌─────────────────────────────────────────────────────────────────┐
│ FUENTES DE DATOS                                                │
│ Sensores IoT │ Satélites │ Comunidades (audios, PDFs)           │
└─────────────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────────┐
│ CAPA DE INGESTA                                                 │
│ Edge Gateway (Raspberry Pi + SQLite) │ LoRaWAN/MQTT │ Kafka     │
│ ─────────── Offline-First ───────────                           │
└─────────────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────────┐
│ CAPA DE ALMACENAMIENTO (LAKEHOUSE)                              │
│ MinIO + Delta Lake                                              │
│ Bronze (crudo) → Silver (validado) → Gold (agregaciones)        │
└─────────────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────────┐
│ CAPA DE PROCESAMIENTO                                           │
│ Apache Spark │ Apache Airflow │ Python (validación)             │
└─────────────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────────┐
│ CAPA ANALÍTICA / IA                                             │
│ Qdrant (vectores) │ RAG │ Modelos ML                            │
└─────────────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────────┐
│ CAPA DE CONSUMO                                                 │
│ Streamlit (dashboard) │ Alertas (Telegram/SMS) │ Agentes IA     │
└─────────────────────────────────────────────────────────────────┘


**Gobernanza transversal:** RBAC + Anonimización

---

## 🛠️ Tecnologías Utilizadas

| Componente | Tecnología | Versión | Función |
| :--- | :--- | :--- | :--- |
| **API de Ingesta** | FastAPI | 0.115+ | Recepción y validación de telemetría |
| **Persistencia Local** | SQLite | 3.x | Buffer local Offline-First |
| **Dashboard** | Streamlit | 1.40+ | Visualización interactiva |
| **Visualización** | Plotly | 5.24+ | Gráficos interactivos |
| **Orquestación** | Apache Airflow | 2.10+ | Pipelines automatizados |
| **Procesamiento** | Apache Spark | 3.5+ | Transformación de datos |
| **Almacenamiento** | MinIO + Delta Lake | Latest | Lakehouse (Medallion) |
| **Base Vectorial** | Qdrant | Latest | Memoria semántica (RAG) |
| **Contenedores** | Docker + Compose | Latest | Despliegue portable |
| **Pruebas** | Pytest | 8.3+ | Pruebas unitarias |
| **Lenguaje** | Python | 3.11+ | Desarrollo principal |

---

## 📁 Estructura del Repositorio

amazonia4.0-mvp/
│
├── README.md # Este archivo
├── requirements.txt # Dependencias de Python
├── docker-compose.yml # Despliegue completo con Docker
├── docker-compose.airflow.yml # Despliegue de Airflow
├── .env.example # Variables de entorno de ejemplo
├── .gitignore # Archivos ignorados por Git
├── LICENSE # Licencia MIT
│
├── src/ # Código fuente del MVP
│ ├── init.py
│ ├── config.py # Configuración central
│ ├── database.py # Persistencia en SQLite
│ ├── validacion.py # Motor de calidad de datos
│ ├── api.py # API de ingesta (FastAPI)
│ ├── simulador.py # Simulador de estaciones
│ ├── sincronizacion.py # Sincronización por lotes
│ └── dashboard.py # Dashboard (Streamlit)
│
├── tests/ # Pruebas unitarias
│ ├── init.py
│ ├── test_validacion.py # Pruebas de validación
│ ├── test_api.py # Pruebas de API
│ └── test_database.py # Pruebas de base de datos
│
├── dags/ # DAGs de Airflow
│ ├── pipeline_amazonia.py # DAG principal
│ ├── utils/
│ │ └── edge_sync.py # Utilidades de sincronización
│ ├── scripts/
│ │ ├── bronze_to_silver.py # Spark: Bronze → Silver
│ │ ├── silver_to_gold.py # Spark: Silver → Gold
│ │ └── rag_indexer.py # Indexador para Qdrant
│ └── config/
│ └── airflow.cfg # Configuración de Airflow
│
├── data/ # Datos (generados en ejecución)
│ └── telemetria.db # Base de datos SQLite
│
├── docs/ # Documentación adicional
│ ├── arquitectura.md # Detalles de la arquitectura
│ ├── api_reference.md # Referencia de la API
│ └── manual_usuario.md # Manual de usuario
│
└── mosquitto/ # Configuración MQTT (fase futura)
└── config/
└── mosquitto.conf


---

## ✅ Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:

- **Python 3.11 o superior** — [Descargar](https://www.python.org/downloads/)
- **pip** (gestor de paquetes de Python)
- **Git** — [Descargar](https://git-scm.com/)
- **Docker** (opcional, para despliegue con contenedores) — [Descargar](https://www.docker.com/)
- **Docker Compose** (opcional) — [Descargar](https://docs.docker.com/compose/)

**Verificar instalación:**

```bash
python --version    # Debe mostrar Python 3.11+
pip --version       # Debe mostrar pip 23+
git --version       # Debe mostrar git 2.40+
docker --version    # Opcional
```


🚀 Instalación
Opción 1: Instalación Local
Paso 1: Clonar el repositorio


git clone https://github.com/tu-usuario/amazonia4.0-mvp.git
cd amazonia4.0-mvp
Paso 2: Crear entorno virtual


# Linux/macOS
python -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
Paso 3: Instalar dependencias


pip install --upgrade pip
pip install -r requirements.txt
Paso 4: Configurar variables de entorno


cp .env.example .env
# Editar .env según sea necesario
Paso 5: Inicializar la base de datos


python -c "from src.database import init_database; init_database()"
Paso 6: Verificar instalación


pytest tests/ -v
Si todas las pruebas pasan, la instalación fue exitosa.

Opción 2: Docker Compose
Paso 1: Clonar el repositorio


git clone https://github.com/tu-usuario/amazonia4.0-mvp.git
cd amazonia4.0-mvp
Paso 2: Construir y levantar los servicios


docker-compose up --build -d
Paso 3: Verificar que los servicios estén corriendo


docker-compose ps
Deberías ver:

text
NAME                    STATUS
amazonia-api            Up
amazonia-dashboard      Up
amazonia-simulador      Up
Paso 4: Acceder a los servicios

API: http://localhost:8000/docs

Dashboard: http://localhost:8501

Paso 5: Detener los servicios


docker-compose down
Opción 3: Docker + Airflow
Paso 1: Levantar los servicios base


docker-compose up --build -d
Paso 2: Levantar Airflow


docker-compose -f docker-compose.airflow.yml up -d
Paso 3: Inicializar Airflow (primera vez)


docker-compose -f docker-compose.airflow.yml exec airflow-webserver airflow db init
docker-compose -f docker-compose.airflow.yml exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@amazonia4.0.org
Paso 4: Acceder a Airflow

Airflow UI: http://localhost:8080

Usuario: admin

Contraseña: admin

Paso 5: Activar el DAG

En la UI de Airflow, busca el DAG amazonia_4_0_pipeline y actívalo con el toggle.


🎯 Uso del Sistema
1. Iniciar la API de Ingesta
La API recibe los datos de telemetría, los valida y los almacena en SQLite.

bash
# Opción local
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload

# Opción Docker (ya está corriendo)
# Acceder a http://localhost:8000/docs
Verificar que la API esté funcionando:

bash
curl http://localhost:8000/health
Respuesta esperada:

json
{"status": "ok", "timestamp": "2026-09-15T10:30:00"}
Documentación interactiva (Swagger UI):

Abre tu navegador en http://localhost:8000/docs para ver todos los endpoints disponibles.

2. Ejecutar el Simulador de Telemetría
El simulador genera datos sintéticos realistas para las estaciones CARONI-001 y CARONI-002.

bash
# Ejecutar simulador (100 iteraciones por estación)
python -m src.simulador

# O ejecutar indefinidamente (Ctrl+C para detener)
python -c "
from src.simulador import simular_estacion
simular_estacion('CARONI-001', intervalo_segundos=10)
"
Salida esperada:

text
[SIMULADOR] Iniciando estación CARONI-001
[CARONI-001] 2026-09-15T10:30:00 → valid
[CARONI-001] 2026-09-15T10:30:10 → valid
[CARONI-001] 2026-09-15T10:30:20 → invalid
[CARONI-001] 2026-09-15T10:30:30 → incomplete
...
3. Iniciar el Dashboard
El dashboard proporciona una interfaz web para visualizar los datos.

bash
streamlit run src/dashboard.py
Abre tu navegador en http://localhost:8501

Funcionalidades del dashboard:

📊 Indicadores actuales (temperatura, humedad, precipitación, batería)

📈 Series temporales interactivas

✅ Distribución de calidad de datos (valid, invalid, incomplete)

📋 Tabla de datos recientes

🚨 Alertas activas

4. Sincronización por Lotes
La sincronización por lotes envía los datos acumulados localmente al sistema central.

bash
# Sincronizar todos los registros pendientes
python -c "
from src.sincronizacion import sincronizar_todo
resultado = sincronizar_todo()
print(resultado)
"
Salida esperada:

json
{
  "total_enviados": 150,
  "total_exitosos": 150,
  "estado": "completado"
}
Ejecutar sincronización periódica (cada 5 minutos):

bash
while true; do
    python -c "from src.sincronizacion import sincronizar_todo; sincronizar_todo()"
    sleep 300
done
5. Orquestación con Airflow
El DAG de Airflow orquesta el flujo completo cada hora.

Acceder a la UI de Airflow:

URL: http://localhost:8080

Usuario: admin

Contraseña: admin

Activar el DAG:

Buscar amazonia_4_0_pipeline en la lista de DAGs

Activar el toggle (ON)

Hacer clic en "Trigger DAG" para ejecutar manualmente

Monitorear la ejecución:

Graph View: Ver el flujo de tareas

Tree View: Ver el historial de ejecuciones

Gantt: Ver los tiempos de ejecución

Tareas del DAG:

verificar_gateway — Verifica disponibilidad del Edge Gateway

sincronizar_datos_edge — Sincroniza datos del buffer local

evaluar_calidad — Branching según calidad (≥ 80%)

procesar_bronze_silver — Limpieza y validación

procesar_silver_gold — Agregaciones e indicadores

actualizar_qdrant — Indexación en base vectorial

enviar_alertas — Verificación de umbrales

notificar_calidad_baja — Ruta alternativa

finalizar — Cierre del DAG





🤝 Contribuciones
Las contribuciones son bienvenidas. Para contribuir:

Fork el repositorio

Crear una rama para tu feature (git checkout -b feature/nueva-funcionalidad)

Commit los cambios (git commit -am 'Añadir nueva funcionalidad')

Push a la rama (git push origin feature/nueva-funcionalidad)

Crear un Pull Request

Áreas de contribución:

Integración con hardware físico (ESP32)

Implementación de LoRaWAN

Modelos de machine learning para predicción

Ampliación del módulo RAG

Traducción a lenguas indígenas

👥 Autores
Nombre  C.I.  Email
Barrios, Massiel  27.375.910  massiel.barrios@uneg.edu.ve
Pizzanni, Luis  27.765.210  luis.pizzanni@uneg.edu.ve
Rodríguez, Junior 23.730.853  junior.rodriguez@uneg.edu.ve
Profesora: Profa. Borjas, Livia

Institución: Universidad Nacional Experimental de Guayana (UNEG) — Maestría en Tecnologías de la Información


