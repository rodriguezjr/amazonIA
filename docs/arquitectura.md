# 🏗️ Arquitectura de AmazonIA 4.0

## Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Las Cinco Capas Funcionales](#las-cinco-capas-funcionales)
3. [Diagrama de Bloques](#diagrama-de-bloques)
4. [Flujo de Datos](#flujo-de-datos)
5. [Decisiones de Diseño](#decisiones-de-diseño)

---

## Visión General

AmazonIA 4.0 es una arquitectura de datos inteligente en el borde (Edge Computing) diseñada para el procesamiento de telemetría microclimática en la cuenca del río Caroní. Se estructura en **cinco capas funcionales** que trabajan de manera articulada para transformar datos crudos en conocimiento accionable.

---

## Las Cinco Capas Funcionales

| Capa | Componentes | Función |
| :--- | :--- | :--- |
| **1. Ingesta** | ESP32, Edge Gateway, LoRaWAN/MQTT, Kafka | Captura de datos heterogéneos |
| **2. Almacenamiento** | MinIO, Delta Lake, Medallion (Bronze/Silver/Gold) | Persistencia escalable |
| **3. Procesamiento** | Spark, Airflow, Python | Transformación y validación |
| **4. Analítica/IA** | Qdrant, RAG, Modelos ML | Interpretación contextual |
| **5. Consumo** | Streamlit, Alertas, Agentes IA | Interfaces para usuarios |

---

## Diagrama de Bloques

```mermaid
flowchart TB
    subgraph Fuentes["🌿 Fuentes de Datos"]
        A1[Sensores IoT]
        A2[Satélites]
        A3[Comunidades]
    end
    subgraph Ingesta["📡 Ingesta"]
        B1[Edge Gateway]
        B2[LoRaWAN / MQTT]
        B3[Kafka / NiFi]
    end
    subgraph Almacenamiento["💾 Lakehouse"]
        C1[Bronze]
        C2[Silver]
        C3[Gold]
    end
    subgraph Procesamiento["⚙️ Procesamiento"]
        D1[Spark]
        D2[Airflow]
        D3[Python]
    end
    subgraph Analitica["🧠 IA"]
        E1[Qdrant]
        E2[RAG]
        E3[ML]
    end
    subgraph Consumo["📊 Consumo"]
        F1[Streamlit]
        F2[Alertas]
        F3[Agentes IA]
    end
    A1 --> B1
    A2 --> B3
    A3 --> B3
    B1 --> B2
    B2 --> B3
    B3 --> C1
    C1 --> D1
    D1 --> C2
    C2 --> D2
    D2 --> C3
    C3 --> E1
    E1 --> E2
    E2 --> E3
    E3 --> F1
    E3 --> F2
    E3 --> F3
Flujo de Datos
Captura: Los sensores ESP32 capturan variables microclimáticas cada 5-15 minutos.

Transmisión: Los datos se envían vía LoRaWAN al Edge Gateway.

Validación local: El Gateway valida la calidad y almacena en SQLite.

Sincronización: Cuando hay conexión, los datos se sincronizan por lotes con Kafka.

Almacenamiento: Los datos se almacenan en MinIO/Delta Lake (Bronze).

Procesamiento: Spark limpia y valida los datos (Silver → Gold).

Analítica: Qdrant y RAG transforman los datos en conocimiento contextualizado.

Consumo: Streamlit despliega el dashboard y las alertas.

Decisiones de Diseño
Decisión	Justificación	Referencia
Edge Computing	Reducir latencia y dependencia de la nube	Shi et al. (2016)
Offline-First	Garantizar continuidad operativa	Trinc (2026)
Calidad de datos	Asegurar confiabilidad científica	Buelvas et al. (2023)
Autonomía energética	Operar sin red eléctrica	Luo et al. (2024)
Microclima	Capturar variaciones ultralocales	Srbinovska et al. (2015)
Lakehouse	Persistencia escalable	Arafat et al. (2025), Griffioen et al. (2026)
Airflow	Orquestación de pipelines	Sserujongi et al. (2025)
RAG	Interpretación contextual	Pourzangbar & Franca (2026)
Gobernanza	Proteger datos indígenas	Mamani (2025)