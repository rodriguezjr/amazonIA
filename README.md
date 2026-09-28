# AmazonIA 4.0 - MVP

Diseño de Arquitectura de Datos Inteligente en el Borde para el Procesamiento
de Telemetría Microclimática en Estaciones Meteorológicas Comunitarias en la
Cuenca del Río Caroní.

## Estructura

- `src/`: Código fuente del MVP
- `tests/`: Pruebas unitarias
- `data/`: Base de datos SQLite
- `dags/`: DAGs de Airflow (fase futura)

## Instalación


pip install -r requirements.txt

## Ejecucion

Ejecución
1. Iniciar la API
uvicorn src.api:app --reload

2. Ejecutar el simulador
python -m src.simulador

3. Iniciar el dashboard
streamlit run src/dashboard.py

4. Ejecutar pruebas
pytest tests/ -v

Docker
docker-compose up --build

Tecnologías
FastAPI (API de ingesta)
SQLite (Persistencia local)
Streamlit (Dashboard)
Plotly (Visualización)
Pytest (Pruebas unitarias)

Autores
Barrios, Massiel
Pizzanni, Luis
Rodríguez, Junior

Institución
UNEG - Maestría en Tecnologías de la Información