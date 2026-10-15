# ============================================
# IMAGEN BASE
# ============================================
FROM python:3.11-slim

# ============================================
# METADATOS
# ============================================
LABEL maintainer="Barrios Massiel, Pizzanni Luis, Rodríguez Junior"
LABEL description="AmazonIA 4.0 - MVP de Arquitectura de Datos Inteligente"
LABEL version="1.0.0"

# ============================================
# VARIABLES DE ENTORNO
# ============================================
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    APP_HOME=/app

# ============================================
# DIRECTORIO DE TRABAJO
# ============================================
WORKDIR $APP_HOME

# ============================================
# DEPENDENCIAS DEL SISTEMA
# ============================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# ============================================
# DEPENDENCIAS DE PYTHON
# ============================================
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# ============================================
# CÓDIGO FUENTE
# ============================================
COPY src/ ./src/
COPY tests/ ./tests/
COPY pyproject.toml .
COPY .env.example .env

# ============================================
# CREAR DIRECTORIOS
# ============================================
RUN mkdir -p data logs

# ============================================
# EXPONER PUERTOS
# ============================================
EXPOSE 8000 8501

# ============================================
# USUARIO NO-ROOT (seguridad)
# ============================================
RUN useradd -m -u 1000 amazonia && \
    chown -R amazonia:amazonia $APP_HOME
USER amazonia

# ============================================
# COMANDO POR DEFECTO
# ============================================
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]