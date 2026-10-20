# ============================================
# AMAZONIA 4.0 - MAKEFILE
# ============================================

.PHONY: help install install-dev test lint format clean run-api run-dashboard run-simulador docker-up docker-down docker-logs sync init-db

# ============================================
# AYUDA
# ============================================
help:
	@echo "Comandos disponibles:"
	@echo "  make install         - Instalar dependencias de producción"
	@echo "  make install-dev     - Instalar dependencias de desarrollo"
	@echo "  make test            - Ejecutar pruebas unitarias"
	@echo "  make lint            - Ejecutar linter (ruff)"
	@echo "  make format          - Formatear código (black + ruff)"
	@echo "  make clean           - Limpiar archivos temporales"
	@echo "  make init-db         - Inicializar base de datos"
	@echo "  make run-api         - Iniciar API de ingesta"
	@echo "  make run-dashboard   - Iniciar dashboard"
	@echo "  make run-simulador   - Iniciar simulador"
	@echo "  make sync            - Ejecutar sincronización por lotes"
	@echo "  make docker-up       - Levantar servicios con Docker"
	@echo "  make docker-down     - Detener servicios Docker"
	@echo "  make docker-logs     - Ver logs de Docker"

# ============================================
# INSTALACIÓN
# ============================================
install:
	pip install --upgrade pip
	pip install -r requirements.txt

install-dev: install
	pip install pytest pytest-cov black ruff mypy pre-commit
	pre-commit install

# ============================================
# PRUEBAS
# ============================================
test:
	pytest tests/ -v --cov=src --cov-report=html

test-fast:
	pytest tests/ -v --tb=short

# ============================================
# CALIDAD DE CÓDIGO
# ============================================
lint:
	ruff check src/ tests/

format:
	black src/ tests/
	ruff check --fix src/ tests/

# ============================================
# LIMPIEZA
# ============================================
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	rm -rf build/ dist/ *.egg-info/

# ============================================
# BASE DE DATOS
# ============================================
init-db:
	python -c "from src.database import init_database; init_database()"
	@echo "✅ Base de datos inicializada"

# ============================================
# EJECUCIÓN
# ============================================
start:
	python start.py

run-api:
	uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload

run-dashboard:
	streamlit run src/dashboard.py

run-simulador:
	python -m src.simulador

sync:
	python -c "from src.sincronizacion import sincronizar_todo; print(sincronizar_todo())"

run-mqtt-pub:
	python -m src.mqtt_publicador

run-mqtt-bridge:
	python -m src.mqtt_suscriptor

# ============================================
# DOCKER
# ============================================
docker-up:
	docker-compose up --build -d
	@echo "✅ Servicios levantados"
	@echo "   API: http://localhost:8000/docs"
	@echo "   Dashboard: http://localhost:8501"
	@echo "   Airflow: http://localhost:8080"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker system prune -f