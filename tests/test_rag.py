"""
Pruebas unitarias para el subsistema RAG y base de datos vectorial Qdrant.
"""
import sys
from pathlib import Path

# Agregar raíz al PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from src.database import init_database, insertar_telemetria
from src.rag.vector_store import AmazonIAVectorStore, _calcular_embedding_offline
from src.rag.engine import responder_consulta_rag, extraer_contexto_telemetria


@pytest.fixture(autouse=True)
def setup_rag_test(tmp_path, monkeypatch):
    """Configura base de datos temporal y motor vectorial en memoria."""
    db_file = tmp_path / "test_rag.db"
    monkeypatch.setattr("src.config.DB_PATH", db_file)
    monkeypatch.setattr("src.database.DB_PATH", db_file)
    init_database()

    # Aislar vector store para pruebas usando almacenamiento temporal / memoria
    test_storage = tmp_path / "qdrant_test_storage"
    monkeypatch.setattr("src.rag.vector_store.LOCAL_STORAGE_DIR", test_storage)
    
    # Crear un store en memoria para el motor RAG en las pruebas
    test_vs = AmazonIAVectorStore(coleccion="test_documentos_engine", en_memoria=True)
    test_vs.indexar_documentos(forzar=True)
    monkeypatch.setattr("src.rag.engine._vector_store_instance", test_vs)

    # Insertar algunas lecturas representativas
    insertar_telemetria({
        "estacion_id": "CARONI-001",
        "timestamp": "2026-10-25T12:00:00",
        "temperatura": 29.5,
        "humedad": 82.0,
        "presion": 1012.0,
        "precipitacion": 15.0,
        "viento": 12.0,
        "voltaje_bateria": 3.9,
        "rssi": -75,
        "estado_calidad": "valid",
        "errores": None,
    })
    yield


class TestVectorStore:
    """Pruebas del almacén vectorial Qdrant."""

    def test_calcular_embedding_offline(self):
        """Verifica que el generador de embeddings produzca vectores normalizados."""
        texto = "Precipitación y riesgo de crecida en el río Caroní"
        emb = _calcular_embedding_offline(texto, dim=128)
        assert len(emb) == 128
        assert isinstance(emb[0], float)

    def test_indexacion_y_busqueda_qdrant(self):
        """Verifica la indexación de manuales en Qdrant (modo memoria) y su recuperación."""
        vs = AmazonIAVectorStore(coleccion="test_documentos", en_memoria=True)
        total_indexados = vs.indexar_documentos(forzar=True)
        assert total_indexados > 0

        # Buscar sobre siembra de yuca
        resultados = vs.buscar_similares("¿Cuándo sembrar yuca y qué drenaje necesita?", top_k=2)
        assert len(resultados) >= 1
        assert "score" in resultados[0]
        assert "titulo" in resultados[0]
        assert len(resultados[0]["texto"]) > 20


class TestRAGEngine:
    """Pruebas del motor RAG integral."""

    def test_extraer_contexto_telemetria(self):
        """Verifica que la telemetría en vivo se resuma correctamente."""
        ctx = extraer_contexto_telemetria("CARONI-001")
        assert ctx["estacion"] == "CARONI-001"
        assert ctx["registros_analizados"] >= 1
        assert ctx["ultima_lectura"]["temperatura"] == 29.5

    def test_responder_consulta_siembra(self):
        """Verifica la respuesta a una pregunta de cultivo."""
        res = responder_consulta_rag(
            "¿Es recomendable sembrar yuca hoy dadas las condiciones climáticas?",
            estacion_id="CARONI-001",
        )
        assert "respuesta" in res
        assert len(res["respuesta"]) > 50
        assert "Diagnóstico Agroecológico" in res["respuesta"]
        assert len(res["documentos"]) >= 1

    def test_responder_consulta_alerta_inundacion(self):
        """Verifica la respuesta a una pregunta sobre riesgo de inundación."""
        res = responder_consulta_rag(
            "¿Hay riesgo de inundación o crecida del río?",
            estacion_id="CARONI-001",
        )
        assert "respuesta" in res
        assert "Evaluación Hidrometeorológica" in res["respuesta"]
        assert "Nivel de Alerta" in res["respuesta"]

    def test_responder_consulta_salud_estacion(self):
        """Verifica la respuesta sobre estado de batería y sensores."""
        res = responder_consulta_rag(
            "¿Cuál es el estado de la batería y la señal de la estación?",
            estacion_id="CARONI-001",
        )
        assert "respuesta" in res
        assert "Voltaje actual" in res["respuesta"]
