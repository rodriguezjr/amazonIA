"""
Motor de persistencia local en SQLite para el Edge Gateway.
Diseñado bajo el enfoque Offline-First.
"""
import sqlite3
from contextlib import contextmanager
from src.config import DB_PATH


@contextmanager
def get_connection():
    """Context manager para conexiones a SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database():
    """Crea las tablas necesarias si no existen."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Tabla principal de telemetría
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetria (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                estacion_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                temperatura REAL,
                humedad REAL,
                presion REAL,
                precipitacion REAL,
                viento REAL,
                voltaje_bateria REAL,
                rssi INTEGER,
                estado_calidad TEXT NOT NULL DEFAULT 'valid',
                errores TEXT,
                sincronizado INTEGER NOT NULL DEFAULT 0,
                fecha_recepcion TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Índices para consultas eficientes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_estacion_timestamp 
            ON telemetria (estacion_id, timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sincronizado 
            ON telemetria (sincronizado)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_estado_calidad 
            ON telemetria (estado_calidad)
        """)
        
        # Tabla de log de sincronización
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS log_sincronizacion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                registros_enviados INTEGER NOT NULL,
                registros_exitosos INTEGER NOT NULL,
                estado TEXT NOT NULL,
                mensaje TEXT
            )
        """)
        
        conn.commit()


def insertar_telemetria(registro: dict) -> int:
    """Inserta un registro de telemetría validado."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO telemetria (
                estacion_id, timestamp, temperatura, humedad, presion,
                precipitacion, viento, voltaje_bateria, rssi,
                estado_calidad, errores
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            registro["estacion_id"],
            registro["timestamp"],
            registro.get("temperatura"),
            registro.get("humedad"),
            registro.get("presion"),
            registro.get("precipitacion"),
            registro.get("viento"),
            registro.get("voltaje_bateria"),
            registro.get("rssi"),
            registro["estado_calidad"],
            registro.get("errores"),
        ))
        return cursor.lastrowid


def obtener_no_sincronizados(limite: int = 100) -> list:
    """Obtiene registros pendientes de sincronización."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM telemetria
            WHERE sincronizado = 0
            ORDER BY timestamp ASC
            LIMIT ?
        """, (limite,))
        return [dict(row) for row in cursor.fetchall()]


def marcar_como_sincronizados(ids: list):
    """Marca registros como sincronizados."""
    with get_connection() as conn:
        cursor = conn.cursor()
        placeholders = ",".join(["?"] * len(ids))
        cursor.execute(f"""
            UPDATE telemetria
            SET sincronizado = 1
            WHERE id IN ({placeholders})
        """, ids)


def obtener_ultimas_lecturas(estacion_id: str = None, limite: int = 100) -> list:
    """Obtiene las últimas lecturas para el dashboard."""
    with get_connection() as conn:
        cursor = conn.cursor()
        if estacion_id:
            cursor.execute("""
                SELECT * FROM telemetria
                WHERE estacion_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (estacion_id, limite))
        else:
            cursor.execute("""
                SELECT * FROM telemetria
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limite,))
        return [dict(row) for row in cursor.fetchall()]


def obtener_lecturas_filtradas(
    estacion_id: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    limite: int = 1000,
) -> list:
    """
    Obtiene lecturas filtradas por estación y rango temporal (hace 1 hora, 1 día, etc.).
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        clausulas = []
        parametros = []

        if estacion_id and estacion_id != "Todas":
            clausulas.append("estacion_id = ?")
            parametros.append(estacion_id)

        if fecha_inicio:
            clausulas.append("timestamp >= ?")
            parametros.append(fecha_inicio)

        if fecha_fin:
            clausulas.append("timestamp <= ?")
            parametros.append(fecha_fin)

        where_sql = f"WHERE {' AND '.join(clausulas)}" if clausulas else ""

        query = f"""
            SELECT * FROM telemetria
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT ?
        """
        parametros.append(limite)
        cursor.execute(query, tuple(parametros))
        return [dict(row) for row in cursor.fetchall()]



def obtener_estadisticas_calidad() -> dict:
    """Obtiene estadísticas de calidad para el dashboard."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT estado_calidad, COUNT(*) as total
            FROM telemetria
            GROUP BY estado_calidad
        """)
        return {row["estado_calidad"]: row["total"] for row in cursor.fetchall()}


def obtener_estaciones() -> list:
    """Obtiene la lista de estaciones registradas."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT estacion_id FROM telemetria
            ORDER BY estacion_id
        """)
        return [row["estacion_id"] for row in cursor.fetchall()]