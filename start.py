"""
Orquestador local de AmazonIA 4.0.
Levanta todos los servicios del proyecto en un solo comando y gestiona su ciclo de vida.
Uso:
    python start.py
    python start.py --modo mqtt
    python start.py --sin-simulador
"""
import sys
import os
import time
import signal
import argparse
import subprocess
import webbrowser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PYTHON_EXE = sys.executable


def imprimir_banner(modo: str, abrir_browser: bool):
    print("=" * 65)
    print(" 🌿 AmazonIA 4.0 — Despliegue Local Unificado")
    print("=" * 65)
    print(f" Modo seleccionado: {modo.upper()}")
    print(" Presiona Ctrl+C en esta terminal para detener todos los servicios.")
    print("=" * 65)


def verificar_base_datos():
    """Asegura que las tablas de SQLite existan antes de iniciar los servicios."""
    print("[INIT] Verificando base de datos SQLite...")
    try:
        from src.database import init_database
        init_database()
        print("  Base de datos lista.")
    except Exception as e:
        print(f"  Advertencia inicializando base de datos: {e}")


def main():
    parser = argparse.ArgumentParser(description="Despliegue local rápido de AmazonIA 4.0")
    parser.add_argument(
        "--modo",
        choices=["http", "mqtt", "ambos"],
        default="http",
        help="Canal de ingestión de sensores: 'http' (REST API), 'mqtt' (Mosquitto) o 'ambos' (por defecto: http)",
    )
    parser.add_argument(
        "--sin-simulador",
        action="store_true",
        help="No arrancar los simuladores de sensores automáticamente",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="No abrir automáticamente el navegador web",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default="localhost",
        help="Host del broker MQTT si usa modo mqtt (por defecto: localhost)",
    )
    args = parser.parse_args()

    imprimir_banner(args.modo, not args.no_browser)
    verificar_base_datos()

    procesos = []

    def terminar_procesos():
        print("\n[SHUTDOWN] Deteniendo todos los servicios de AmazonIA 4.0...")
        for nombre, proc in procesos:
            if proc.poll() is None:
                print(f"  Deteniendo {nombre} (PID: {proc.pid})...")
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    proc.kill()
        print("  Todos los servicios se han detenido correctamente. ¡Hasta luego!\n")

    # Registrar señales para apagado limpio
    def signal_handler(sig, frame):
        terminar_procesos()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, "SIGBREAK"):  # Para Windows Ctrl+Break
        signal.signal(signal.SIGBREAK, signal_handler)

    try:
        # 1. Iniciar API FastAPI (Core)
        print("\n🚀 [1/3] Iniciando API de Ingesta (FastAPI en :8000)...")
        p_api = subprocess.Popen(
            [PYTHON_EXE, "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"],
            cwd=str(BASE_DIR),
        )
        procesos.append(("API FastAPI", p_api))
        time.sleep(1.5)

        # 2. Iniciar Dashboard Streamlit (Visualización)
        print("📊 [2/3] Iniciando Dashboard (Streamlit en :8501)...")
        p_dash = subprocess.Popen(
            [
                PYTHON_EXE,
                "-m",
                "streamlit",
                "run",
                "src/dashboard.py",
                "--server.port=8501",
                "--server.headless=true",
            ],
            cwd=str(BASE_DIR),
        )
        procesos.append(("Dashboard Streamlit", p_dash))

        # 3. Iniciar Componentes MQTT si corresponde
        if args.modo in ["mqtt", "ambos"]:
            print(f"📡 [*] Iniciando Bridge MQTT (suscriptor hacia SQLite) [Broker: {args.broker}]...")
            p_bridge = subprocess.Popen(
                [PYTHON_EXE, "-m", "src.mqtt_suscriptor", "--broker", args.broker],
                cwd=str(BASE_DIR),
            )
            procesos.append(("Bridge MQTT", p_bridge))

        # 4. Iniciar Simuladores si no se desactivaron
        if not args.sin_simulador:
            print("🛰️ [3/3] Iniciando Generador de Telemetría...")
            if args.modo in ["http", "ambos"]:
                p_sim_http = subprocess.Popen(
                    [PYTHON_EXE, "-m", "src.simulador"],
                    cwd=str(BASE_DIR),
                )
                procesos.append(("Simulador HTTP", p_sim_http))

            if args.modo == "mqtt":
                p_sim_mqtt = subprocess.Popen(
                    [PYTHON_EXE, "-m", "src.mqtt_publicador", "--broker", args.broker, "--intervalo", "5"],
                    cwd=str(BASE_DIR),
                )
                procesos.append(("Publicador MQTT", p_sim_mqtt))
        else:
            print("🛰️ [3/3] Simuladores omitidos (--sin-simulador activo).")

        print("\n" + "=" * 65)
        print("  Servicios en ejecución:")
        print("   - API Docs:  http://localhost:8000/docs")
        print("   - Dashboard: http://localhost:8501")
        if args.modo in ["mqtt", "ambos"]:
            print(f"   - MQTT:      {args.broker}:1883")
        print("=" * 65)
        print(" Presiona Ctrl+C en cualquier momento para apagar todo.\n")

        # Abrir el navegador tras 3 segundos
        if not args.no_browser:
            time.sleep(2.5)
            webbrowser.open("http://localhost:8501")

        # Mantener el script activo monitoreando los procesos hijos
        while True:
            for nombre, proc in procesos:
                codigo = proc.poll()
                if codigo is not None:
                    print(f"\n[ALERTA] El proceso '{nombre}' finalizó inesperadamente con código {codigo}.")
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        terminar_procesos()


if __name__ == "__main__":
    main()
