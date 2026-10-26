# 🔧 Manual de Operación y Salud de Estaciones Meteorológicas Edge

## 1. Subsistema de Alimentación Eléctrica
Las estaciones comunitarias operan con paneles solares monocristalinos y bancos de baterías de ion de litio (3.7 V nominal, rango operativo 3.0 V a 4.2 V):
- **Voltaje Óptimo (4.0 V - 4.2 V):** Plena carga, operación normal diurna o tras jornada soleada.
- **Voltaje Estable (3.5 V - 3.9 V):** Operación nocturna típica.
- **Voltaje Crítico (< 3.2 V):** Alerta de batería baja. El microcontrolador ESP32 entra en modo de bajo consumo (*deep sleep*) para proteger la celda de sobre-descarga.
- **Causas comunes de batería baja:**
  1. Polvo o excrementos de aves cubriendo la superficie del panel solar.
  2. Nubosidad persistente de más de 4 días en temporada de invierno.
  3. Desgaste químico de la celda de litio tras más de 2 años de ciclo continuo.

---

## 2. Subsistema de Comunicaciones y Radioenlace (RSSI)
La intensidad de señal recibida (RSSI, en dBm) mide la calidad del enlace LoRaWAN o Wi-Fi/MQTT hacia el Edge Gateway:
- **-60 a -80 dBm:** Señal excelente. Cero pérdidas de paquetes de telemetría.
- **-81 a -100 dBm:** Señal moderada. Ocasional retransmisión de tramas; el protocolo offline-first almacena en SQLite si hay desconexión temporal.
- **< -105 dBm:** Señal marginal. Alta tasa de error en la trama de bits. Requiere reorientar la antena omnidireccional o elevar el mástil sobre la copa de los árboles cercanos.

---

## 3. Diagnóstico de Calidad de Datos (Framework Buelvas et al., 2023)
- **`valid`:** Todas las variables están dentro de límites termodinámicos y físicos de la cuenca del Caroní.
- **`invalid`:** Sensores dañados o descalibrados (ej. temperatura de 150 °C o humedad de -10%). Requiere limpieza o reemplazo del sensor DHT22 / SHT3x.
- **`incomplete`:** Faltan variables en la trama JSON recibida. Puede indicar falla en el bus I2C o cable desconectado.
