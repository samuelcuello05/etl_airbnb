"""
Orquestador del proceso ETL Airbnb Bogotá.

Ejecuta en orden las tres fases y deja un único archivo de log por ejecución
en logs/log_YYYYMMDD_HHMM.txt:

    MongoDB --(Extraccion)--> DataFrames crudos
            --(Transformacion)--> DataFrames limpios
            --(Carga)--> SQLite + XLSX (+ verificación)

Uso::

    python src/main.py                 # ETL completo (MongoDB ya poblado)
    python src/main.py --cargar-mongo  # primero pobla MongoDB desde data/raw
    python src/main.py --sin-xlsx      # omite la exportación a Excel (más rápido)
"""

import argparse
import gc
import sys
import time

from carga import Carga
from extraccion import Extraccion
from logger import obtener_logger, ruta_log_actual
from transformacion import Transformacion

log = obtener_logger("main")


def ejecutar_etl(cargar_mongo: bool = False, exportar_xlsx: bool = True) -> bool:
    """Ejecuta el proceso ETL completo y devuelve True si terminó sin errores."""
    inicio_total = time.perf_counter()
    log.info("########## INICIO DEL PROCESO ETL AIRBNB BOGOTÁ ##########")
    try:
        if cargar_mongo:
            import cargar_mongo
            cargar_mongo.main()

        t = time.perf_counter()
        with Extraccion() as extraccion:
            datos = extraccion.extraer_todo()
        gc.collect()
        log.info("Fase de extracción completada en %.1fs", time.perf_counter() - t)

        t = time.perf_counter()
        tablas = Transformacion(datos).transformar_todo()
        del datos
        gc.collect()
        log.info("Fase de transformación completada en %.1fs", time.perf_counter() - t)

        t = time.perf_counter()
        carga = Carga(tablas)
        carga.cargar_sqlite()
        if exportar_xlsx:
            carga.exportar_xlsx()
        else:
            log.warning("Exportación a XLSX omitida por parámetro --sin-xlsx")
        ok = carga.verificar_carga()
        log.info("Fase de carga completada en %.1fs", time.perf_counter() - t)
    except Exception as e:
        log.error("El proceso ETL se detuvo por un error (%s): %s", type(e).__name__, e, exc_info=True)
        return False

    nivel = log.info if ok else log.error
    nivel("########## FIN DEL PROCESO ETL (%s) en %.1fs ##########",
          "OK" if ok else "CON ERRORES", time.perf_counter() - inicio_total)
    log.info("Log de la ejecución: %s", ruta_log_actual())
    return ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso ETL Airbnb Bogotá (MongoDB -> SQLite/XLSX)")
    parser.add_argument("--cargar-mongo", action="store_true",
                        help="poblar MongoDB desde data/raw antes de ejecutar el ETL")
    parser.add_argument("--sin-xlsx", action="store_true", help="no exportar a Excel")
    args = parser.parse_args()
    sys.exit(0 if ejecutar_etl(args.cargar_mongo, not args.sin_xlsx) else 1)
