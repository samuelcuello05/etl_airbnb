"""
Módulo centralizado de logs del proceso ETL.

Cumple el requerimiento de la guía (sección 5):
  * un archivo por ejecución: logs/log_YYYYMMDD_HHMM.txt
  * niveles INFO, WARNING y ERROR
  * cada línea incluye fecha, hora, nivel, módulo y una descripción del evento

Todos los módulos (extraccion, transformacion, carga, main, cargar_mongo)
piden su logger con ``obtener_logger("nombre")``. Los loggers cuelgan de un
logger raíz "etl" que se configura una sola vez por proceso, de modo que las
tres fases de una misma ejecución escriben en el mismo archivo.

Ejemplo de línea generada::

    2026-10-07 10:15:02 | INFO     | etl.extraccion | Colección 'Listings': 19187 registros extraídos
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from config import DIR_LOGS

_NOMBRE_RAIZ = "etl"
_FORMATO = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"
_ruta_log_actual: Path | None = None


def _configurar_raiz() -> logging.Logger:
    """Configura (una sola vez) el logger raíz con salida a archivo y consola."""
    global _ruta_log_actual
    raiz = logging.getLogger(_NOMBRE_RAIZ)
    if raiz.handlers:
        return raiz

    DIR_LOGS.mkdir(parents=True, exist_ok=True)
    marca = datetime.now().strftime("%Y%m%d_%H%M")
    _ruta_log_actual = DIR_LOGS / f"log_{marca}.txt"

    formato = logging.Formatter(_FORMATO, datefmt=_FORMATO_FECHA)

    manejador_archivo = logging.FileHandler(_ruta_log_actual, mode="a", encoding="utf-8")
    manejador_archivo.setFormatter(formato)
    manejador_archivo.setLevel(logging.INFO)

    manejador_consola = logging.StreamHandler(sys.stdout)
    manejador_consola.setFormatter(formato)
    manejador_consola.setLevel(logging.INFO)

    raiz.setLevel(logging.INFO)
    raiz.addHandler(manejador_archivo)
    raiz.addHandler(manejador_consola)
    raiz.propagate = False
    raiz.info("Archivo de log de esta ejecución: %s", _ruta_log_actual)
    return raiz


def obtener_logger(nombre: str) -> logging.Logger:
    """
    Devuelve un logger hijo de "etl" listo para usar.

    Parameters
    ----------
    nombre : str
        Nombre corto del componente (p. ej. "extraccion"). Aparece en cada
        línea del log para identificar qué parte del ETL generó el evento.
    """
    _configurar_raiz()
    return logging.getLogger(f"{_NOMBRE_RAIZ}.{nombre}")


def ruta_log_actual() -> Path | None:
    """Ruta del archivo de log de la ejecución en curso (None si aún no existe)."""
    return _ruta_log_actual
