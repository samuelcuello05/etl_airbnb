"""
Paso previo al ETL: poblar la base de datos MongoDB local.

La guía exige que el ETL use MongoDB como fuente (no los CSV). Este script
toma los archivos de Inside Airbnb ubicados en data/raw y:

1. crea la base de datos ``airbnb_bogota_etl`` (configurable en config.py),
2. importa cada archivo como colección: Listings, Calendar y Reviews,
3. crea índices sobre las llaves que usará el ETL,
4. valida que la cantidad de documentos en MongoDB sea igual a la cantidad
   de filas del archivo fuente.

Los datos se cargan "tal como vienen" (precio con "$" y ",", fechas como
texto, booleanos como "t"/"f"), igual que lo haría ``mongoimport``. Así la
limpieza ocurre realmente en la fase de transformación del ETL.

Uso::

    python src/cargar_mongo.py
"""

import time

import pandas as pd
from pymongo import ASCENDING, MongoClient

from config import COLECCIONES, DIR_DATOS_CRUDOS, MONGO_DB, MONGO_URI
from logger import obtener_logger

log = obtener_logger("cargar_mongo")

TAMANO_LOTE = 100_000

INDICES = {
    "Listings": ["id", "host_id"],
    "Calendar": ["listing_id", "date"],
    "Reviews": ["listing_id", "date"],
}


def _a_documentos(df: pd.DataFrame) -> list[dict]:
    """Convierte un DataFrame en documentos BSON, cambiando NaN por None (null)."""
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


def cargar_coleccion(db, nombre: str, archivo: str) -> None:
    """Importa un archivo .csv.gz a una colección y valida el conteo."""
    ruta = DIR_DATOS_CRUDOS / archivo
    if not ruta.exists():
        log.error("No se encontró el archivo fuente %s", ruta)
        raise FileNotFoundError(ruta)

    inicio = time.perf_counter()
    db.drop_collection(nombre)
    coleccion = db[nombre]
    log.info("Importando %s -> colección '%s'", ruta.name, nombre)

    filas_fuente = 0
    for lote in pd.read_csv(ruta, chunksize=TAMANO_LOTE, low_memory=False):
        coleccion.insert_many(_a_documentos(lote), ordered=False)
        filas_fuente += len(lote)
        log.info("  '%s': %s documentos insertados...", nombre, f"{filas_fuente:,}")

    for campo in INDICES.get(nombre, []):
        coleccion.create_index([(campo, ASCENDING)])

    total_mongo = coleccion.count_documents({})
    segundos = time.perf_counter() - inicio
    if total_mongo == filas_fuente:
        log.info("Validación OK '%s': %s filas en CSV = %s documentos en MongoDB (%.1fs)",
                 nombre, f"{filas_fuente:,}", f"{total_mongo:,}", segundos)
    else:
        log.error("Validación FALLIDA '%s': CSV=%s, MongoDB=%s",
                  nombre, f"{filas_fuente:,}", f"{total_mongo:,}")


def main() -> None:
    log.info("=== Carga inicial de MongoDB: %s / BD '%s' ===", MONGO_URI, MONGO_DB)
    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        cliente.admin.command("ping")
        db = cliente[MONGO_DB]
        for nombre, archivo in COLECCIONES.items():
            cargar_coleccion(db, nombre, archivo)
        log.info("Colecciones disponibles en '%s': %s", MONGO_DB, sorted(db.list_collection_names()))
    except Exception as e:
        log.error("Error durante la carga inicial de MongoDB: %s", e)
        raise
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
