"""
Configuración central del proyecto ETL Airbnb Bogotá.

Todas las rutas se calculan a partir de la raíz del proyecto para que el
proceso funcione igual en cualquier equipo, sin importar desde qué carpeta
se ejecute. La conexión a MongoDB puede sobrescribirse con variables de
entorno (MONGO_URI, MONGO_DB) sin modificar el código.
"""

import os
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
DIR_DATOS_CRUDOS = RAIZ_PROYECTO / "data" / "raw"
DIR_DATOS_PROCESADOS = RAIZ_PROYECTO / "data" / "processed"
DIR_LOGS = RAIZ_PROYECTO / "logs"
DIR_FIGURAS = RAIZ_PROYECTO / "reports" / "figures"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "airbnb_bogota_etl")

COLECCIONES = {
    "Listings": "listings.csv.gz",
    "Calendar": "calendar.csv.gz",
    "Reviews": "reviews.csv.gz",
}

RUTA_SQLITE = DIR_DATOS_PROCESADOS / "airbnb_bogota.db"
RUTA_XLSX_RESUMEN = DIR_DATOS_PROCESADOS / "airbnb_bogota_resumen.xlsx"
RUTA_XLSX_REVIEWS = DIR_DATOS_PROCESADOS / "airbnb_bogota_reviews.xlsx"

MAX_FILAS_EXCEL = 1_048_575

RANGOS_PRECIO_COP = [0, 100_000, 200_000, 400_000, 1_000_000, float("inf")]
ETIQUETAS_PRECIO = ["Económico", "Medio", "Alto", "Premium", "Lujo"]

MAX_NOCHES_VALIDO = 1125

AMENITIES_CLAVE = {
    "tiene_wifi": r"\bwifi\b",
    "tiene_cocina": r"\bkitchen\b",
    "tiene_lavadora": r"\bwasher\b",
    "tiene_parqueadero": r"\bparking\b",
    "tiene_agua_caliente": r"\bhot water\b",
    "tiene_tv": r"\b(?:hd)?tv\b",
    "tiene_espacio_trabajo": r"\bworkspace\b",
    "tiene_aire_acondicionado": r"\bair conditioning\b",
    "tiene_calefaccion": r"\bheating\b",
    "tiene_gimnasio": r"\bgym\b",
    "tiene_piscina": r"\bpool\b(?! table)",
    "tiene_ascensor": r"\belevator\b",
    "admite_mascotas": r"\bpets allowed\b",
    "tiene_autocheckin": r"\bself check-in\b",
}
