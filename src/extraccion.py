"""
Fase de EXTRACCIÓN del proceso ETL.

Contiene la clase ``Extraccion``, encargada de conectarse a la base de datos
MongoDB local, consultar las colecciones Listings, Calendar y Reviews y
devolverlas como DataFrames de pandas, registrando en el log la conexión y
la cantidad de registros extraídos por colección.
"""

import time

import pandas as pd
from pandas.api.types import union_categoricals
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from config import MONGO_DB, MONGO_URI
from logger import obtener_logger


class Extraccion:
    """
    Extrae datos desde MongoDB hacia DataFrames de pandas.

    Parameters
    ----------
    uri : str
        Cadena de conexión de MongoDB (por defecto la de config.py).
    nombre_bd : str
        Base de datos que contiene las colecciones de Airbnb.
    tamano_lote : int
        Documentos con los que se arma cada bloque del DataFrame. Calendar
        tiene ~7 millones de documentos; construir el DataFrame por bloques
        evita mantener millones de diccionarios en memoria al mismo tiempo.
    lote_cursor : int
        Documentos que MongoDB envía en cada respuesta del cursor. Un valor
        moderado mantiene bajo el consumo de memoria del servidor.

    Ejemplo
    -------
    >>> with Extraccion() as ext:
    ...     datos = ext.extraer_todo()
    >>> datos["Listings"].shape
    (19187, 90)
    """

    COLECCIONES_REQUERIDAS = ("Listings", "Calendar", "Reviews")

    def __init__(self, uri: str = MONGO_URI, nombre_bd: str = MONGO_DB,
                 tamano_lote: int = 200_000, lote_cursor: int = 10_000):
        self.uri = uri
        self.nombre_bd = nombre_bd
        self.tamano_lote = tamano_lote
        self.lote_cursor = lote_cursor
        self.cliente: MongoClient | None = None
        self.db = None
        self.log = obtener_logger("extraccion")

    def conectar(self) -> None:
        """
        Abre la conexión con MongoDB y verifica que existan las colecciones
        requeridas. Lanza la excepción (y la registra como ERROR) si el
        servidor no responde.
        """
        try:
            self.cliente = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            info = self.cliente.server_info()
            self.db = self.cliente[self.nombre_bd]
            colecciones = self.db.list_collection_names()
            self.log.info("Conexión exitosa a MongoDB %s (versión %s), base de datos '%s'",
                          self.uri, info.get("version"), self.nombre_bd)
            self.log.info("Colecciones disponibles: %s", sorted(colecciones))

            faltantes = [c for c in self.COLECCIONES_REQUERIDAS if c not in colecciones]
            if faltantes:
                self.log.warning("Faltan colecciones requeridas: %s. Ejecute primero "
                                 "'python src/cargar_mongo.py'", faltantes)
        except ServerSelectionTimeoutError as e:
            self.log.error("No fue posible conectarse a MongoDB en %s: %s", self.uri, e)
            raise

    def cerrar(self) -> None:
        """Cierra la conexión con MongoDB."""
        if self.cliente is not None:
            self.cliente.close()
            self.log.info("Conexión a MongoDB cerrada")
            self.cliente = None

    def __enter__(self):
        self.conectar()
        return self

    def __exit__(self, *exc):
        self.cerrar()

    @staticmethod
    def _unir_bloques(partes: list[pd.DataFrame], categoricas: list[str]) -> pd.DataFrame:
        """
        Une los bloques en un solo DataFrame. Las columnas categóricas se
        alinean a un mismo conjunto de categorías para que sigan siendo
        categóricas tras la unión (si no, pandas las convierte en texto).
        """
        for col in categoricas:
            categorias = union_categoricals([p[col] for p in partes]).categories
            for p in partes:
                p[col] = p[col].cat.set_categories(categorias)
        return pd.concat(partes, ignore_index=True)

    def extraer_coleccion(self, nombre: str, filtro: dict | None = None,
                          proyeccion: dict | None = None,
                          limite: int = 0,
                          categoricas: list[str] | None = None) -> pd.DataFrame:
        """
        Consulta una colección y devuelve sus documentos como DataFrame.

        Parameters
        ----------
        nombre : str
            Nombre de la colección (Listings, Calendar o Reviews).
        filtro : dict, opcional
            Filtro de MongoDB, p. ej. ``{"room_type": "Private room"}``.
        proyeccion : dict, opcional
            Campos a incluir. El campo ``_id`` (ObjectId interno de Mongo)
            se excluye siempre porque no aporta valor analítico.
        limite : int, opcional
            Máximo de documentos (0 = sin límite). Útil para pruebas rápidas.
        categoricas : list[str], opcional
            Columnas de texto muy repetitivas que se guardan como ``category``
            (p. ej. las 366 fechas distintas del calendario repetidas en 7
            millones de filas). Reduce cientos de MB de memoria sin alterar los
            valores.
        """
        if self.db is None:
            self.conectar()

        proyeccion = {**(proyeccion or {}), "_id": 0}
        categoricas = categoricas or []
        inicio = time.perf_counter()
        try:
            cursor = (self.db[nombre]
                      .find(filtro or {}, proyeccion)
                      .limit(limite)
                      .batch_size(self.lote_cursor))

            def a_bloque(docs):
                bloque = pd.DataFrame(docs)
                for col in categoricas:
                    if col in bloque:
                        bloque[col] = bloque[col].astype("category")
                return bloque

            partes, lote = [], []
            for doc in cursor:
                lote.append(doc)
                if len(lote) >= self.tamano_lote:
                    partes.append(a_bloque(lote))
                    lote = []
            if lote:
                partes.append(a_bloque(lote))
            presentes = [c for c in categoricas if partes and all(c in p for p in partes)]
            df = self._unir_bloques(partes, presentes) if partes else pd.DataFrame()
        except PyMongoError as e:
            self.log.error("Error consultando la colección '%s': %s", nombre, e)
            raise

        segundos = time.perf_counter() - inicio
        if df.empty:
            self.log.warning("La colección '%s' no devolvió registros", nombre)
        self.log.info("Colección '%s': %s registros y %d columnas extraídos (%.1fs)",
                      nombre, f"{len(df):,}", df.shape[1], segundos)
        return df

    def extraer_listings(self, **kwargs) -> pd.DataFrame:
        """Extrae la colección Listings (un documento por alojamiento)."""
        return self.extraer_coleccion("Listings", **kwargs)

    def extraer_calendar(self, **kwargs) -> pd.DataFrame:
        """
        Extrae la colección Calendar (un documento por alojamiento y día).
        ``date`` y ``available`` se cargan como categóricas para ahorrar memoria.
        """
        kwargs.setdefault("categoricas", ["date", "available"])
        return self.extraer_coleccion("Calendar", **kwargs)

    def extraer_reviews(self, **kwargs) -> pd.DataFrame:
        """Extrae la colección Reviews (un documento por reseña)."""
        return self.extraer_coleccion("Reviews", **kwargs)

    def extraer_todo(self) -> dict[str, pd.DataFrame]:
        """
        Extrae las tres colecciones requeridas.

        Returns
        -------
        dict
            ``{"Listings": df, "Calendar": df, "Reviews": df}``
        """
        self.log.info("=== Inicio de la fase de EXTRACCIÓN ===")
        datos = {
            "Listings": self.extraer_listings(),
            "Calendar": self.extraer_calendar(),
            "Reviews": self.extraer_reviews(),
        }
        total = sum(len(df) for df in datos.values())
        self.log.info("Extracción finalizada: %s registros en total", f"{total:,}")
        return datos
