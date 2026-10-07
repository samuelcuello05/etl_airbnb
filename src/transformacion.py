"""
Fase de TRANSFORMACIÓN del proceso ETL.

Contiene la clase ``Transformacion``, que recibe los DataFrames crudos
extraídos de MongoDB y produce un conjunto de DataFrames limpios, tipados y
listos para cargarse en una base de datos analítica (SQLite) y en Excel.

Cada transformación responde a un hallazgo del análisis exploratorio
(notebooks/exploracion_airbnb.ipynb). El *por qué* de cada paso está en el
docstring del método correspondiente; el *cuánto* (filas antes/después,
valores corregidos) queda registrado en el log de la ejecución y en la tabla
de auditoría ``auditoria_transformaciones``.

Tablas resultantes
------------------
listings                 Un registro por alojamiento (limpio, sin datos del host)
hosts                    Dimensión de anfitriones (desanidada de listings)
listing_amenities        Amenities en formato largo: (listing_id, amenity)
amenities_resumen        Frecuencia de cada amenity
calendar                 Calendario diario con variables de fecha derivadas
calendar_mensual         Calendario resumido por alojamiento y mes
calendar_semanal         Calendario resumido por semana a nivel ciudad
reviews                  Reseñas limpias con variables de fecha derivadas
reviews_mensual          Reseñas resumidas por mes
auditoria_transformaciones  Bitácora de cada paso (antes/después)
"""

import json
import unicodedata

import numpy as np
import pandas as pd

from config import (AMENITIES_CLAVE, ETIQUETAS_PRECIO, MAX_NOCHES_VALIDO,
                    RANGOS_PRECIO_COP)
from logger import obtener_logger

LOCALIDADES_OFICIALES = [
    "Usaquén", "Chapinero", "Santa Fe", "San Cristóbal", "Usme", "Tunjuelito",
    "Bosa", "Kennedy", "Fontibón", "Engativá", "Suba", "Barrios Unidos",
    "Teusaquillo", "Los Mártires", "Antonio Nariño", "Puente Aranda",
    "La Candelaria", "Rafael Uribe Uribe", "Ciudad Bolívar", "Sumapaz",
]

DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

COLUMNAS_IRRELEVANTES = [
    "listing_url", "picture_url", "host_url", "host_profile_url",
    "host_picture_url", "host_thumbnail_url", "scrape_id", "source",
    "host_profile_id", "calendar_last_scraped",
]

COLUMNAS_HOST = [
    "host_id", "host_name", "host_location", "host_about", "host_is_superhost",
    "host_has_profile_pic", "host_identity_verified", "host_listings_count",
    "hosts_time_as_user_years", "hosts_time_as_user_months",
    "hosts_time_as_host_years", "hosts_time_as_host_months",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
]


def _sin_tildes(texto: str) -> str:
    """Quita tildes/diacríticos: 'Bogotá' -> 'Bogota'."""
    return "".join(c for c in unicodedata.normalize("NFKD", texto)
                   if not unicodedata.combining(c))


class Transformacion:
    """
    Limpia y transforma los datos de Airbnb Bogotá.

    Parameters
    ----------
    datos : dict[str, pandas.DataFrame]
        Salida de ``Extraccion.extraer_todo()``: claves Listings, Calendar
        y Reviews. Listings se copia antes de transformarse; Calendar (~7
        millones de filas) y Reviews se toman del diccionario y se
        transforman sin copia para ahorrar memoria.

    Ejemplo
    -------
    >>> trans = Transformacion(datos)
    >>> tablas = trans.transformar_todo()
    >>> tablas["listings"]["price"].dtype
    dtype('float64')
    """

    def __init__(self, datos: dict[str, pd.DataFrame]):
        self.crudos = datos
        self.log = obtener_logger("transformacion")
        self.auditoria: list[dict] = []
        self.tablas: dict[str, pd.DataFrame] = {}

    def _registrar(self, tabla: str, paso: str, antes: int, despues: int,
                   detalle: str = "") -> None:
        """Guarda un paso en la auditoría y lo escribe en el log (INFO)."""
        self.auditoria.append({"tabla": tabla, "paso": paso, "filas_antes": antes,
                               "filas_despues": despues, "detalle": detalle})
        self.log.info("[%s] %s | filas: %s -> %s%s", tabla, paso, f"{antes:,}",
                      f"{despues:,}", f" | {detalle}" if detalle else "")

    def eliminar_duplicados(self, df: pd.DataFrame, tabla: str,
                            llave: list[str] | None = None) -> pd.DataFrame:
        """
        Elimina registros duplicados exactos y, si se indica, duplicados por
        llave de negocio (p. ej. ``id`` o ``listing_id + date``).

        Por qué: un mismo alojamiento/día/reseña repetido inflaría conteos,
        ocupación e ingresos. En el EDA no se encontraron duplicados, pero el
        control se mantiene para que el proceso sea robusto ante nuevas cargas.

        Cuando hay llave, basta revisar la llave: todo duplicado exacto es
        también un duplicado por llave. Así, en el calendario (7 millones de
        filas) se comparan 2 columnas en lugar de todas.
        """
        antes = len(df)
        dup = df.duplicated(subset=llave, keep="last") if llave else df.duplicated()
        eliminados = int(dup.sum())
        if eliminados:
            df = df[~dup]
            self.log.warning("[%s] Se eliminaron %d registros duplicados (criterio: %s)",
                             tabla, eliminados, llave or "fila completa")
        self._registrar(tabla, "Eliminar duplicados", antes, len(df),
                        f"duplicados por {llave or 'fila completa'} = {eliminados}")
        return df

    def normalizar_precio(self, serie: pd.Series) -> pd.Series:
        """
        Convierte precios en texto ("$284,294.01") a número (284294.01).

        Elimina el símbolo "$" y el separador de miles ",", y convierte a
        float. Los valores que no se puedan convertir quedan como NaN y se
        reportan como WARNING.
        """
        texto = serie.astype("string").str.replace(r"[\$,\s]", "", regex=True)
        numero = pd.to_numeric(texto, errors="coerce")
        invalidos = int((numero.isna() & texto.notna() & (texto != "")).sum())
        if invalidos:
            self.log.warning("%d precios no pudieron convertirse a número en '%s'",
                             invalidos, serie.name)
        return numero.astype("float64")

    @staticmethod
    def estandarizar_fecha(serie: pd.Series) -> pd.Series:
        """
        Convierte una columna de fechas al formato estándar ISO YYYY-MM-DD.

        Se guarda como texto ISO porque SQLite no tiene un tipo DATE nativo y
        su documentación recomienda TEXT ISO-8601; además ordena correctamente
        y Excel/pandas lo reconocen como fecha.
        """
        return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")

    @staticmethod
    def a_fecha(serie: pd.Series) -> pd.Series:
        """
        Convierte texto a datetime. Si la columna es categórica (calendario),
        convierte solo las categorías distintas (366) y las expande con los
        códigos, en vez de interpretar 7 millones de textos.
        """
        if isinstance(serie.dtype, pd.CategoricalDtype):
            fechas = pd.to_datetime(serie.cat.categories, errors="coerce", format="ISO8601")
            codigos = serie.cat.codes.to_numpy()
            valores = np.where(codigos >= 0, fechas.to_numpy()[codigos], np.datetime64("NaT"))
            return pd.Series(valores, index=serie.index, dtype="datetime64[ns]")
        return pd.to_datetime(serie, errors="coerce", format="ISO8601")

    @classmethod
    def derivar_variables_fecha(cls, df: pd.DataFrame, columna: str = "date") -> pd.DataFrame:
        """
        Deriva año, mes, día, trimestre, día de la semana, semana ISO y
        bandera de fin de semana a partir de una columna de fecha, y deja la
        fecha en formato YYYY-MM-DD.

        Por qué: permite agrupar por periodos (estacionalidad, temporadas,
        fines de semana) directamente en SQL o Excel sin recalcular fechas.
        """
        original = df[columna]
        if isinstance(original.dtype, pd.CategoricalDtype):
            return cls._derivar_fecha_categorica(df, columna)
        fecha = cls.a_fecha(original)
        df[columna] = fecha.dt.strftime("%Y-%m-%d")
        df["anio"] = fecha.dt.year.astype("Int16")
        df["mes"] = fecha.dt.month.astype("Int8")
        df["dia"] = fecha.dt.day.astype("Int8")
        df["trimestre"] = fecha.dt.quarter.astype("Int8")
        df["dia_semana"] = fecha.dt.dayofweek.map(dict(enumerate(DIAS_SEMANA)))
        df["semana_iso"] = fecha.dt.isocalendar().week.astype("Int8")
        df["es_fin_de_semana"] = (fecha.dt.dayofweek >= 5).astype("int8")
        return df

    @staticmethod
    def _derivar_fecha_categorica(df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Igual que ``derivar_variables_fecha`` pero para una columna categórica
        (calendario): las variables se calculan una sola vez sobre las fechas
        distintas (366) y se expanden a los 7 millones de filas con los
        códigos de la categoría. El resultado es idéntico y usa una fracción
        de la memoria y del tiempo.
        """
        original = df[columna]
        codigos = original.cat.codes.to_numpy()
        if (codigos < 0).any():
            raise ValueError(f"La columna '{columna}' tiene fechas nulas")
        fechas = pd.DatetimeIndex(pd.to_datetime(original.cat.categories, errors="coerce", format="ISO8601"))
        iso = fechas.strftime("%Y-%m-%d")

        df[columna] = pd.Categorical.from_codes(
            pd.Index(sorted(iso)).get_indexer(iso)[codigos], categories=sorted(iso), ordered=True)
        df["anio"] = fechas.year.to_numpy().astype("int16")[codigos]
        df["mes"] = fechas.month.to_numpy().astype("int8")[codigos]
        df["dia"] = fechas.day.to_numpy().astype("int8")[codigos]
        df["trimestre"] = fechas.quarter.to_numpy().astype("int8")[codigos]
        df["dia_semana"] = pd.Categorical.from_codes(fechas.dayofweek.to_numpy()[codigos],
                                                     categories=DIAS_SEMANA)
        df["semana_iso"] = fechas.isocalendar().week.to_numpy().astype("int8")[codigos]
        df["es_fin_de_semana"] = (fechas.dayofweek.to_numpy() >= 5).astype("int8")[codigos]
        return df

    @staticmethod
    def limpiar_texto(serie: pd.Series) -> pd.Series:
        """Quita etiquetas HTML (<br/>), espacios repetidos y espacios en los extremos."""
        return (serie.astype("string")
                .str.replace(r"<[^>]+>", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip())

    def canonizar_texto(self, serie: pd.Series) -> pd.Series:
        """
        Unifica variantes de escritura de un mismo valor.

        Agrupa los valores ignorando tildes, mayúsculas y espacios, y los
        reemplaza por la variante más frecuente del grupo. Ejemplo real del
        EDA: "Bogota, Colombia" (2.253) -> "Bogotá, Colombia" (9.451);
        "Medellin, Colombia" -> "Medellín, Colombia". También elimina
        repeticiones como "Colombia, Colombia" -> "Colombia".
        """
        limpia = self.limpiar_texto(serie)

        def sin_repeticiones(valor):
            if pd.isna(valor):
                return valor
            partes = [p.strip() for p in str(valor).split(",") if p.strip()]
            unicas = [p for i, p in enumerate(partes) if i == 0 or p.lower() != partes[i - 1].lower()]
            return ", ".join(unicas)

        limpia = limpia.map(sin_repeticiones)
        clave = limpia.map(lambda v: v if pd.isna(v) else _sin_tildes(v).lower())
        canonico = limpia.groupby(clave).agg(lambda s: s.value_counts().idxmax())
        resultado = clave.map(canonico)
        cambios = int((resultado.fillna("") != limpia.fillna("")).sum())
        self.log.info("Texto '%s' canonizado: %d valores unificados", serie.name, cambios)
        return resultado

    def acotar_noches(self, df: pd.DataFrame, tabla: str, columnas: list[str]) -> pd.DataFrame:
        """
        Reemplaza valores de noches mayores al máximo permitido por Airbnb
        (1125) por ese máximo.

        Por qué: el EDA mostró valores de 2.147.483.647 (máximo de un entero de
        32 bits), que son marcadores técnicos de "sin límite" y no estadías
        reales; dejarlos distorsiona promedios y desviaciones.
        """
        for col in columnas:
            if col not in df:
                continue
            invalidos = int((df[col] > MAX_NOCHES_VALIDO).sum())
            if invalidos:
                self.log.warning("[%s] '%s': %s valores > %d acotados a %d", tabla, col,
                                 f"{invalidos:,}", MAX_NOCHES_VALIDO, MAX_NOCHES_VALIDO)
                df[col] = df[col].clip(upper=MAX_NOCHES_VALIDO)
        return df

    def _eliminar_columnas_vacias(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina columnas 100 % nulas y columnas sin valor analítico.

        Por qué: el EDA encontró 13 columnas completamente vacías en este
        scrape (host_since, host_response_rate, neighbourhood,
        calendar_updated, instant_bookable, ...). No es posible imputarlas
        y solo agregan ruido. Las URLs e identificadores técnicos tampoco
        aportan al análisis.
        """
        vacias = [c for c in df.columns if df[c].isna().all()]
        irrelevantes = [c for c in COLUMNAS_IRRELEVANTES if c in df.columns and c not in vacias]
        df = df.drop(columns=vacias + irrelevantes)
        self.log.info("[listings] Columnas 100%% nulas eliminadas (%d): %s", len(vacias), vacias)
        self.log.info("[listings] Columnas irrelevantes eliminadas (%d): %s",
                      len(irrelevantes), irrelevantes)
        self.auditoria.append({"tabla": "listings", "paso": "Eliminar columnas vacías/irrelevantes",
                               "filas_antes": len(df), "filas_despues": len(df),
                               "detalle": f"{len(vacias)} vacías + {len(irrelevantes)} irrelevantes; "
                                          f"quedan {df.shape[1]} columnas"})
        return df

    def _desanidar_cotizacion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Desanida el campo JSON ``price_quote_raw`` (cotización de Airbnb).

        Se extraen la moneda y los cargos adicionales (limpieza, servicio,
        impuestos, descuento). La moneda permite validar que todos los precios
        estén en COP antes de compararlos.
        """
        def leer(valor):
            try:
                q = json.loads(valor).get("quote", {}) if isinstance(valor, str) else {}
            except (json.JSONDecodeError, AttributeError):
                q = {}
            return pd.Series({
                "moneda": q.get("currency"),
                "tarifa_limpieza": q.get("cleaning_fee"),
                "tarifa_servicio": q.get("service_fee"),
                "impuestos": q.get("taxes"),
                "descuento_cotizacion": q.get("discount_amount"),
            })

        extra = df["price_quote_raw"].apply(leer).set_index(df.index)
        for col in ["tarifa_limpieza", "tarifa_servicio", "impuestos", "descuento_cotizacion"]:
            extra[col] = pd.to_numeric(extra[col], errors="coerce")
        extra["moneda"] = extra["moneda"].fillna("COP")
        otras = extra.loc[extra["moneda"] != "COP", "moneda"].value_counts()
        if not otras.empty:
            self.log.warning("[listings] Hay precios en monedas distintas a COP: %s", otras.to_dict())
        sin_datos = [c for c in extra.columns if extra[c].isna().all()]
        if sin_datos:
            self.log.warning("[listings] Campos de la cotización sin ningún valor (se descartan): %s",
                             sin_datos)
            extra = extra.drop(columns=sin_datos)
        df = pd.concat([df.drop(columns=["price_quote_raw"]), extra], axis=1)
        self._registrar("listings", "Desanidar price_quote_raw (JSON)", len(df), len(df),
                        f"monedas={extra['moneda'].value_counts().to_dict()}")
        return df

    def _limpiar_precios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza ``price`` (y los precios de la cotización) a número en COP.

        Además:
        * precios nulos (195 en el EDA, ninguno con cotización disponible) se
          imputan con la mediana de su localidad y tipo de habitación, que es
          el grupo más parecido; se marca ``precio_imputado = 1`` para que el
          análisis pueda excluirlos si lo desea;
        * se marca ``precio_outlier`` con el criterio IQR sobre el logaritmo del
          precio (la distribución es muy asimétrica) en lugar de eliminar los
          registros: muchos son alojamientos de lujo reales;
        * ``price_ajustado`` recorta (winsoriza) el precio entre los percentiles
          1 y 99 para que promedios y gráficas no se distorsionen por errores
          evidentes (el EDA encontró un máximo de 448 millones COP/noche).
        """
        n = len(df)
        df["price"] = self.normalizar_precio(df["price"])
        for col in ["price_quote_total_price", "price_quote_price_per_night"]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        nulos = df["price"].isna()
        df["precio_imputado"] = nulos.astype("int8")
        if nulos.any():
            self.log.warning("[listings] %d alojamientos sin precio; se imputan con la mediana "
                             "por localidad y tipo de habitación", int(nulos.sum()))
            mediana_grupo = df.groupby(["neighbourhood_cleansed", "room_type"])["price"].transform("median")
            mediana_tipo = df.groupby("room_type")["price"].transform("median")
            df["price"] = df["price"].fillna(mediana_grupo).fillna(mediana_tipo)

        log_p = np.log(df["price"])
        q1, q3 = log_p.quantile([0.25, 0.75])
        lim_inf, lim_sup = np.exp(q1 - 1.5 * (q3 - q1)), np.exp(q3 + 1.5 * (q3 - q1))
        df["precio_outlier"] = ((df["price"] < lim_inf) | (df["price"] > lim_sup)).astype("int8")
        p01, p99 = df["price"].quantile([0.01, 0.99])
        df["price_ajustado"] = df["price"].clip(p01, p99).round(2)

        self.log.warning("[listings] %d precios atípicos (fuera de %.0f - %.0f COP) marcados; "
                         "precio máximo encontrado %.0f COP", int(df["precio_outlier"].sum()),
                         lim_inf, lim_sup, df["price"].max())
        self._registrar("listings", "Normalizar precio ($ y , -> número COP)", n, len(df),
                        f"imputados={int(nulos.sum())}, outliers={int(df['precio_outlier'].sum())}, "
                        f"winsorizado P1={p01:,.0f} P99={p99:,.0f}")
        return df

    def _categorizar_precio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea ``categoria_precio`` agrupando el precio por noche en rangos COP
        (Económico, Medio, Alto, Premium, Lujo), definidos en config.py según
        la distribución observada en el EDA. Facilita segmentar el mercado en
        tablas dinámicas y dashboards.
        """
        df["categoria_precio"] = pd.cut(df["price"], bins=RANGOS_PRECIO_COP,
                                        labels=ETIQUETAS_PRECIO, right=False).astype("string")
        self._registrar("listings", "Categorizar precio por rangos", len(df), len(df),
                        str(df["categoria_precio"].value_counts().to_dict()))
        return df

    def _convertir_booleanos(self, df: pd.DataFrame, columnas: list[str], tabla: str) -> pd.DataFrame:
        """Convierte columnas 't'/'f' en enteros 1/0 (nulos se conservan como NULL)."""
        for col in columnas:
            if col in df:
                df[col] = df[col].map({"t": 1, "f": 0, True: 1, False: 0}).astype("Int8")
        self._registrar(tabla, "Booleanos t/f -> 1/0", len(df), len(df), ", ".join(columnas))
        return df

    def _tratar_nulos_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Trata los valores faltantes de listings según su significado:

        * ``minimum_nights`` (4 nulos): se toma ``minimum_minimum_nights`` y si
          tampoco existe, 1 (valor por defecto de Airbnb).
        * ``beds`` / ``bedrooms``: se imputan con la mediana según capacidad
          (``accommodates``); en habitaciones privadas/compartidas/hotel el
          número de habitaciones es 1 por definición.
        * ``review_scores_*`` y ``first/last_review``: se dejan nulos a propósito.
          No son datos perdidos sino alojamientos sin reseñas (22 %); imputarlos
          inventaría calificaciones. Se añade ``tiene_resenas`` para filtrarlos.
        * ``reviews_per_month``: 0 cuando no hay reseñas.
        * textos (descripción, ubicación del host, licencia): "No especificado"
          / "Sin licencia", y ``tiene_licencia`` (registro RNT) como bandera.
        """
        antes = int(df.isna().sum().sum())
        df["minimum_nights"] = df["minimum_nights"].fillna(df["minimum_minimum_nights"]).fillna(1)

        es_habitacion = df["room_type"].isin(["Private room", "Shared room", "Hotel room"])
        df.loc[es_habitacion & df["bedrooms"].isna(), "bedrooms"] = 1
        for col in ["bedrooms", "beds"]:
            mediana = df.groupby("accommodates")[col].transform("median")
            df[col] = df[col].fillna(mediana).fillna(df[col].median())

        df["tiene_resenas"] = (df["number_of_reviews"] > 0).astype("int8")
        df["reviews_per_month"] = df["reviews_per_month"].fillna(0)

        df["tiene_licencia"] = df["license"].notna().astype("int8")
        df["license"] = df["license"].astype("string").str.replace(r"\.0$", "", regex=True).fillna("Sin licencia")
        for col in ["description", "neighborhood_overview"]:
            if col in df:
                df[col] = df[col].fillna("No especificado")

        despues = int(df.isna().sum().sum())
        self._registrar("listings", "Tratar valores nulos", len(df), len(df),
                        f"celdas nulas {antes:,} -> {despues:,} (review_scores sin reseñas se "
                        f"conservan nulos: {int((df['tiene_resenas'] == 0).sum()):,} alojamientos)")
        return df

    def _procesar_banos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae el número de baños y si son compartidos desde ``bathrooms_text``
        ("1.5 shared baths", "Half-bath", ...). La columna numérica ``bathrooms``
        venía con 1.066 nulos; el texto permite completarla.
        """
        texto = df["bathrooms_text"].astype("string").str.lower()
        numero = pd.to_numeric(texto.str.extract(r"(\d+(?:\.\d+)?)")[0], errors="coerce")
        numero = numero.mask(numero.isna() & texto.str.contains("half", na=False), 0.5)
        antes = int(df["bathrooms"].isna().sum())
        df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce").fillna(numero)
        df["bathrooms"] = df["bathrooms"].fillna(df["bathrooms"].median())
        df["bano_compartido"] = texto.str.contains("shared", na=False).astype("int8")
        self._registrar("listings", "Derivar baños desde bathrooms_text", len(df), len(df),
                        f"bathrooms nulos {antes} -> {int(df['bathrooms'].isna().sum())}")
        return df

    def _estandarizar_textos_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estandariza los campos de texto:

        * ``neighbourhood_cleansed`` -> ``localidad`` con el nombre oficial
          (tildes y "La Candelaria"), para cruzar con datos del Distrito.
        * ``host_location``: unifica variantes ("Bogota"/"Bogotá").
        * ``name``, ``description``: sin HTML ni espacios repetidos.
        """
        oficial = {_sin_tildes(n).lower(): n for n in LOCALIDADES_OFICIALES}
        clave = df["neighbourhood_cleansed"].map(lambda v: _sin_tildes(str(v)).lower())
        df["localidad"] = clave.map(oficial).fillna(clave.map(lambda k: oficial.get(f"la {k}")))
        sin_match = df.loc[df["localidad"].isna(), "neighbourhood_cleansed"].unique()
        if len(sin_match):
            self.log.warning("[listings] Localidades no reconocidas: %s", list(sin_match))
            df["localidad"] = df["localidad"].fillna(df["neighbourhood_cleansed"])
        df = df.drop(columns=["neighbourhood_cleansed"])

        df["host_location"] = self.canonizar_texto(df["host_location"]).fillna("No especificado")
        for col in ["name", "description", "host_about", "host_name"]:
            if col in df:
                df[col] = self.limpiar_texto(df[col])
        self._registrar("listings", "Estandarizar textos y localidades", len(df), len(df),
                        f"{df['localidad'].nunique()} localidades oficiales")
        return df

    def _procesar_amenities(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Desanida ``amenities`` (lista JSON guardada como texto).

        Resultado:
        * ``amenities_count``: cantidad de amenities por alojamiento;
        * columnas 0/1 para las amenities clave definidas en config.py;
        * tabla larga ``listing_amenities`` (una fila por alojamiento y amenity)
          y ``amenities_resumen`` con su frecuencia. Una lista dentro de una
          celda no se puede filtrar ni agregar en SQL; la tabla larga sí.
        """
        def leer(valor):
            try:
                return [a.strip() for a in json.loads(valor)] if isinstance(valor, str) else []
            except json.JSONDecodeError:
                return []

        listas = df["amenities"].apply(leer)
        df["amenities_count"] = listas.str.len().astype("int16")
        texto = listas.map(lambda l: "|".join(l).lower())
        for columna, patron in AMENITIES_CLAVE.items():
            df[columna] = texto.str.contains(patron, regex=True).astype("int8")

        largo = (pd.DataFrame({"listing_id": df["id"], "amenity": listas})
                 .explode("amenity").dropna(subset=["amenity"]))
        largo = largo[largo["amenity"] != ""].drop_duplicates()
        resumen = (largo.groupby("amenity").size().rename("num_listings")
                   .sort_values(ascending=False).reset_index())
        resumen["porcentaje_listings"] = (resumen["num_listings"] / len(df) * 100).round(2)

        self.tablas["listing_amenities"] = largo.reset_index(drop=True)
        self.tablas["amenities_resumen"] = resumen
        df = df.drop(columns=["amenities"])
        self._registrar("listing_amenities", "Desanidar amenities (JSON -> tabla larga)",
                        len(df), len(largo), f"{len(resumen):,} amenities distintas")
        return df

    def _crear_dimension_hosts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Separa la información del anfitrión en la tabla ``hosts``.

        Por qué: un mismo host puede tener decenas de alojamientos; repetir sus
        datos en cada fila es redundante y puede generar inconsistencias. Se
        deja ``host_id`` en listings como llave foránea. Se agrega
        ``tipo_anfitrion`` (Individual / Pequeño / Profesional) según cuántos
        alojamientos administra, variable útil para analizar la
        profesionalización del mercado.
        """
        columnas = [c for c in COLUMNAS_HOST if c in df.columns]
        hosts = df[columnas].copy()
        inconsistentes = int(hosts.groupby("host_id")["host_is_superhost"].nunique().gt(1).sum())
        if inconsistentes:
            self.log.warning("[hosts] %d hosts con valores distintos entre sus listings; "
                             "se conserva el último registro", inconsistentes)
        hosts = hosts.drop_duplicates(subset="host_id", keep="last")
        hosts["tipo_anfitrion"] = pd.cut(hosts["calculated_host_listings_count"],
                                         bins=[0, 1, 5, np.inf],
                                         labels=["Individual (1)", "Pequeño (2-5)", "Profesional (6+)"]
                                         ).astype("string")
        self.tablas["hosts"] = hosts.reset_index(drop=True)
        df = df.drop(columns=[c for c in columnas if c != "host_id"])
        self._registrar("hosts", "Desanidar información del host (dimensión)", len(df), len(hosts),
                        f"{len(hosts):,} anfitriones únicos para {len(df):,} alojamientos")
        return df

    def transformar_listings(self) -> pd.DataFrame:
        """Aplica, en orden, todas las transformaciones de la tabla listings."""
        df = self.crudos["Listings"].copy()
        self.log.info("--- Transformando LISTINGS (%s filas, %d columnas) ---",
                      f"{len(df):,}", df.shape[1])
        df = self.eliminar_duplicados(df, "listings", llave=["id"])
        df = self._eliminar_columnas_vacias(df)
        df = self._desanidar_cotizacion(df)
        df = self._limpiar_precios(df)
        df = self._categorizar_precio(df)
        df = self._convertir_booleanos(df, ["host_is_superhost", "host_has_profile_pic",
                                            "host_identity_verified", "has_availability"], "listings")
        for col in ["last_scraped", "first_review", "last_review",
                    "price_quote_checkin_date", "price_quote_checkout_date"]:
            if col in df:
                df[col] = self.estandarizar_fecha(df[col])
        self._registrar("listings", "Fechas -> YYYY-MM-DD", len(df), len(df),
                        "last_scraped, first_review, last_review, price_quote_*_date")
        df = self.acotar_noches(df, "listings", ["maximum_nights", "minimum_maximum_nights",
                                                 "maximum_maximum_nights", "maximum_nights_avg_ntm"])
        df["disponible_todo_el_anio"] = (df["availability_365"] == 365).astype("int8")
        df = self._tratar_nulos_listings(df)
        df = self._procesar_banos(df)
        df = self._estandarizar_textos_listings(df)
        df = self._procesar_amenities(df)
        df = self._crear_dimension_hosts(df)
        self.log.info("Listings final: %s filas, %d columnas", f"{len(df):,}", df.shape[1])
        return df.reset_index(drop=True)

    def transformar_calendar(self, listings: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el calendario diario (≈7 millones de filas).

        * Duplicados por (listing_id, date).
        * Fecha a YYYY-MM-DD y variables derivadas (año, mes, día, trimestre,
          día de la semana, semana ISO, fin de semana).
        * ``available`` t/f -> ``disponible`` 1/0 y ``no_disponible`` (1 =
          reservado o bloqueado por el anfitrión; Airbnb no distingue ambos).
        * Noches mínimas/máximas acotadas al máximo válido.
        * Integridad referencial: se descartan días de alojamientos que no
          existan en listings.
        * Este scrape de Inside Airbnb no trae precio diario; se toma el precio
          ajustado del alojamiento para estimar ingresos en el resumen mensual.

        A diferencia de las otras tablas, el calendario crudo (~7 millones de
        filas) no se copia: se transforma sobre el mismo DataFrame para no
        duplicar ~1 GB de memoria.
        """
        df = self.crudos.pop("Calendar")
        self.log.info("--- Transformando CALENDAR (%s filas) ---", f"{len(df):,}")
        df = self.eliminar_duplicados(df, "calendar", llave=["listing_id", "date"])

        antes = len(df)
        validos = df["listing_id"].isin(listings["id"])
        if not validos.all():
            df = df[validos]
            self.log.warning("[calendar] %d filas de alojamientos inexistentes eliminadas", antes - len(df))
        self._registrar("calendar", "Integridad referencial con listings", antes, len(df))

        df["disponible"] = (df["available"].astype("string") == "t").astype("int8")
        df["no_disponible"] = (1 - df["disponible"]).astype("int8")
        df = df.drop(columns=["available"])
        df = self.acotar_noches(df, "calendar", ["maximum_nights"])
        for col in ["minimum_nights", "maximum_nights"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

        df = self.derivar_variables_fecha(df, "date")
        nulas = int(df["date"].isna().sum())
        if nulas:
            self.log.warning("[calendar] %d fechas inválidas eliminadas", nulas)
            df = df.dropna(subset=["date"])
        self._registrar("calendar", "Fecha YYYY-MM-DD + año/mes/día/trimestre/semana",
                        antes, len(df), f"rango {df['date'].min()} a {df['date'].max()}")
        return df.reset_index(drop=True)

    def resumir_calendar(self, calendar: pd.DataFrame, listings: pd.DataFrame) -> None:
        """
        Agrupa el calendario por mes (por alojamiento) y por semana (ciudad).

        Por qué: 7 millones de filas no caben en Excel (máx. 1.048.576 por hoja)
        y para el análisis de temporada basta el nivel mensual/semanal. La
        ``tasa_no_disponible`` aproxima la ocupación (incluye bloqueos del host).
        """
        precios = listings.set_index("id")[["price_ajustado", "localidad", "room_type"]]
        mensual = (calendar.groupby(["listing_id", "anio", "mes"], observed=True)
                   .agg(dias=("date", "size"), dias_disponibles=("disponible", "sum"),
                        dias_no_disponibles=("no_disponible", "sum"),
                        min_noches_promedio=("minimum_nights", "mean"))
                   .reset_index())
        mensual["periodo"] = (mensual["anio"].astype(str) + "-" +
                              mensual["mes"].astype(str).str.zfill(2))
        mensual["tasa_no_disponible"] = (mensual["dias_no_disponibles"] / mensual["dias"]).round(4)
        mensual = mensual.join(precios, on="listing_id")
        mensual["ingreso_estimado_cop"] = (mensual["dias_no_disponibles"] * mensual["price_ajustado"]).round(0)
        mensual["min_noches_promedio"] = mensual["min_noches_promedio"].astype(float).round(2)
        self.tablas["calendar_mensual"] = mensual
        self._registrar("calendar_mensual", "Agrupar calendario por alojamiento y mes",
                        len(calendar), len(mensual))

        diario = (calendar.groupby("date", observed=True)
                  .agg(dias_alojamiento=("disponible", "size"),
                       dias_disponibles=("disponible", "sum"),
                       dias_no_disponibles=("no_disponible", "sum")))
        fecha = pd.to_datetime(diario.index.astype(str))
        diario["semana_inicio"] = (fecha - pd.to_timedelta(fecha.dayofweek, unit="D")).strftime("%Y-%m-%d")
        semanal = diario.groupby("semana_inicio").sum().reset_index()
        semanal["tasa_no_disponible"] = (semanal["dias_no_disponibles"] / semanal["dias_alojamiento"]).round(4)
        self.tablas["calendar_semanal"] = semanal
        self._registrar("calendar_semanal", "Agrupar calendario por semana (ciudad)",
                        len(calendar), len(semanal))

    def transformar_reviews(self, listings: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia las reseñas.

        * Duplicados por ``id`` de reseña.
        * Integridad referencial con listings.
        * ``comments``: se quitan etiquetas ``<br/>`` (≈69 mil reseñas las
          traen) y espacios repetidos. Los 191 comentarios nulos se rellenan
          con "Sin comentario" en lugar de borrar la fila, porque la reseña
          sigue siendo evidencia de una estadía (actividad del alojamiento).
        * ``longitud_comentario`` y ``comentario_corto`` (≤3 caracteres como
          "." u "Ok"), útiles para futuros análisis de texto/sentimiento.
        * Fecha a YYYY-MM-DD y variables año/mes/día/trimestre.
        """
        df = self.crudos.pop("Reviews")
        self.log.info("--- Transformando REVIEWS (%s filas) ---", f"{len(df):,}")
        df = self.eliminar_duplicados(df, "reviews", llave=["id"])

        antes = len(df)
        df = df[df["listing_id"].isin(listings["id"])]
        if len(df) < antes:
            self.log.warning("[reviews] %d reseñas de alojamientos inexistentes eliminadas", antes - len(df))
        self._registrar("reviews", "Integridad referencial con listings", antes, len(df))

        nulos = int(df["comments"].isna().sum())
        df["comments"] = self.limpiar_texto(df["comments"])
        vacios = df["comments"].isna() | (df["comments"] == "")
        df["comments"] = df["comments"].mask(vacios, "Sin comentario")
        df["comentario_vacio"] = vacios.astype("int8")
        df["longitud_comentario"] = df["comments"].str.len().where(~vacios, 0).astype("int32")
        df["comentario_corto"] = ((df["longitud_comentario"] <= 3) & ~vacios).astype("int8")
        df["reviewer_name"] = self.limpiar_texto(df["reviewer_name"])
        if nulos:
            self.log.warning("[reviews] %d comentarios nulos rellenados con 'Sin comentario'", nulos)
        self._registrar("reviews", "Limpiar comentarios (HTML, espacios, nulos)", len(df), len(df),
                        f"nulos/vacíos={int(vacios.sum())}, cortos={int(df['comentario_corto'].sum())}")

        df = self.derivar_variables_fecha(df, "date")
        self._registrar("reviews", "Fecha YYYY-MM-DD + año/mes/día/trimestre", len(df), len(df),
                        f"rango {df['date'].min()} a {df['date'].max()}")
        return df.reset_index(drop=True)

    def resumir_reviews(self, reviews: pd.DataFrame) -> None:
        """Agrupa las reseñas por año y mes para analizar la evolución de la demanda."""
        mensual = (reviews.groupby(["anio", "mes"])
                   .agg(num_resenas=("id", "size"), listings_resenados=("listing_id", "nunique"),
                        longitud_promedio=("longitud_comentario", "mean"))
                   .reset_index())
        mensual["periodo"] = mensual["anio"].astype(str) + "-" + mensual["mes"].astype(str).str.zfill(2)
        mensual["longitud_promedio"] = mensual["longitud_promedio"].round(1)
        self.tablas["reviews_mensual"] = mensual
        self._registrar("reviews_mensual", "Agrupar reseñas por mes", len(reviews), len(mensual))

    def transformar_todo(self) -> dict[str, pd.DataFrame]:
        """
        Ejecuta todas las transformaciones y devuelve las tablas limpias.

        Returns
        -------
        dict[str, pandas.DataFrame]
            Tablas listas para la carga (ver docstring del módulo).
        """
        self.log.info("=== Inicio de la fase de TRANSFORMACIÓN ===")
        try:
            listings = self.transformar_listings()
            self.tablas["listings"] = listings

            calendar = self.transformar_calendar(listings)
            self.tablas["calendar"] = calendar
            self.resumir_calendar(calendar, listings)

            reviews = self.transformar_reviews(listings)
            self.tablas["reviews"] = reviews
            self.resumir_reviews(reviews)
        except Exception as e:
            self.log.error("Error durante la transformación: %s", e, exc_info=True)
            raise

        self.tablas["auditoria_transformaciones"] = pd.DataFrame(self.auditoria)
        orden = ["listings", "hosts", "listing_amenities", "amenities_resumen", "calendar",
                 "calendar_mensual", "calendar_semanal", "reviews", "reviews_mensual",
                 "auditoria_transformaciones"]
        self.tablas = {k: self.tablas[k] for k in orden}
        for nombre, tabla in self.tablas.items():
            self.log.info("Tabla lista para carga: %-26s %s filas x %d columnas",
                          nombre, f"{len(tabla):,}", tabla.shape[1])
        return self.tablas
