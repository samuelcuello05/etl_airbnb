# Proceso ETL — Airbnb Bogotá, D.C. (MongoDB → Python → SQLite / XLSX)

Taller evaluativo 2 · **Inteligencia de Negocios** · Institución Universitaria ITM

## Descripción y objetivo

Proceso **ETL automatizado en Python** sobre los datos públicos de
[Inside Airbnb](https://insideairbnb.com/get-the-data/) para **Bogotá, Distrito Capital, Colombia**
(scrape del 21-22 de junio de 2026).

1. **Extracción** — se conecta a una base de datos **MongoDB local** (`airbnb_bogota_etl`) con las
   colecciones `Listings`, `Calendar` y `Reviews` y las carga en DataFrames de pandas.
2. **Análisis exploratorio (EDA)** — notebook con la estructura, calidad y distribución de los datos,
   gráficas interpretadas y las decisiones que justifican cada transformación.
3. **Transformación** — limpieza de nulos y duplicados, normalización de precios, fechas en formato
   `YYYY-MM-DD` con variables derivadas, categorización de precios, desanidado de campos JSON
   (amenities, host, cotización) y resúmenes mensuales/semanales.
4. **Carga** — inserción en una base de datos **SQLite** nueva, exportación a **XLSX** y verificación de
   que los registros se cargaron correctamente.

Todas las fases registran sus eventos (INFO / WARNING / ERROR) en un archivo de log por ejecución:
`logs/log_YYYYMMDD_HHMM.txt`.

```
CSV.gz ──(cargar_mongo.py)──► MongoDB ──(Extraccion)──► DataFrames crudos
                                       ──(Transformacion)──► 10 tablas limpias
                                       ──(Carga)──► SQLite + XLSX + verificación
```

## Estructura del repositorio

```
etl_airbnb/
├── data/
│   ├── raw/                     # listings / calendar / reviews .csv.gz (fuente para poblar MongoDB)
│   └── processed/               # airbnb_bogota.db (SQLite) y archivos .xlsx generados por el ETL
├── src/
│   ├── config.py                # rutas, conexión a MongoDB y reglas de negocio (rangos de precio…)
│   ├── logger.py                # módulo centralizado de logs (un archivo por ejecución)
│   ├── cargar_mongo.py          # paso previo: crea las colecciones en MongoDB desde data/raw
│   ├── extraccion.py            # clase Extraccion
│   ├── transformacion.py        # clase Transformacion
│   ├── carga.py                 # clase Carga
│   └── main.py                  # orquestador del ETL (E → T → L)
├── notebooks/
│   └── exploracion_airbnb.ipynb # análisis exploratorio de datos
├── reports/
│   ├── figures/                 # gráficas exportadas por el notebook
│   └── Informe_ETL_Airbnb_Bogota.pdf
├── logs/                        # log_YYYYMMDD_HHMM.txt
├── README.md
└── requirements.txt
```

## Requisitos

- **Python 3.11 o superior** (probado con 3.14)
- **MongoDB Community Server** corriendo en `mongodb://localhost:27017/`
  (para otra dirección, defina la variable de entorno `MONGO_URI`; para otro nombre de base, `MONGO_DB`)
- ~4 GB de RAM libres (el calendario tiene 7 millones de registros)

## Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/samuelcuello05/etl_airbnb.git
cd etl_airbnb

# 2. Crear y activar el entorno virtual
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux / macOS

# 3. Instalar dependencias
pip install -r requirements.txt
```

## Ejecución

```bash
# 1. (Una sola vez) Crear la base airbnb_bogota_etl y las colecciones Listings, Calendar y Reviews
#    a partir de data/raw. Valida que los documentos en MongoDB coincidan con las filas del CSV (~2 min).
python src/cargar_mongo.py

# 2. Ejecutar el proceso ETL completo: MongoDB -> SQLite + XLSX (~5 min)
python src/main.py
#    Opciones:  --cargar-mongo  (ejecuta el paso 1 antes del ETL)
#               --sin-xlsx      (omite la exportación a Excel)

# 3. Abrir / reejecutar el análisis exploratorio
jupyter notebook notebooks/exploracion_airbnb.ipynb
#    o de forma no interactiva:
jupyter nbconvert --to notebook --execute --inplace notebooks/exploracion_airbnb.ipynb
```

### Salidas generadas

| Archivo | Contenido |
|---|---|
| `data/processed/airbnb_bogota.db` | Base SQLite con 10 tablas (ver abajo) |
| `data/processed/airbnb_bogota_resumen.xlsx` | Todas las tablas que caben en Excel (listings, hosts, amenities, resúmenes, auditoría) |
| `data/processed/airbnb_bogota_reviews.xlsx` | Reseñas limpias (~528 mil filas) |
| `logs/log_YYYYMMDD_HHMM.txt` | Log de la ejecución |

| Tabla SQLite | Descripción |
|---|---|
| `listings` | Un registro por alojamiento: precio numérico (COP), `price_ajustado`, `categoria_precio`, `localidad` oficial, banderas de calidad y de amenities |
| `hosts` | Dimensión de anfitriones (desanidada de listings) con `tipo_anfitrion` |
| `listing_amenities` / `amenities_resumen` | Amenities desanidadas (formato largo) y su frecuencia |
| `calendar` | Calendario diario (7 M filas) con año, mes, día, trimestre, día de la semana, semana ISO y fin de semana |
| `calendar_mensual` / `calendar_semanal` | Disponibilidad resumida por alojamiento-mes (con ingreso estimado) y por semana |
| `reviews` / `reviews_mensual` | Reseñas limpias con variables de fecha y su resumen mensual |
| `auditoria_transformaciones` | Bitácora de cada transformación: filas antes y después |

> El calendario diario (7 M filas) supera el límite de Excel (1.048.576 filas por hoja), por eso se exporta
> resumido (`calendar_mensual`, `calendar_semanal`) y completo solo en SQLite. El log lo registra como WARNING.

## Ejemplo de ejecución del proceso ETL

```text
$ python src/main.py
2026-10-06 21:46:50 | INFO     | etl.main | ########## INICIO DEL PROCESO ETL AIRBNB BOGOTÁ ##########
2026-10-06 21:46:50 | INFO     | etl.extraccion | Conexión exitosa a MongoDB mongodb://localhost:27017/ (versión 8.3.8), base de datos 'airbnb_bogota_etl'
2026-10-06 21:46:51 | INFO     | etl.extraccion | Colección 'Listings': 19,187 registros y 90 columnas extraídos (1.0s)
2026-10-06 21:47:27 | INFO     | etl.extraccion | Colección 'Calendar': 7,003,255 registros y 5 columnas extraídos (35.6s)
2026-10-06 21:47:31 | INFO     | etl.extraccion | Colección 'Reviews': 527,731 registros y 6 columnas extraídos (3.7s)
2026-10-06 21:47:31 | INFO     | etl.main | Fase de extracción completada en 40.6s
2026-10-06 21:47:31 | INFO     | etl.transformacion | [listings] Columnas 100% nulas eliminadas (13): ['neighborhood_overview', 'host_since', ...]
2026-10-06 21:47:35 | WARNING  | etl.transformacion | [listings] 195 alojamientos sin precio; se imputan con la mediana por localidad y tipo de habitación
2026-10-06 21:47:35 | INFO     | etl.transformacion | [listings] Normalizar precio ($ y , -> número COP) | filas: 19,187 -> 19,187 | imputados=195, outliers=667, ...
2026-10-06 21:47:38 | INFO     | etl.transformacion | [listing_amenities] Desanidar amenities (JSON -> tabla larga) | filas: 19,187 -> 505,595 | 3,687 amenities distintas
2026-10-06 21:47:47 | INFO     | etl.main | Fase de transformación completada en 16.2s
2026-10-06 21:48:02 | INFO     | etl.carga | SQLite: tabla 'calendar' cargada con 7,003,255 registros (14.2s)
2026-10-06 21:48:35 | WARNING  | etl.carga | XLSX: 'calendar' tiene 7,003,255 filas y supera el límite de Excel (1,048,575); no se exporta a XLSX (queda completa en SQLite)
2026-10-06 21:49:52 | INFO     | etl.carga | Verificación SQLite OK  calendar                   7,003,255 = 7,003,255
2026-10-06 21:49:54 | INFO     | etl.carga | Verificación de tipos OK: price es REAL y todas las fechas son YYYY-MM-DD
2026-10-06 21:49:54 | INFO     | etl.carga | Carga verificada correctamente: todos los registros coinciden
2026-10-06 21:49:54 | INFO     | etl.main | ########## FIN DEL PROCESO ETL (OK) en 183.6s ##########
```

El log completo de esa ejecución está en [`logs/log_20261006_2146.txt`](logs/log_20261006_2146.txt) y el de la
carga inicial de MongoDB en [`logs/log_20261006_2119.txt`](logs/log_20261006_2119.txt).

## Principales decisiones de transformación

| Hallazgo del EDA | Decisión |
|---|---|
| `price` como texto (`"$284,294.01"`), 195 nulos y un máximo de 448 M COP/noche | Número en COP; imputación por mediana de localidad × tipo de habitación (`precio_imputado`); atípicos marcados (`precio_outlier`) y `price_ajustado` winsorizado a P1–P99 |
| 13 columnas 100 % vacías | Se eliminan |
| `maximum_nights` = 2.147.483.647 | Se acota a 1.125 (máximo de Airbnb) |
| 22 % de alojamientos sin reseñas | Las calificaciones no se imputan; bandera `tiene_resenas` |
| `amenities`, host y `price_quote_raw` anidados | Tablas `listing_amenities` y `hosts`; validación de moneda COP |
| Variantes de texto (`Bogota`/`Bogotá`), localidades sin tilde, `<br/>` en comentarios | Texto canonizado, nombres oficiales de las 20 localidades, HTML eliminado |
| Calendario de 7 M filas sin precio diario | Variables de fecha, `disponible`/`no_disponible`, resúmenes mensual y semanal |

El detalle y la justificación de cada decisión están en el notebook (sección 7) y en los docstrings de
`src/transformacion.py`.

## Integrantes y responsabilidades

| Integrante | Responsabilidades |
|---|---|
| Samuel Cristobal Cuello Duque | Desarrollo completo del taller (trabajo individual): carga de MongoDB, extracción, análisis exploratorio, transformación, carga, módulo de logs, informe final y README |

**Docente:** Carlos Andrés Jaramillo Patiño

## Fuente de datos

Inside Airbnb — Bogotá, Distrito Capital, Colombia: `listings.csv.gz` (Detailed Listings data),
`calendar.csv.gz` (Detailed Calendar Data), `reviews.csv.gz` (Detailed Review Data).
Datos bajo licencia [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).
