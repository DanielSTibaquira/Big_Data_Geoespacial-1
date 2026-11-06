# Datos locales

Los datasets originales se mantienen localmente y no se versionan en Git. El notebook del trabajo previo puede usar `train.csv` para explorar los datos de taxis; la entrega LifeMap usa los archivos JSON Lines de Yelp.

La descarga reproducible de Yelp está definida en `ingestion/yelp_ingest.py` usando KaggleHub. Jenkins monta la caché en `data/kagglehub`; una ejecución manual usa la caché estándar de KaggleHub del usuario. En Jenkins configura una credencial **Secret file** con ID `kaggle-json`. No guardes aquí tokens ni archivos `kaggle.json`.

## Yelp Open Dataset

La adaptación a Yelp descarga `business.json` y `review.json` mediante KaggleHub y los lee como JSON Lines particionados con Dask:

```powershell
python -m ingestion.yelp_dataset
```

El comando imprime el directorio de KaggleHub, los esquemas, el número de particiones y una pequeña vista previa sin exponer el texto de las reseñas. Para volver a leer una descarga existente, pasa `--dataset-dir`; para solicitar una descarga nueva, usa `--force-download`. Configura el acceso a Kaggle fuera del repositorio y no guardes credenciales ni datos descargados en Git.

El área inicial de estudio son los negocios de PA, NJ o DE dentro de un radio de 50 km alrededor de Philadelphia City Hall (39.9526, -75.1636); la familia seleccionada es `Restaurants`. La delimitación operacional es reproducible y descarta inconsistencias de estado en el origen, pero no equivale a la frontera administrativa oficial de la MSA.

La inspección de la versión descargada encontró 150.346 negocios en el archivo, 52.268 con categoría `Restaurants`, 16.943 en los estados seleccionados y 16.407 dentro del radio con coordenadas válidas. De las 6.990.280 reseñas del archivo, 1.296.634 corresponden a esos negocios. Una validación Dask de texto, estrellas y fechas mantuvo las 1.296.634 filas (antes de deduplicar), por encima del mínimo de 1.000.000. Estos conteos describen esa versión del dataset; la ingesta guarda el reporte de conteos de la ejecución bajo `data/`.
