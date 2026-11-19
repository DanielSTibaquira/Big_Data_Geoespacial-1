# LifeMap: reputación, ubicación y supervivencia de negocios

Proyecto académico de Big Data para analizar reputación, ubicación y supervivencia de restaurantes en el área de Philadelphia usando Yelp Open Dataset. Se reutilizan componentes del trabajo geoespacial de taxis, que se conserva como flujo previo y no como dataset de la entrega final.

## Alcance de la entrega final

- **Fuente:** [Yelp Open Dataset en Kaggle](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset), con `business.json` y `review.json` en formato JSON Lines.
- **Área:** negocios de PA, NJ o DE a no más de 50 km de Philadelphia City Hall (39.9526, -75.1636).
- **Familia:** `Restaurants`, detectada como categoría completa y no como coincidencia parcial.
- **Ingesta nueva:** `ingestion/yelp_dataset.py` descarga/inspecciona la fuente; `ingestion/yelp_ingest.py` filtra, limpia, etiqueta reseñas y carga negocios y reseñas en MongoDB.
- **Persistencia:** base `lifemap`, colecciones `businesses` y `reviews`; cada negocio contiene un punto GeoJSON y la aproximación de vida/evento/censura calculada desde sus reseñas.
- **Estado:** ingesta completa ejecutada (16.407 negocios y 1.296.634 reseñas); Spark MLlib, tasas MapReduce y benchmark ejecutados localmente. API y `/predict` probados con el modelo generado. El despliegue público sigue pendiente de un servidor/URL autorizado.

El área es una delimitación operacional reproducible del equipo, no la frontera administrativa oficial de la MSA. El README mantiene también las instrucciones del dataset de taxis para reproducir los experimentos anteriores.

## Estado

La infraestructura local está definida en `docker-compose.yml`: MongoDB, Dask, Spark y una API Flask. Jenkins y los contenedores de benchmark se activan con perfiles opcionales.

## Dataset

El trabajo previo usó [Taxi Trajectory Data](https://www.kaggle.com/datasets/crailtap/taxi-trajectory), de aproximadamente 1,94 GB. Ese flujo se conserva para reproducir el análisis anterior; no sustituye la fuente Yelp de LifeMap.

## Ingesta Yelp con Dask

Requiere Docker Compose v2 y una conexión a Internet la primera vez que se descarga el dataset. Los datos se guardan bajo `data/` (excluida de Git) y las credenciales de Kaggle se configuran fuera del repositorio.

Primero inicia MongoDB y el clúster Dask:

```powershell
docker compose up -d mongodb dask-scheduler dask-worker-1 dask-worker-2
```

Para inspeccionar esquemas y probar la selección sin MongoDB:

```powershell
python -m pip install -r ingestion/requirements.txt
python -m ingestion.yelp_dataset --rows 2
```

La ingesta real descarga Yelp con KaggleHub si no está en caché, conserva el dataset fuera de Git y procesa las reseñas por particiones:

```powershell
docker compose --profile jobs run --build --rm yelp-ingest --limit 10000 --report /data/yelp_smoke_report.json
```

`--limit` restringe las primeras filas fuente de reseñas para una prueba funcional; no representa la muestra mínima para el análisis final. Para la ejecución completa, omite `--limit`:

```powershell
docker compose --profile jobs run --build --rm yelp-ingest --report /data/yelp_ingestion_report.json
```

El proceso crea índices `2dsphere` para `businesses.location` y un índice por `reviews.business_id`. Los documentos de reseña guardan estrellas, etiqueta derivada (1–2 negativa, 3 neutral, 4–5 positiva), texto, fecha y referencia al negocio. Los documentos de negocio incluyen GeoJSON, categorías y `life` (primera/última reseña, días-negocio, evento y censura). La carga es idempotente por `business_id`/`review_id`; usa `--replace-collections` solo cuando quieras borrar y reconstruir ambas colecciones.

El reporte JSON registra cantidades de negocios que superan cada filtro geográfico y los conteos de reseñas leídas, fuera del área, sin texto, con estrellas/fecha inválidas, duplicadas y aceptadas. La vida inferida entre reseñas es una aproximación y no la fecha real de apertura o cierre.

## Sentimiento, tasas y balanceo con Spark

Después de cargar el dataset completo, ejecuta el job Spark conectado al mismo MongoDB. El umbral de 365 días-negocio equivale a un año de exposición agregado y evita publicar tasas basadas en exposición insignificante:

```powershell
docker compose up -d spark-master spark-worker
docker compose exec spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1536m --conf spark.executor.cores=1 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 --conf spark.jars.ivy=/tmp/spark-ivy --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017 --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017 /opt/spark-apps/jobs/lifemap_batch.py --minimum-days-business 365 --report /lifemap/batch_report.json'
```

El job separa entrenamiento y prueba por `business_id` (80/20 determinista), entrena Naive Bayes y regresión logística sobre tokenización, stopwords inglesas, CountVectorizer e IDF, y guarda métricas por clase, matrices de confusión y modelos. También exporta `data/lifemap/model_serving.json` para `/predict`. El MapReduce emite y suma cierres/días-negocio con `reduceByKey` por categoría principal y geohash; valida ambos resultados contra un `groupBy` DataFrame antes de guardar `lifemap.closure_rates`. Por defecto mide 2, 8, 32 y 200 particiones, tres repeticiones por configuración y filas por partición al repartir sin clave y por ciudad. Los resultados quedan en `data/lifemap/batch_report.json`.

Compose construye `lifemap-spark:3.5.7` con NumPy 1.24.4, requerido por MLlib; el worker anuncia 2 GB y el comando reserva 1536 MB para el executor.

### Modelo congelado para la Entrega 2

Para evitar fuga temporal, el modelo de sentimiento de E2 se entrena solo con reseñas fechadas hasta `2021-01-01T00:00:00Z` (inclusive). Este comando guarda sus artefactos con nombres versionados bajo `data/lifemap/`, sin reemplazar el modelo de E1 ni el reporte habitual:

```powershell
docker compose exec spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1536m --conf spark.executor.cores=1 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 --conf spark.jars.ivy=/tmp/spark-ivy --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017 --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017 /opt/spark-apps/jobs/lifemap_batch.py --minimum-days-business 365 --sentiment-train-cutoff 2021-01-01T00:00:00Z --skip-benchmark --model-dir /lifemap/models_as_of_2021-01-01 --serving-model /lifemap/model_serving_as_of_2021-01-01.json --report /lifemap/batch_report_as_of_2021-01-01.json'
```

El reporte registra el corte UTC y cuántas reseñas fueron excluidas por ser posteriores al corte. El productor E2 lee solo reseñas posteriores a T y el consumidor de Structured Streaming carga este modelo congelado; el flujo no lo reentrena. La API de E1 continúa usando `model_serving.json`.

## Entrega 2: Kafka, streaming y supervivencia

La Entrega 2 agrega Kafka en modo KRaft, replay temporal, inferencia con Spark Structured Streaming, ventanas geohash, curvas Kaplan–Meier, modelo de riesgo y alertas. El perfil `e2` evita iniciar el streaming automáticamente al levantar E1. Antes de usarlo, deben existir las colecciones Yelp y los artefactos temporales de sentimiento.

Desde la raíz del proyecto:

```powershell
# En una instalación nueva, descarga e ingesta Yelp si MongoDB aún no tiene los datos.
docker compose --profile jobs run --build --rm yelp-ingest --report /data/yelp_ingestion_report.json

# Entrena y exporta el modelo de sentimiento limitado a T siguiendo el comando de arriba.

# Calcula curvas de supervivencia y entrena el modelo de riesgo.
docker compose --profile jobs run --build --rm lifemap-e2-analytics

# Levanta Kafka, topic y streaming.
$env:LIFEMAP_STREAMING_ENABLED = "true"
docker compose --profile e2 up --build -d
docker compose --profile e2 ps
```

Kafka crea el topic `reviews` con tres particiones. El job `lifemap-e2-analytics` construye las curvas y el clasificador de riesgo antes de iniciar el perfil de streaming. Structured Streaming procesa reseñas con checkpoint persistente en `data/lifemap/checkpoints/`. La reproducción de prueba se limita a diez reseñas:

```powershell
docker compose --profile e2 --profile jobs run --build --rm replay-reviews --rate 5 --limit 10
```

Para la demostración, se puede aumentar la velocidad con `--rate` y omitir `--limit` para reproducir todas las reseñas posteriores a T. El productor ordena por fecha y publica usando `business_id` como clave. El consumidor guarda sentimiento predicho, probabilidades, hora de procesamiento y lote; los upserts por `review_id` permiten reintentar sin duplicar reseñas. Reiniciar el productor vuelve a publicar el rango post-T; Spark conserva su checkpoint y los upserts son idempotentes.

El watermark es de 7 días y las ventanas de geohash son de 30 días. Se marca hotspot cuando hay al menos 10 reseñas en la ventana y al menos 60 % son negativas según el clasificador congelado: el mínimo reduce alertas por celdas con pocas observaciones y el porcentaje identifica concentración de experiencia negativa. Las coordenadas guardadas son el centro de la celda geohash.

El job de riesgo usa reseñas hasta T para las variables base, actividad después de T para construir la etiqueta a 12 meses y excluye negocios sin actividad post-T. La regresión logística pondera clases inversamente a su frecuencia; el corte de alerta por defecto es 0,5 y se reportan AUC-ROC, AUC-PR y recall. La densidad de competidores usa `$geoNear` con radio de 500 m y categoría principal. Las curvas tratan los negocios abiertos como censurados y usan la primera reseña como entrada tardía; primera/última reseña son proxies, no fechas reales de apertura y cierre. Grupos con menos de 20 negocios se suprimen.

Con el perfil E2 activo, `/health` añade Kafka, estado del stream y retraso del consumidor en offsets. Endpoints nuevos:

```text
GET  /live/hotspots?lon=-75.16&lat=39.95&radius_m=5000&window_days=30
GET  /alerts?lon=-75.16&lat=39.95&radius_m=5000
POST /alerts  {"polygon":{"type":"Polygon","coordinates":[...]}, "limit":100}
GET  /survival/Italian
GET  /survival/Italian?polygon=<GeoJSON codificado en URL>
GET  /risk/near?lon=-75.16&lat=39.95&radius_m=5000
```

`/live/hotspots` acepta `include_all=true` para devolver también ventanas que no cruzan el umbral, útil para graficar la evolución del porcentaje negativo. Las alertas se consultan por radio o polígono; `/survival/<categoria>` acepta un polígono GeoJSON serializado en el parámetro `polygon` para calcular la curva de la zona usando `$geoWithin`. Las consultas geoespaciales requieren índices 2dsphere.

Las consultas M de [docs/powerbi/](./docs/powerbi/) conectan Power BI a hotspots, alertas, supervivencia y evolución del porcentaje negativo. En Power BI Desktop crea una consulta en blanco por archivo, pega su contenido en el Editor avanzado y cambia `BaseUrl` por la URL pública desplegada. Configura cuatro visualizaciones (mapa, tabla de alertas, curva de supervivencia y serie temporal) y actualízalas durante la demo. Los archivos `.pq` son consultas de origen; el archivo `.pbix` y la publicación en Power BI deben crearse en Power BI Desktop.

Jenkins en `main` ejecuta la ingesta, entrena ambos modelos temporales, calcula supervivencia/riesgo, levanta el candidato E2, publica reseñas de humo y espera a que el retraso Kafka llegue a cero antes de desplegar. La prueba falla y bloquea el despliegue si falla la API, el streaming, las métricas o el modelo.

## API LifeMap

La página principal (`http://localhost:5000/`) ofrece un mapa Leaflet de restaurantes de Philadelphia, búsqueda por radio/categoría, resumen de tasas y sentimiento de la zona, y una prueba interactiva del modelo `/predict`. Haz clic en el mapa para mover el centro y luego pulsa **Buscar en el mapa**. El explorador de taxis anterior sigue disponible en `http://localhost:5000/taxi`. Leaflet, las fuentes y las teselas de OpenStreetMap se cargan desde servicios externos, por lo que la interfaz cartográfica requiere Internet.

La API conserva las rutas `/api/v1/trips/*` del trabajo geoespacial anterior y agrega las consultas de Yelp:

```text
GET  /businesses/near?lon=-75.16&lat=39.95&radius_m=1000&category=Italian
GET  /businesses/within?min_lon=-75.3&min_lat=39.8&max_lon=-75.0&max_lat=40.1
POST /businesses/within  {"polygon":{"type":"Polygon","coordinates":[...]}, "limit":100}
GET  /zones/summary?lon=-75.16&lat=39.95&radius_m=1000&min_days_business=365
GET  /stats/closure?category=Italian&min_days_business=365
GET  /stats/closure?geohash=dr4e3&min_days_business=365
POST /predict  {"text":"The service was wonderful"}
```

La API usa `$near`, `$geoWithin` y `$geoNear`, valida coordenadas, radios, GeoJSON y límites, y devuelve HTTP 503 para predicción hasta que exista el artefacto del modelo.

## Carpetas

- `data/`: datos locales, no versionados.
- `notebooks/`: notebooks de exploración y análisis.
- `docs/`: documentación del proyecto, arquitectura e informe.
- `docs/reference/`: material de referencia local, excluido de Git.
- `api/`: API REST Flask para consultas espaciales.
- `benchmark/`: jobs equivalentes de agregación con Dask y Spark.
- `jenkins/`: imagen personalizada del controlador Jenkins.

## Ejecución

Requiere Docker Desktop en ejecución y Docker Compose v2. Desde la raíz del proyecto:

```powershell
docker compose config
docker compose up -d
docker compose ps
```

Interfaces locales:

- MongoDB: `mongodb://localhost:27017`
- Panel de Dask: `http://localhost:8787`
- Panel de Spark: `http://localhost:8081`
- Panel del worker Spark: `http://localhost:8082`
- API Flask: `http://localhost:5000`

Para ver los registros o detener los servicios:

```powershell
docker compose logs -f
docker compose down
```

MongoDB conserva sus datos en el volumen `mongodb_data`; `docker compose down` no lo elimina. No uses `docker compose down -v` si quieres conservarlos.

### Ingesta de datos

Coloca `train.csv` en `data/` (el archivo y sus copias comprimidas no se versionan). Para construir la imagen de ingesta y probar primero con 5.000 filas:

```powershell
docker compose --profile jobs build ingest
docker compose --profile jobs run --rm ingest --limit 5000
```

Para procesar todo el archivo, omite `--limit`:

```powershell
docker compose --profile jobs run --rm ingest
```

El proceso usa Dask del clúster de Compose, convierte origen/destino a puntos GeoJSON, descarta registros con datos o coordenadas inválidos, hace upsert por lotes en `taxi_geospatial.trips` y crea el índice `location_2dsphere`. El `_id` se deriva de una huella estable de la fila completa: así, reejecutar la ingesta no duplica filas idénticas y no se pierden viajes distintos que compartan `TRIP_ID`.

### Agregación espacial y temporal con Spark

`spark/jobs/aggregate_trips.py` lee `taxi_geospatial.trips` con MongoDB Spark Connector y agrupa los viajes por celdas de `0.01°` y hora UTC. Guarda el centro GeoJSON de cada celda y su conteo en `taxi_geospatial.trip_aggregates`. Para ejecutar el job:

```powershell
docker compose up -d spark-master spark-worker
docker compose exec spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 --conf spark.jars.ivy=/tmp/spark-ivy --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017/taxi_geospatial.trips --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017/taxi_geospatial.trip_aggregates /opt/spark-apps/jobs/aggregate_trips.py'
```

El modo `overwrite` reemplaza la colección de agregados al volver a ejecutar el job; no altera `trips`. Para validar en MongoDB Compass o en MongoDB shell, el total de `trip_count` debe coincidir con el total de documentos de origen:

```javascript
db.trips.countDocuments({})
db.trip_aggregates.countDocuments({})
db.trip_aggregates.aggregate([
  { $group: { _id: null, total: { $sum: "$trip_count" } } }
])
```

### API Flask

La interfaz web interactiva de Leaflet está disponible en `http://localhost:5000` y la API en el mismo origen. Desde el mapa puedes buscar viajes en un radio alrededor de un punto, dibujar un polígono y consultar viajes con `$geoWithin`, o visualizar las celdas de agregados Spark dentro del área visible. La página consulta `/health` para indicar el estado de la API/MongoDB.

El mapa base usa teselas de OpenStreetMap y Leaflet se sirve desde un CDN; para ver el mapa y sus teselas se requiere conexión a Internet. Las consultas y datos pasan por la API Flask local.

La API escucha en `http://localhost:5000`. Rutas disponibles:

```text
GET /health
GET /api/v1/trips/near?longitude=-8.61&latitude=41.14&radius_m=500&limit=100
GET /api/v1/trips/within?min_lon=-8.7&min_lat=41.1&max_lon=-8.5&max_lat=41.2&limit=100
POST /api/v1/trips/within
GET /api/v1/trips/aggregate/near?longitude=-8.61&latitude=41.14&radius_m=500
GET /api/v1/aggregates/within?min_lon=-8.7&min_lat=41.1&max_lon=-8.5&max_lat=41.2&limit=100
```

`trips/near` implementa una búsqueda por radio con `$near`. El método GET de `trips/within` conserva la consulta rectangular existente y construye un polígono GeoJSON cerrado. El método POST de la misma ruta recibe un polígono GeoJSON Polygon arbitrario en el cuerpo, con posiciones 2D `[longitud, latitud]` WGS84 y anillos cerrados:

```json
{
  "polygon": {
    "type": "Polygon",
    "coordinates": [[
      [-8.62, 41.14],
      [-8.60, 41.14],
      [-8.61, 41.16],
      [-8.62, 41.14]
    ]]
  },
  "limit": 100
}
```

Envía este cuerpo a `POST /api/v1/trips/within` con `Content-Type: application/json`. `limit` es opcional (predeterminado 100, máximo 500). GeoJSON inválido, coordenadas fuera de rango o límites inválidos producen HTTP 400. Ambos métodos consultan `location` con `$geoWithin` y devuelven `count` y `results`. `trips/aggregate/near` inicia su pipeline con `$geoNear`, restringe resultados al radio indicado y agrupa por tipo de llamada, devolviendo conteo y distancias/duración medias. Las rutas espaciales crean de forma idempotente los índices `location_2dsphere` o `cell_center_2dsphere`, según corresponda; los errores de MongoDB producen HTTP 503.

### Benchmark Dask/Spark

Ambos jobs leen las mismas columnas de `data/train.csv` y agrupan el punto inicial por celda de `0.01°` y hora UTC. Inicia Spark y prepara el directorio de resultados:

```powershell
docker compose up -d spark-master spark-worker
New-Item -ItemType Directory -Force benchmark/results
```

Ejecuta Dask con una y dos instancias worker:

```powershell
docker compose --profile benchmark run --rm benchmark-dask --workers 1 --csv /data/train.csv --output /results/dask-1.json
docker compose --profile benchmark run --rm benchmark-dask --workers 2 --csv /data/train.csv --output /results/dask-2.json
```

Ejecuta Spark en contenedores aislados, en modo local con uno y dos slots:

```powershell
docker compose --profile benchmark run --rm benchmark-spark /opt/benchmark/spark_aggregate.py --master 'local[1]' --csv /data/train.csv --output /results/spark-1.json
docker compose --profile benchmark run --rm benchmark-spark /opt/benchmark/spark_aggregate.py --master 'local[2]' --csv /data/train.csv --output /results/spark-2.json
```

Cada ejecución imprime y guarda JSON con duración, paralelismo, grupos, viajes válidos y pico de memoria (`peak_memory_mib`). La memoria se lee del cgroup v2 (`memory.peak`) o v1 (`memory.max_usage_in_bytes`) del contenedor efímero e incluye el runtime de Dask/Spark y sus workers/JVM. Si el entorno no expone estas métricas, el benchmark falla explícitamente en vez de reportar un valor inventado. Compara `valid_trip_count` y `aggregate_group_count` entre motores antes de interpretar tiempos o memoria; las ejecuciones en Docker no sustituyen una medición controlada de hardware.

Medición realizada sobre el CSV completo en el entorno local del proyecto (memoria máxima observada del contenedor):

| Motor | Paralelismo | Tiempo (s) | Pico memoria (MiB) | Viajes válidos | Grupos |
|---|---:|---:|---:|---:|---:|
| Dask | 1 worker | 83.90 | 668.44 | 1,704,759 | 6,781 |
| Dask | 2 workers | 39.75 | 765.50 | 1,704,759 | 6,781 |
| Spark | `local[1]` | 124.41 | 870.54 | 1,704,759 | 6,781 |
| Spark | `local[2]` | 70.05 | 716.17 | 1,704,759 | 6,781 |

En estas ejecuciones Dask tardó menos que Spark en ambas configuraciones: 83.90 frente a 124.41 s con paralelismo 1 y 39.75 frente a 70.05 s con paralelismo 2. Su pico fue menor con paralelismo 1 (668.44 frente a 870.54 MiB), pero algo mayor con paralelismo 2 (765.50 frente a 716.17 MiB). Aumentar paralelismo redujo el tiempo en ambos motores; el pico subió en Dask y bajó en Spark, así que no crece necesariamente de forma lineal por asignación del heap, GC y variabilidad de una sola corrida. Para esta agregación Dask mostró menor tiempo; Spark ofrece mejor integración con procesamiento distribuido conectado a MongoDB. No se debe generalizar: tiempos y memoria dependen de la máquina, caché y carga del sistema.

### Jenkins y descarga automatizada de Kaggle

Inicia Jenkins bajo el perfil `ci`:

```powershell
docker compose --profile ci up -d --build jenkins
docker compose --profile ci logs jenkins
```

Abre `http://localhost:8080`, termina el asistente inicial y crea una credencial **Secret file** con ID `kaggle-json` a partir de tu archivo local de Kaggle. La imagen instala Docker CLI/Compose y los plugins de **GitHub**, **Pipeline** y **Credentials Binding**. Crea un job **Pipeline** conectado al repositorio y selecciona `Jenkinsfile`; el pipeline declara el trigger `githubPush()`. Cada build ejecuta pytest y prueba una API candidata, incluyendo rutas Yelp. Solo `main` descarga/ingesta Yelp completa con KaggleHub, ejecuta Spark para sentimiento/MapReduce/balanceo y despliega la API; el caché, los modelos y reportes quedan bajo `data/`, no versionados. La credencial se copia a un directorio temporal con permisos restringidos y se elimina al terminar. El flujo de taxis se conserva como legado y su smoke test continúa en el pipeline. El parámetro `COMPOSE_PROJECT_NAME` debe coincidir con el proyecto donde corre Jenkins (en este entorno: `parcial`).

Jenkins monta `/var/run/docker.sock` para ejecutar el despliegue en el Docker del equipo. **Ese socket otorga a los jobs control prácticamente administrativo sobre Docker y los contenedores del host**; úsalo solo en este entorno local y no ejecutes cambios de repositorios o ramas no confiables. En Linux, si Jenkins no puede acceder al socket, define `DOCKER_GID` con el grupo propietario del socket antes de recrear el servicio. El volumen `jenkins_home` conserva la configuración y `./data` comparte datasets, caché y reportes con los servicios del proyecto.

Para que GitHub dispare el webhook, Jenkins debe tener una URL HTTPS pública alcanzable desde GitHub: la dirección local `http://localhost:8080` no es accesible desde Internet. En **Settings → Webhooks → Add webhook**, usa `<URL pública de Jenkins>/github-webhook/`, selecciona `application/json` y el evento **Just the push event**. No expongas directamente el puerto de desarrollo sin un túnel/reverse proxy HTTPS protegido. Después de guardar el webhook, verifica el evento en la pestaña **Recent Deliveries** y que Jenkins inicie el job.

### Pruebas locales

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```
