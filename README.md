# Procesamiento geoespacial de trayectorias de taxis

Proyecto académico de Big Data para analizar trayectorias de taxis de Porto. La solución se construirá con Dask, MongoDB, Spark, Flask, Docker Compose y Jenkins.

## Estado

La infraestructura local está definida en `docker-compose.yml`: MongoDB, Dask, Spark y una API Flask. Jenkins y los contenedores de benchmark se activan con perfiles opcionales.

## Dataset

El archivo original es [Taxi Trajectory Data](https://www.kaggle.com/datasets/crailtap/taxi-trajectory), de aproximadamente 1,94 GB. Jenkins lo descarga automáticamente cuando `/data/train.csv` no existe; los CSV y ZIP están excluidos de Git. Las credenciales de Kaggle se administran en Jenkins y nunca deben agregarse al repositorio.

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

Abre `http://localhost:8080`, termina el asistente inicial y crea una credencial **Secret file** con ID `kaggle-json` a partir de tu archivo local de Kaggle. La imagen instala Docker CLI/Compose y los plugins de **GitHub**, **Pipeline** y **Credentials Binding**. Crea un job **Pipeline** conectado al repositorio y selecciona `Jenkinsfile`; el pipeline declara el trigger `githubPush()`. El job descarga `crailtap/taxi-trajectory` a `data/train.csv` si aún no existe, ejecuta pytest, construye la imagen de API, levanta una API candidata y MongoDB, comprueba `/health` y ejecuta un POST GeoJSON a `/api/v1/trips/within` desde Jenkins contra MongoDB. Solo si ambos smoke tests responden correctamente, actualiza el servicio `api`; marca `FORCE_DATASET_DOWNLOAD` para forzar la descarga. El parámetro `COMPOSE_PROJECT_NAME` debe coincidir con el proyecto donde corre Jenkins (en este entorno: `parcial`).

Jenkins monta `/var/run/docker.sock` para ejecutar el despliegue en el Docker del equipo. **Ese socket otorga a los jobs control prácticamente administrativo sobre Docker y los contenedores del host**; úsalo solo en este entorno local y no ejecutes cambios de repositorios o ramas no confiables. En Linux, si Jenkins no puede acceder al socket, define `DOCKER_GID` con el grupo propietario del socket antes de recrear el servicio. El volumen `jenkins_home` conserva la configuración y `./data` comparte el CSV con los servicios del proyecto.

Para que GitHub dispare el webhook, Jenkins debe tener una URL HTTPS pública alcanzable desde GitHub: la dirección local `http://localhost:8080` no es accesible desde Internet. En **Settings → Webhooks → Add webhook**, usa `<URL pública de Jenkins>/github-webhook/`, selecciona `application/json` y el evento **Just the push event**. No expongas directamente el puerto de desarrollo sin un túnel/reverse proxy HTTPS protegido. Después de guardar el webhook, verifica el evento en la pestaña **Recent Deliveries** y que Jenkins inicie el job.

### Pruebas locales

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```
