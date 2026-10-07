# Procesamiento geoespacial de trayectorias de taxis

Proyecto académico de Big Data para analizar trayectorias de taxis de Porto. La solución se construirá con Dask, MongoDB, Spark, Flask, Docker Compose y Jenkins.

## Estado

La infraestructura local inicial está definida en `docker-compose.yml`: MongoDB, un scheduler de Dask con dos workers y un master de Spark con un worker. La API Flask y Jenkins se agregarán en etapas posteriores.

## Dataset

El archivo original se obtiene de Kaggle mediante el pipeline y se guarda localmente en `data/`. Los CSV y ZIP están excluidos de Git por su tamaño. Las credenciales de Kaggle se administran fuera del repositorio; no agregues tokens ni archivos `kaggle.json`.

## Carpetas

- `data/`: datos locales, no versionados.
- `notebooks/`: notebooks de exploración y análisis.
- `docs/`: documentación del proyecto, arquitectura e informe.
- `docs/reference/`: material de referencia local, excluido de Git.

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

Para ver los registros o detener los servicios:

```powershell
docker compose logs -f
docker compose down
```

MongoDB conserva sus datos en el volumen `mongodb_data`; `docker compose down` no lo elimina. No uses `docker compose down -v` si quieres conservarlos.
