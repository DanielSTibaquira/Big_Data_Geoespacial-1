# Procesamiento geoespacial de trayectorias de taxis

Proyecto académico de Big Data para analizar trayectorias de taxis de Porto. La solución se construirá con Dask, MongoDB, Spark, Flask, Docker Compose y Jenkins.

## Estado

Estructura inicial del proyecto. Los servicios y las instrucciones de despliegue se documentarán aquí a medida que se implementen.

## Dataset

El archivo original se obtiene de Kaggle mediante el pipeline y se guarda localmente en `data/`. Los CSV y ZIP están excluidos de Git por su tamaño. Las credenciales de Kaggle se administran fuera del repositorio; no agregues tokens ni archivos `kaggle.json`.

## Carpetas

- `data/`: datos locales, no versionados.
- `notebooks/`: notebooks de exploración y análisis.
- `docs/`: documentación del proyecto, arquitectura e informe.
- `docs/reference/`: material de referencia local, excluido de Git.

## Ejecución

Las instrucciones para levantar los servicios desde cero se agregarán cuando estén disponibles `docker-compose.yml` y los servicios del proyecto.
