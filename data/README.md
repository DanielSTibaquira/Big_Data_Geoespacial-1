# Datos locales

El dataset original se mantiene localmente y no se versiona en Git. El notebook puede usar `train.csv` desde esta carpeta para explorar una muestra.

La descarga reproducible está definida en `Jenkinsfile` y obtiene el dataset de Kaggle cuando no existe `train.csv`. Configura en Jenkins una credencial **Secret file** con ID `kaggle-json`. No guardes aquí tokens ni archivos `kaggle.json`.
