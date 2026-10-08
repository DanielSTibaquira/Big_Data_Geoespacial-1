FROM daskdev/dask:2026.8.0-py3.12

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

ENTRYPOINT ["python", "ingest.py"]
